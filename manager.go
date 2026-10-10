package main

import (
	"fmt"
	"log"
	"os/exec"
	"sort"
	"sync"
	"time"
)

// Manager 维护所有隧道，负责分配槽位与端口。
type Manager struct {
	mu       sync.RWMutex
	tunnels  map[int]*Tunnel
	nodes    []Node
	fetched  time.Time
	workDir  string
	maxSlots int
	jobs     JobStore
	// applier 是后端配置写入的唯一入口（见 applier.go）。
	applier *configApplier
	// refreshMu 串行化节点列表的拉取。
	//
	// 重连循环每条隧道每轮都要刷一次列表，11 条隧道同时失败就是对上游的
	// 11 个并发请求，很容易被限流甚至封 IP。拿这把锁的只有真正要发请求的人，
	// 其余调用者等它拉完直接复用结果。
	refreshMu sync.Mutex
	// fetchNodes 是拉取节点列表的实现。留成字段是为了单测能替换掉，
	// 免得跑个测试真的去打 VPN Gate。
	fetchNodes func(time.Duration) ([]Node, error)
}

func NewManager(maxSlots int, workDir string) *Manager {
	m := &Manager{
		tunnels:  map[int]*Tunnel{},
		workDir:  workDir,
		maxSlots: maxSlots,
	}
	m.applier = newConfigApplier(m)
	m.fetchNodes = fetchNodes
	return m
}

// 节点列表的刷新节奏。
const (
	// nodeRefreshEvery 是后台周期刷新的间隔。
	//
	// VPN Gate 节点寿命很短（志愿者机器说关就关），列表只在启动/手动/重连时更新的话，
	// 界面上的地区可用数会越来越不准，挑节点也容易挑到已经死掉的。
	nodeRefreshEvery = 12 * time.Minute
	// nodeCacheTTL 是列表的最短复用时间：没过期就直接复用，不再发请求。
	nodeCacheTTL = 90 * time.Second
)

// RefreshNodes 重新拉取节点列表。
//
// 多个并发调用只会真的拉一次：后到的等前一个拉完，直接复用刚拿到的结果。
func (m *Manager) RefreshNodes() (int, error) {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	return m.refreshNodesLocked()
}

// refreshNodesIfStale 只在列表过期时才真的拉一次。
//
// 重连循环里用它替代 RefreshNodes：一条隧道一轮重试拉一次列表，
// 11 条隧道同时失败就是 11 个并发请求打到上游。VPN Gate 的列表十几分钟才更新一次，
// 缓存没过期就复用完全够用，也免得把出口池刷成别人眼里的攻击源。
func (m *Manager) refreshNodesIfStale() (int, error) {
	if m.nodesFresh() {
		return len(m.NodesOnly()), nil
	}
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	// 等锁期间可能有人刚拉过，再确认一次
	if m.nodesFresh() {
		return len(m.NodesOnly()), nil
	}
	return m.refreshNodesLocked()
}

// nodesFresh 判断缓存的节点列表还能不能用。
func (m *Manager) nodesFresh() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes) > 0 && time.Since(m.fetched) < nodeCacheTTL
}

// NodesOnly 只取节点列表，不关心拉取时间。
func (m *Manager) NodesOnly() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Node, len(m.nodes))
	copy(out, m.nodes)
	return out
}

// refreshNodesLocked 真的发一次请求。调用方必须已持 refreshMu。
func (m *Manager) refreshNodesLocked() (int, error) {
	nodes, err := m.fetchNodes(60 * time.Second)
	if err != nil {
		return 0, err
	}
	// 上游抽风返回空列表时保留原列表：把手里能用的节点全丢掉，
	// 等于让所有出口一起失去候选，比用一份略旧的列表糟得多。
	if len(nodes) == 0 {
		return 0, fmt.Errorf("上游返回 0 个节点，保留原列表")
	}
	m.mu.Lock()
	m.nodes = nodes
	m.fetched = time.Now()
	m.mu.Unlock()
	return len(nodes), nil
}

// StartNodeRefresher 起一个后台协程周期刷新节点列表。
func (m *Manager) StartNodeRefresher() {
	go func() {
		for range time.Tick(nodeRefreshEvery) {
			if n, err := m.RefreshNodes(); err != nil {
				log.Printf("周期刷新节点列表失败: %v", err)
			} else {
				log.Printf("周期刷新节点列表完成，共 %d 个节点", n)
			}
		}
	}()
}

func (m *Manager) Nodes() ([]Node, time.Time) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Node, len(m.nodes))
	copy(out, m.nodes)
	return out, m.fetched
}

func (m *Manager) Tunnels() []*Tunnel {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Tunnel, 0, len(m.tunnels))
	for _, t := range m.tunnels {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}

// freeSlot 找一个未占用的槽位。槽位同时决定端口与网段。
func (m *Manager) freeSlot() (int, error) {
	for i := 1; i <= m.maxSlots; i++ {
		if _, used := m.tunnels[i]; !used {
			return i, nil
		}
	}
	return 0, fmt.Errorf("槽位已满（上限 %d）", m.maxSlots)
}

// Start 为指定节点开一条隧道，返回分配到的本地端口。
func (m *Manager) Start(node Node) (*Tunnel, error) {
	m.mu.Lock()
	slot, err := m.freeSlot()
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	// 端口随机取，避免固定规律撞上机器上的其他服务
	taken := map[int]bool{}
	for _, other := range m.tunnels {
		taken[other.Port] = true
	}
	port, err := freeRandomPort(taken)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	cred, err := newSocksCred()
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	t := &Tunnel{
		Slot:   slot,
		Port:   port,
		Node:   node,
		Status: "starting",
		Since:  time.Now(),
		Cred:   cred,
	}
	m.tunnels[slot] = t
	m.mu.Unlock()

	go m.bringUp(t, true)
	return t, nil
}

// bringUp 把一条隧道拉起来。
//
// notify 决定成功后是否立刻重建后端配置。换节点重连时要传 false：
// 那条路径随后会调 rebind/resync 把入站改绑到新节点，在那之前重建配置
// 会因为入站还指着旧节点名而把路由规则丢掉。
func (m *Manager) bringUp(t *Tunnel, notify bool) {
	m.bringUpPersist(t, notify, false)
}

// 自动重连的退避区间：一轮候选全挂后等一会儿再刷新节点列表重来，
// 别把死节点列表打爆，也别让恢复拖太久。
const (
	reconnectBackoffMin = 5 * time.Second
	reconnectBackoffMax = 60 * time.Second
	// backoffSlowFactor 是"这条出口最近掉得比较勤"时起步退避放大的倍数。
	backoffSlowFactor = 4
)

// initialBackoff 按这条出口最近的掉线频率决定从多长的退避起步。
//
// 退避原来是 bringUpPersist 的局部变量，每次重连都从 5 秒重新起步。
// 于是一个"连上几十秒又掉"的节点会永远以 5 秒空转：退避永远升不上去，
// 重连循环就成了常态，出口看着一直在动，实际上没一会儿是通的。
// 掉得越勤，起步退避越长，到熔断线直接拉到上限。
func (m *Manager) initialBackoff(t *Tunnel) time.Duration {
	switch n := t.flapRate(time.Now()); {
	case n >= flapCircuitBreaker:
		return reconnectBackoffMax
	case n >= flapSwapAfter:
		return reconnectBackoffMin * backoffSlowFactor
	default:
		return reconnectBackoffMin
	}
}

// bringUpPersist 把一条隧道拉起来。
//
// persist=false（手动新建）：走一轮候选，全失败就标 failed，让用户能立刻看到并重试。
// persist=true（自动重连 / 重启恢复）：一轮全失败不放弃，退避后刷新节点列表再来一轮，
// 一直循环到连上或这条隧道被用户停掉。VPN Gate 死节点多，"当前都不可用"往往只是
// 这一批候选恰好都挂了，过一会儿就有新节点，不该让出口永久躺死。
func (m *Manager) bringUpPersist(t *Tunnel, notify bool, persist bool) {
	backoff := m.initialBackoff(t)
	for {
		if m.tryCandidates(t, notify) {
			return
		}
		// 隧道已被用户停掉或从管理器移除，别再重试
		if !persist || !m.tunnelActive(t) {
			if persist {
				return
			}
			t.Status = "failed"
			if serr := m.saveState(); serr != nil {
				log.Printf("保存状态失败: %v", serr)
			}
			return
		}

		t.Status = "starting"
		t.Err = fmt.Sprintf("暂无可用节点，%.0f 秒后重试", backoff.Seconds())
		log.Printf("隧道 %d 一轮候选均失败，%.0f 秒后刷新节点重试", t.Slot, backoff.Seconds())
		time.Sleep(backoff)
		if !m.tunnelActive(t) {
			return
		}
		if _, err := m.refreshNodesIfStale(); err != nil {
			log.Printf("重试前刷新节点列表失败: %v", err)
		}
		if backoff < reconnectBackoffMax {
			backoff *= 2
			if backoff > reconnectBackoffMax {
				backoff = reconnectBackoffMax
			}
		}
	}
}

// tryCandidates 走一轮候选节点，成功返回 true。失败不改 Status（留给调用方决定）。
func (m *Manager) tryCandidates(t *Tunnel, notify bool) bool {
	// VPN Gate 是志愿者节点，列表里有相当比例已下线或满员（AUTH_FAILED），
	// 连不上就顺着候选列表换下一个，不必让用户手动试。
	candidates := m.candidatesFor(t)
	// 记下这次出发时想去的地区：候选里混进跨地区备胎时，日志里能看出来
	want := t.Node.CountryCode
	for i, node := range candidates {
		if !m.tunnelActive(t) {
			return false
		}
		// 其他隧道可能在重试期间占用了这个节点，跳过以免多个端口撞同一出口 IP
		if i > 0 && m.nodeInUse(node.HostName, t.Slot) {
			continue
		}
		t.Node = node
		t.Status = "starting"
		if i > 0 {
			t.Err = fmt.Sprintf("已换到第 %d 个候选节点", i+1)
		}

		err := m.tryNode(t)
		if err == nil {
			t.Status = "up"
			t.Err = ""
			if want != "" && node.CountryCode != "" && node.CountryCode != want {
				log.Printf("隧道 %d 原地区 %s 没有可用节点，已换到 %s (%s)",
					t.Slot, want, node.CountryCode, node.HostName)
			}
			if serr := m.saveState(); serr != nil {
				log.Printf("保存状态失败: %v", serr)
			}
			if notify {
				m.notifyPanel()
			}
			return true
		}
		t.teardownNetns()
	}
	return false
}

// tunnelActive 判断这条隧道是否还归管理器所有且未被用户停掉。
// 用指针比对：Stop 会从 map 里删除并把 Status 置 stopped，
// 重连循环据此退出，避免对着一条已经不存在的隧道空转。
func (m *Manager) tunnelActive(t *Tunnel) bool {
	if t.Status == "stopped" {
		return false
	}
	m.mu.RLock()
	cur, ok := m.tunnels[t.Slot]
	m.mu.RUnlock()
	return ok && cur == t
}

// tryNode 尝试用当前节点把隧道拉起来。
func (m *Manager) tryNode(t *Tunnel) error {
	if err := t.setupNetns(); err != nil {
		return err
	}
	if err := t.startOpenVPN(m.workDir); err != nil {
		return err
	}
	if t.listener == nil {
		if err := t.serve(); err != nil {
			return err
		}
	}
	ip, err := t.probeExitIP()
	if err != nil {
		return err
	}
	t.ExitIP = ip
	return nil
}

// candidatesFor 以这条隧道当前的节点打头，后面跟上同地区的其他节点作为备选。
//
// 打头的一般是 t.Node：自动重连的目标是把这条出口恢复原样，先试原节点。
// 备选会避开用户手动换掉过的节点——那些是他明确不想要的 IP，
// 让重连悄悄换回去等于撤销了他的操作。
//
// 例外是抖动黑名单里的节点（见 Tunnel.flapped）：它们连得上、但留不住，
// 再把它排在第一位只会让重连一轮轮空转，每轮都白重启一次面板的 Xray。
func (m *Manager) candidatesFor(t *Tunnel) []Node {
	const maxTries = 6
	// minRegionCandidates 是同地区候选少到这个数以下时，放宽到任意地区兜底。
	const minRegionCandidates = 2
	first := t.Node
	avoid := t.swapAvoid()
	skipFirst := t.isFlapped(first.HostName)

	m.mu.RLock()
	defer m.mu.RUnlock()

	used := map[string]bool{first.HostName: true}
	for _, other := range m.tunnels {
		used[other.Node.HostName] = true
	}

	// 地区决定了备选范围，缺失时先从当前列表补一次，
	// 否则会退化成"任意地区都算同区"。
	region := first.CountryCode
	if region == "" {
		for _, n := range m.nodes {
			if n.HostName == first.HostName {
				region = n.CountryCode
				break
			}
		}
	}

	pool := m.nodePoolLocked()
	out := []Node{}
	if !skipFirst {
		out = append(out, first)
	}
	for _, n := range pool {
		if len(out) >= maxTries {
			break
		}
		if used[n.HostName] || avoid[n.HostName] {
			continue
		}
		// 地区实在拿不到时不做限制，总比连不上强
		if region != "" && n.CountryCode != region {
			continue
		}
		out = append(out, n)
	}

	// 同地区候选太少就放宽到任意地区兜底。
	//
	// VPN Gate 有些地区就那么一两个节点，还经常整段挂掉——US 和 UA 两条出口
	// 就是死在这：同地区再没有能试的节点，于是一轮轮"候选均失败"，出口一直躺着。
	// 首选仍然是原地区（上面已经排在前面），跨地区的只是失败之后的备胎：
	// 换个国家的出口，总比这条出口彻底不通强。
	if len(out) < minRegionCandidates {
		have := map[string]bool{}
		for _, n := range out {
			have[n.HostName] = true
		}
		for _, n := range pool {
			if len(out) >= maxTries {
				break
			}
			if used[n.HostName] || avoid[n.HostName] || have[n.HostName] {
				continue
			}
			out = append(out, n)
		}
	}

	if len(out) == 0 {
		// 连跨地区备胎都没有：宁可回原节点再试一次，
		// 也不能让这条隧道连一个候选都没有
		out = append(out, first)
	}
	return out
}

// Stop 停掉一条隧道并释放槽位。
func (m *Manager) Stop(slot int) error {
	invalidateInbounds()
	m.mu.Lock()
	t, ok := m.tunnels[slot]
	if ok {
		delete(m.tunnels, slot)
	}
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("槽位 %d 没有运行中的隧道", slot)
	}
	t.stop()
	if err := m.saveState(); err != nil {
		log.Printf("保存状态失败: %v", err)
	}
	m.notifyPanel()
	return nil
}

// Swap 把一条隧道换到同地区的另一个节点上，端口与已分发的客户端配置保持不变。
//
// 与健康检查的自动重连不同：那边优先重连原节点（目标是恢复），
// 这里用户是嫌当前出口 IP 不好用，必须真的换一个。
//
// 换过的节点会记进历史一并避开。只排除"当前这个"是不够的：
// A 换成 B 之后 A 就空出来了，而候选是按速度排的，A 往往又排在最前面，
// 于是再点一次就换回了 A。历史让每次点击都真的换一个没用过的。
func (m *Manager) Swap(slot int) error {
	m.mu.RLock()
	t, ok := m.tunnels[slot]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("槽位 %d 没有运行中的隧道", slot)
	}
	if t.Status == "starting" {
		return fmt.Errorf("这个出口正在连接中，稍等一下")
	}

	node, err := m.pickSwapTarget(t)
	if err != nil {
		return err
	}
	oldHost := t.Node.HostName
	t.Node = node
	m.reconnect(t, oldHost)
	return nil
}

// pickSwapTarget 给"换节点"挑下一个目标，并把该跳过的节点记进历史。
//
// 记两个：换下来的那个，以及刚挑中的这个。挑中的也记是因为真机上踩到过——
// 它连不上时会被候选列表换成别人，但它自己没进历史，
// 于是下次点换节点又从它开始试一遍，白等一轮握手超时。
func (m *Manager) pickSwapTarget(t *Tunnel) (Node, error) {
	avoid := t.swapAvoid()
	picks, err := m.pickNodes(t.Node.CountryCode, 1, avoid)
	if err != nil && len(avoid) > 1 {
		// 这个地区的节点都换过一轮了。清掉历史重新开始，
		// 总比告诉用户"没得换了"好——转一圈之后原来那些节点未必还是当初的状态。
		t.forgetSwaps()
		picks, err = m.pickNodes(t.Node.CountryCode, 1, t.swapAvoid())
	}
	if err != nil {
		return Node{}, err
	}
	t.rememberSwap(t.Node.HostName)
	t.rememberSwap(picks[0].HostName)
	return picks[0], nil
}

// StopAll 停掉所有隧道并清空状态文件。
func (m *Manager) StopAll() {
	for _, t := range m.Tunnels() {
		_ = m.Stop(t.Slot)
	}
}

// SetCred 改一条出口的 SOCKS5 凭据。cred 两个字段都为空表示随机重置。
//
// 改完要通知后端：本机 Xray 的 socks 出站里带着这套凭据，
// 不同步的话面板侧的节点会立刻连不上自己的出口。
func (m *Manager) SetCred(slot int, cred SocksCred) (SocksCred, error) {
	m.mu.RLock()
	t, ok := m.tunnels[slot]
	m.mu.RUnlock()
	if !ok {
		return SocksCred{}, fmt.Errorf("槽位 %d 没有运行中的隧道", slot)
	}

	if cred.User == "" && cred.Pass == "" {
		gen, err := newSocksCred()
		if err != nil {
			return SocksCred{}, err
		}
		cred = gen
	}
	if err := validateCred(cred); err != nil {
		return SocksCred{}, err
	}

	t.setCredential(cred)
	if err := m.saveState(); err != nil {
		log.Printf("保存状态失败: %v", err)
	}
	m.syncCred(t)
	return cred, nil
}

// ReconcileOutbounds 在启动恢复隧道后跑一次，把后端出站对齐到当前隧道（含 SOCKS5 凭据）。
//
// 只为 3x-ui 模式而生：它的 OnTunnelsChanged 是空操作，重启不会重写面板出站，
// 而从旧版本升上来时面板里持久化的 socks 出站没有认证字段，端口一旦要认证就连不上。
// 自建模式恢复时每条隧道 up 都会重建配置，本就自洽，这里跳过免得多重启一次 Xray。
func (m *Manager) ReconcileOutbounds() {
	p, err := openPanel()
	if err != nil || p.Kind() != "3x-ui" {
		return
	}

	// 等隧道尽量都起完再重写一次，避免只覆盖到先 up 的那几条
	deadline := time.Now().Add(90 * time.Second)
	for {
		tunnels := m.Tunnels()
		if len(tunnels) == 0 {
			return
		}
		var up *Tunnel
		settled := true
		for _, t := range tunnels {
			if t.Status == "up" && up == nil {
				up = t
			}
			if t.Status == "starting" {
				settled = false
			}
		}
		if (settled || time.Now().After(deadline)) && up != nil {
			if err := m.resync(up); err != nil {
				log.Printf("启动对账面板出站失败: %v", err)
			}
			return
		}
		if settled || time.Now().After(deadline) {
			return // 全 failed，没有可写的出站
		}
		time.Sleep(2 * time.Second)
	}
}

// syncCred 把新凭据写进后端的 socks 出站。
//
// 两种后端的做法不同：自建模式整份重建配置，3x-ui 模式只改出站那一段。
// 都走 ResyncOutbound，接口语义正好是"重写这条隧道对应的出站"。
func (m *Manager) syncCred(t *Tunnel) {
	if err := m.resync(t); err != nil {
		log.Printf("同步 SOCKS5 凭据到节点链接后端失败: %v", err)
	}
}

// Shutdown 停掉运行态但保留状态文件，让下次启动能恢复同样的隧道。
func (m *Manager) Shutdown() {
	m.applier.close()
	for _, t := range m.Tunnels() {
		t.stop()
	}
}

// prepareHost 打开转发开关。netns 出网依赖它。
func prepareHost() error {
	if err := cmdRun(exec.Command("sysctl", "-qw", "net.ipv4.ip_forward=1")); err != nil {
		return fmt.Errorf("开启 ip_forward 失败: %w", err)
	}
	return nil
}

// nodeInUse 判断某节点是否已被别的隧道占用。
func (m *Manager) nodeInUse(host string, exceptSlot int) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for slot, t := range m.tunnels {
		if slot != exceptSlot && t.Node.HostName == host {
			return true
		}
	}
	return false
}

// rebind 在隧道换节点后，把原先指向旧节点的 3x-ui 入站改绑到新节点。
// 面板不可用时静默跳过，健康检查本身不应因此失败。
func (m *Manager) rebind(oldHost string, t *Tunnel) error {
	x, err := openPanel()
	if err != nil {
		return nil
	}
	return x.Rebind(oldHost, t, m.Tunnels())
}

// resync 在节点没换但重连过之后，把 3x-ui 的出站配置刷新一遍。
// 面板不可用时静默跳过，健康检查本身不应因此失败。
func (m *Manager) resync(t *Tunnel) error {
	x, err := openPanel()
	if err != nil {
		return nil
	}
	return x.ResyncOutbound(t, m.Tunnels())
}

// notifyPanel 告诉后端隧道集合变了。
//
// 自建模式下出站是由隧道列表现算出来的，不通知的话新开的出口在 Xray 里
// 没有对应的 socks 出站，绑定会指向一个不存在的 tag。接管 3x-ui 时是空操作。
//
// 通知走去抖合并（见 applier.go）：启动恢复、批量建出口时多条隧道几乎同时 up，
// 逐条通知就是逐条重建配置 + 逐条重启 Xray。后端不可用不该让开关出口失败。
func (m *Manager) notifyPanel() {
	m.applier.markDirty()
}
