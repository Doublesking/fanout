package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeXUI 是一个最小的 3x-ui 面板替身，只实现 fanout 用到的几个接口，
// 并把"写配置"和"重启 Xray"的次数记下来——这两件事就是整池出口瞬断的来源。
type fakeXUI struct {
	srv      *httptest.Server
	setting  map[string]any
	inbounds []map[string]any
	testURL  string
	mu       sync.Mutex
	updates  int // panel/api/xray/update 被调了几次
	restarts int // panel/api/server/restartXrayService 被调了几次
}

func (f *fakeXUI) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch r.URL.Path {
	case "/panel/api/xray/":
		blob, _ := json.Marshal(f.setting)
		inner, _ := json.Marshal(map[string]any{
			"outboundTestUrl": f.testURL,
			"xraySetting":     json.RawMessage(blob),
		})
		obj, _ := json.Marshal(string(inner))
		fmt.Fprintf(w, `{"success":true,"msg":"","obj":%s}`, obj)
	case "/panel/api/xray/update":
		_ = r.ParseForm()
		var next map[string]any
		if err := json.Unmarshal([]byte(r.Form.Get("xraySetting")), &next); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.setting = next
		f.testURL = r.Form.Get("outboundTestUrl")
		f.updates++
		fmt.Fprint(w, `{"success":true,"msg":""}`)
	case "/panel/api/inbounds/list":
		blob, _ := json.Marshal(f.inbounds)
		fmt.Fprintf(w, `{"success":true,"msg":"","obj":%s}`, blob)
	case "/panel/api/server/restartXrayService":
		f.restarts++
		fmt.Fprint(w, `{"success":true,"msg":""}`)
	default:
		http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
	}
}

func (f *fakeXUI) counts() (updates, restarts int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updates, f.restarts
}

// newFakeXUI 起一个替身面板，并返回指向它的 XUI 客户端。
func newFakeXUI(t *testing.T) (*fakeXUI, *XUI) {
	t.Helper()
	f := &fakeXUI{setting: map[string]any{"outbounds": []any{}}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)

	u, err := url.Parse(f.srv.URL)
	if err != nil {
		t.Fatalf("解析测试服务器地址失败: %v", err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("解析测试服务器端口失败: %v", err)
	}
	return f, &XUI{
		Host:   u.Hostname(),
		Port:   port,
		Scheme: "http",
		token:  "test-token",
		client: f.srv.Client(),
	}
}

func testTunnel(slot, port int, host string) *Tunnel {
	return &Tunnel{
		Slot:   slot,
		Port:   port,
		Status: "up",
		Node:   Node{HostName: host, CountryCode: "JP"},
		Cred:   SocksCred{User: "u" + strconv.Itoa(slot), Pass: "p" + strconv.Itoa(slot)},
	}
}

// 隧道原地重连（节点名没变）后走的 resync 路径，算出来的配置与面板里已有的
// 完全一样。这种情况下必须不写回、不重启 Xray：一次全量重启会瞬断所有出口。
func TestResyncOutboundSkipsWriteWhenUnchanged(t *testing.T) {
	fake, x := newFakeXUI(t)
	tunnels := []*Tunnel{testTunnel(1, 21001, "jp-01")}

	if err := x.ResyncOutbound(tunnels[0], tunnels); err != nil {
		t.Fatalf("首次同步失败: %v", err)
	}
	if updates, restarts := fake.counts(); updates != 1 || restarts != 1 {
		t.Fatalf("首次同步应当写一次并重启一次，实际 update=%d restart=%d", updates, restarts)
	}

	if err := x.ResyncOutbound(tunnels[0], tunnels); err != nil {
		t.Fatalf("重复同步失败: %v", err)
	}
	if updates, restarts := fake.counts(); updates != 1 || restarts != 1 {
		t.Fatalf("配置没变时不该写回也不该重启 Xray，实际 update=%d restart=%d", updates, restarts)
	}
}

// 反过来：配置真的变了就必须写。别把"跳过"做成"永远不写"。
func TestResyncOutboundWritesWhenConfigChanged(t *testing.T) {
	fake, x := newFakeXUI(t)
	tunnels := []*Tunnel{testTunnel(1, 21001, "jp-01")}
	if err := x.ResyncOutbound(tunnels[0], tunnels); err != nil {
		t.Fatalf("首次同步失败: %v", err)
	}

	tunnels = append(tunnels, testTunnel(2, 21002, "jp-02"))
	if err := x.ResyncOutbound(tunnels[1], tunnels); err != nil {
		t.Fatalf("新增出口后同步失败: %v", err)
	}
	if updates, restarts := fake.counts(); updates != 2 || restarts != 2 {
		t.Fatalf("多了一条出口就该重新写并重启，实际 update=%d restart=%d", updates, restarts)
	}
}

// 一条出口掉线时不该连带把别的出口踢下线：只改自己那一段。
func TestResyncOutboundDoesNotDropOtherExits(t *testing.T) {
	_, x := newFakeXUI(t)
	tunnels := []*Tunnel{
		testTunnel(1, 21001, "jp-01"),
		testTunnel(2, 21002, "jp-02"),
	}
	if err := x.ResyncOutbound(tunnels[0], tunnels); err != nil {
		t.Fatalf("同步失败: %v", err)
	}

	// 第 2 条掉线，重连失败后 Status 不是 up
	tunnels[1].Status = "starting"
	if err := x.ResyncOutbound(tunnels[0], tunnels); err != nil {
		t.Fatalf("同步失败: %v", err)
	}

	// 面板里的 fanout- 出站只应留下 jp-01 那条
	setting, _, err := x.loadXray()
	if err != nil {
		t.Fatalf("读回配置失败: %v", err)
	}
	got := map[string]bool{}
	outbounds, _ := setting["outbounds"].([]any)
	for _, ob := range outbounds {
		m, ok := ob.(map[string]any)
		if !ok {
			continue
		}
		tag, _ := m["tag"].(string)
		if len(tag) >= len(xuiTagPrefix) && tag[:len(xuiTagPrefix)] == xuiTagPrefix {
			got[tag] = true
		}
	}
	if !got[tunnelTag(tunnels[0])] {
		t.Fatalf("jp-01 的出站丢了: %v", got)
	}
	if got[tunnelTag(tunnels[1])] {
		t.Fatalf("jp-02 已掉线，不该还有出站: %v", got)
	}
}

func TestMarkFlapOnlyReportsFirstTime(t *testing.T) {
	var tn Tunnel

	if !tn.markFlap("jp-01") {
		t.Fatal("首次记入应当返回 true（调用方据此打日志）")
	}
	if tn.markFlap("jp-01") {
		t.Fatal("重复记入不该再返回 true，否则日志会刷屏")
	}
	if !tn.isFlapped("jp-01") {
		t.Fatal("jp-01 应被判定为抖动节点")
	}
	if tn.isFlapped("jp-02") {
		t.Fatal("别的节点不该被牵连")
	}
	if !tn.clearFlap("jp-01") {
		t.Fatal("解除抖动标记应当返回 true")
	}
	if tn.isFlapped("jp-01") {
		t.Fatal("解除后不该还在黑名单里")
	}
	if tn.clearFlap("jp-01") {
		t.Fatal("重复解除不该返回 true")
	}
	if tn.markFlap("") {
		t.Fatal("空节点名不该被记入")
	}
}

// 抖动黑名单有上限，免得把同地区候选排干、反而让重连无节点可选。
func TestMarkFlapIsBounded(t *testing.T) {
	var tn Tunnel
	for i := 0; i < flapHistoryMax+5; i++ {
		tn.markFlap(fmt.Sprintf("node-%d", i))
	}
	tn.mu.Lock()
	n := len(tn.flapped)
	tn.mu.Unlock()
	if n > flapHistoryMax {
		t.Fatalf("黑名单最多留 %d 条，实际 %d 条", flapHistoryMax, n)
	}
}

// 核心行为：抖动节点不再排在候选第一位，否则重连会对着它一轮轮空转。
func TestCandidatesForSkipsFlappedHead(t *testing.T) {
	withResidentialOnly(t, boolPtr(false))
	m := mgrWith([]Node{
		{HostName: "jp-01", CountryCode: "JP"},
		{HostName: "jp-02", CountryCode: "JP"},
		{HostName: "jp-03", CountryCode: "JP"},
	}, "jp-01")
	tn := m.tunnels[1]

	got := hostsOf(m.candidatesFor(tn))
	if len(got) < 2 {
		t.Fatalf("候选池不该是空的，实际 %v", got)
	}
	if got[0] != "jp-01" {
		t.Fatalf("正常情况下应当优先原节点，实际 %v", got)
	}

	tn.markFlap("jp-01")
	got = hostsOf(m.candidatesFor(tn))
	if len(got) == 0 {
		t.Fatal("记入抖动黑名单后仍应给出其他候选")
	}
	if got[0] == "jp-01" {
		t.Fatalf("抖动节点不该再排第一，实际 %v", got)
	}
	for _, h := range got {
		if h == "jp-01" {
			t.Fatalf("抖动节点干脆不该出现在候选里（除非无路可走），实际 %v", got)
		}
	}
}

// 同地区候选全被避开时宁可回原节点试一次，也不能一个候选都不给。
func TestCandidatesForFallsBackToFlappedNode(t *testing.T) {
	withResidentialOnly(t, boolPtr(false))
	m := mgrWith([]Node{{HostName: "jp-01", CountryCode: "JP"}}, "jp-01")
	tn := m.tunnels[1]
	tn.markFlap("jp-01")

	got := hostsOf(m.candidatesFor(tn))
	if len(got) != 1 || got[0] != "jp-01" {
		t.Fatalf("候选全被避开时应当回退到原节点，实际 %v", got)
	}
}

// 绑定/解绑走的是同一条 mutateXray 路径：该写的时候必须写，重复绑同一个目标
// 则不该再白重启一次 Xray。
func TestBindWritesOnceAndUnbindClearsRule(t *testing.T) {
	fake, x := newFakeXUI(t)
	fake.inbounds = []map[string]any{
		{"id": 1, "port": 2053, "protocol": "vless", "remark": "线路A", "enable": true, "tag": "in-2053-tcp"},
	}
	tunnels := []*Tunnel{testTunnel(1, 21001, "jp-01")}

	if err := x.Bind("in-2053-tcp", "jp-01", tunnels); err != nil {
		t.Fatalf("绑定失败: %v", err)
	}
	if _, restarts := fake.counts(); restarts != 1 {
		t.Fatalf("首次绑定应当写回并重启一次，实际 %d 次", restarts)
	}
	if got := boundTagFor(t, x, "in-2053-tcp"); got != tunnelTag(tunnels[0]) {
		t.Fatalf("规则没写对，实际 %q", got)
	}

	// 再绑同一个出口：算出来的配置与面板里的一样，不该再重启
	if err := x.Bind("in-2053-tcp", "jp-01", tunnels); err != nil {
		t.Fatalf("重复绑定失败: %v", err)
	}
	if _, restarts := fake.counts(); restarts != 1 {
		t.Fatalf("重复绑同一个出口不该再重启 Xray，实际 %d 次", restarts)
	}

	// 解绑：规则必须真的被清掉
	if err := x.Bind("in-2053-tcp", "", tunnels); err != nil {
		t.Fatalf("解绑失败: %v", err)
	}
	if _, restarts := fake.counts(); restarts != 2 {
		t.Fatalf("解绑应当写回并重启，实际 %d 次", restarts)
	}
	if got := boundTagFor(t, x, "in-2053-tcp"); got != "" {
		t.Fatalf("解绑后不该还有规则，实际 %q", got)
	}
}

// 换节点时命中的每个入站原来各调一次 Bind，每次都以全量重启 Xray 收尾。
// 改造后应当一次写完。
func TestRebindWritesOnceForMultipleInbounds(t *testing.T) {
	fake, x := newFakeXUI(t)
	fake.setting = map[string]any{
		"outbounds": []any{},
		"routing": map[string]any{"rules": []any{
			map[string]any{"type": "field", "inboundTag": []any{"in-2053-tcp"}, "outboundTag": "fanout-jp-01"},
			map[string]any{"type": "field", "inboundTag": []any{"in-2054-tcp"}, "outboundTag": "fanout-jp-01"},
		}},
	}
	// 备注用非 fanout 生成的写法，避开改名路径，专测写入次数
	fake.inbounds = []map[string]any{
		{"id": 1, "port": 2053, "protocol": "vless", "remark": "线路A", "enable": true, "tag": "in-2053-tcp"},
		{"id": 2, "port": 2054, "protocol": "vless", "remark": "线路B", "enable": true, "tag": "in-2054-tcp"},
	}
	target := testTunnel(1, 21001, "jp-02")
	tunnels := []*Tunnel{target}

	if err := x.Rebind("jp-01", target, tunnels); err != nil {
		t.Fatalf("改绑失败: %v", err)
	}
	if _, restarts := fake.counts(); restarts != 1 {
		t.Fatalf("两个入站改绑只该重启一次 Xray，实际 %d 次", restarts)
	}
	for _, tag := range []string{"in-2053-tcp", "in-2054-tcp"} {
		if got := boundTagFor(t, x, tag); got != tunnelTag(target) {
			t.Fatalf("%s 没改绑到新节点，实际 %q", tag, got)
		}
	}
}

// boundTagFor 读回某个入站当前指向的 fanout 出站 tag，空串表示没绑定。
func boundTagFor(t *testing.T, x *XUI, inboundTag string) string {
	t.Helper()
	setting, _, err := x.loadXray()
	if err != nil {
		t.Fatalf("读回配置失败: %v", err)
	}
	routing, _ := setting["routing"].(map[string]any)
	rules, _ := routing["rules"].([]any)
	for _, r := range rules {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		tag, _ := m["outboundTag"].(string)
		if !strings.HasPrefix(tag, xuiTagPrefix) {
			continue
		}
		for _, it := range toStringSlice(m["inboundTag"]) {
			if it == inboundTag {
				return tag
			}
		}
	}
	return ""
}

func hostsOf(nodes []Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.HostName)
	}
	return out
}
