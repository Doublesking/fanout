package main

import (
	"log"
	"sync"
	"time"
)

// notifyDebounce 是"隧道集合变化"通知的去抖窗口。
//
// 启动恢复、批量建出口时多条隧道会在几乎同一瞬间变成 up，
// 逐条通知就是一次配置重建 + 一次 Xray 重启。攒一个短窗口合并成一次。
// 窗口取得很小：出口状态要在界面上尽快反映出来，这里只挡"同一瞬间的批量"。
const notifyDebounce = 800 * time.Millisecond

// configApplier 收敛"后端配置需要更新"的触发点，保证两件事：
//
//  1. 串行化——同一时刻只有一个写入者在跑（applyMu）。没有它时两条隧道
//     同时重连会各自读到同一份旧模板、各自改各自的、再各自写回，
//     后写的把先写的覆盖掉，出站配置就这么丢了一条。
//  2. 去抖合并——短时间内的多次触发只应用一次，避免批量变更引发批量重启。
//
// 注意：同步语义的写入（重连后的 resync / rebind）不经过这里。
// 它们的调用方要在返回后立刻清 prevHost 标记，改成异步会丢掉崩溃恢复的线索。
// 那两条路径靠 XUI.mutateXray 里的写锁 + "无改动就跳过"保证不重复重启。
type configApplier struct {
	mgr *Manager

	mu      sync.Mutex
	timer   *time.Timer
	dirty   bool
	closed  bool
	applyMu sync.Mutex
}

func newConfigApplier(m *Manager) *configApplier {
	return &configApplier{mgr: m}
}

// markDirty 记下"后端配置需要重新对齐"，去抖窗口结束后执行一次。
func (a *configApplier) markDirty() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	a.dirty = true
	if a.timer == nil {
		a.timer = time.AfterFunc(notifyDebounce, a.flush)
	}
}

// flush 执行一次合并后的通知。窗口内又来了新变更就再排一次。
func (a *configApplier) flush() {
	a.mu.Lock()
	if a.closed || !a.dirty {
		a.timer = nil
		a.mu.Unlock()
		return
	}
	a.dirty = false
	a.timer = nil
	a.mu.Unlock()

	a.applyMu.Lock()
	defer a.applyMu.Unlock()

	p, err := openPanel()
	if err != nil {
		return
	}
	if err := p.OnTunnelsChanged(a.mgr.Tunnels()); err != nil {
		log.Printf("同步节点链接后端失败: %v", err)
	}
}

// close 停掉待执行的通知。进程退出时调用，避免关到一半又去写后端。
func (a *configApplier) close() {
	a.mu.Lock()
	a.closed = true
	if a.timer != nil {
		a.timer.Stop()
		a.timer = nil
	}
	a.mu.Unlock()
}
