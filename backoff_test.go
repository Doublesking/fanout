package main

import (
	"testing"
	"time"
)

// 退避原来每次重连都从 5 秒重新起步，于是"连上几十秒又掉"的出口永远在 5 秒空转。
// 起步退避要随最近的掉线频率往上走，到熔断线直接拉到上限。
func TestInitialBackoffGrowsWithFlapRate(t *testing.T) {
	m := mgrWith(nil)
	tn := &Tunnel{Slot: 1, Node: Node{HostName: "jp-01", CountryCode: "JP"}}

	if got := m.initialBackoff(tn); got != reconnectBackoffMin {
		t.Fatalf("没掉过线应当从最小退避起步，实际 %v", got)
	}

	now := time.Now()
	for i := 0; i < flapSwapAfter; i++ {
		tn.noteFlap(now)
	}
	if got := m.initialBackoff(tn); got != reconnectBackoffMin*backoffSlowFactor {
		t.Fatalf("掉线到 %d 次应当放慢起步退避，实际 %v", flapSwapAfter, got)
	}

	for i := flapSwapAfter; i < flapCircuitBreaker; i++ {
		tn.noteFlap(now)
	}
	if got := m.initialBackoff(tn); got != reconnectBackoffMax {
		t.Fatalf("到熔断线应当退避到上限，实际 %v", got)
	}
}

// 掉线记录只统计窗口内的：一次网络抽风不该把出口永久钉成"惯犯"。
func TestFlapWindowForgetsOldDrops(t *testing.T) {
	var tn Tunnel
	old := time.Now().Add(-flapWindow - time.Minute)
	tn.noteFlap(old)
	tn.noteFlap(old)

	if got := tn.flapRate(time.Now()); got != 0 {
		t.Fatalf("窗口外的掉线不该计入，实际 %d", got)
	}

	if n := tn.noteFlap(time.Now()); n != 1 {
		t.Fatalf("窗口内只该有 1 次，实际 %d", n)
	}
	if got := tn.flapRate(time.Now()); got != 1 {
		t.Fatalf("flapRate 应当也是 1，实际 %d", got)
	}

	tn.clearFlaps()
	if got := tn.flapRate(time.Now()); got != 0 {
		t.Fatalf("清空后应当为 0，实际 %d", got)
	}
}

// 熔断告警一件事只报一次，稳定下来之后还能再报。
func TestBreakerNotifiedOnlyOnce(t *testing.T) {
	var tn Tunnel
	if !tn.markBreakerNotified() {
		t.Fatal("首次触发熔断应当返回 true（调用方据此打日志）")
	}
	if tn.markBreakerNotified() {
		t.Fatal("同一次熔断不该反复刷日志")
	}
	tn.clearFlaps()
	if !tn.markBreakerNotified() {
		t.Fatal("掉线记录清零后应当能再次告警")
	}
}

// 同地区候选枯竭时放宽到任意地区兜底，但首选仍然是原地区的原节点。
func TestCandidatesForFallsBackToOtherRegions(t *testing.T) {
	withResidentialOnly(t, boolPtr(false))
	m := mgrWith([]Node{
		{HostName: "ua-01", CountryCode: "UA"},
		{HostName: "jp-01", CountryCode: "JP"},
		{HostName: "us-01", CountryCode: "US"},
	}, "ua-01")
	tn := m.tunnels[1]

	got := hostsOf(m.candidatesFor(tn))
	if len(got) < 2 {
		t.Fatalf("UA 只有一个节点时应当放宽到其他地区，实际 %v", got)
	}
	if got[0] != "ua-01" {
		t.Fatalf("首选仍然应当是原地区的原节点，实际 %v", got)
	}
}

// 同地区候选充足时不该混进别的地区：出口的国家是用户选过的。
func TestCandidatesForKeepsRegionWhenEnough(t *testing.T) {
	withResidentialOnly(t, boolPtr(false))
	m := mgrWith([]Node{
		{HostName: "jp-01", CountryCode: "JP"},
		{HostName: "jp-02", CountryCode: "JP"},
		{HostName: "us-01", CountryCode: "US"},
	}, "jp-01")
	tn := m.tunnels[1]

	got := hostsOf(m.candidatesFor(tn))
	if len(got) < 2 {
		t.Fatalf("JP 有 2 个节点，候选不该是空的，实际 %v", got)
	}
	for _, h := range got {
		if h == "us-01" {
			t.Fatalf("JP 候选够用时不该混进 US，实际 %v", got)
		}
	}
}

// 抖动节点被跳过、且同地区没有备胎时，应当拿跨地区节点补上，而不是回退到坏节点。
func TestCandidatesForFlappedHeadUsesOtherRegion(t *testing.T) {
	withResidentialOnly(t, boolPtr(false))
	m := mgrWith([]Node{
		{HostName: "ua-01", CountryCode: "UA"},
		{HostName: "jp-01", CountryCode: "JP"},
	}, "ua-01")
	tn := m.tunnels[1]
	tn.markFlap("ua-01")

	got := hostsOf(m.candidatesFor(tn))
	if len(got) == 0 {
		t.Fatal("应当给出候选")
	}
	if got[0] == "ua-01" {
		t.Fatalf("抖动节点不该排第一，实际 %v", got)
	}
}
