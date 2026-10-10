package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubNodes 造一个不碰网络的节点拉取实现，并记下被调了几次。
func stubNodes(calls *int32, nodes []Node) func(time.Duration) ([]Node, error) {
	return func(time.Duration) ([]Node, error) {
		atomic.AddInt32(calls, 1)
		return nodes, nil
	}
}

// 缓存没过期就不该再打上游：重连循环一轮一次请求，11 条隧道同时失败
// 就是 11 个并发请求，很容易被限流。
func TestRefreshNodesIfStaleReusesCache(t *testing.T) {
	m := NewManager(4, t.TempDir())
	var calls int32
	m.fetchNodes = stubNodes(&calls, []Node{{HostName: "jp-01", CountryCode: "JP"}})

	if _, err := m.refreshNodesIfStale(); err != nil {
		t.Fatalf("首次刷新失败: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("首次应当真的拉一次，实际 %d 次", got)
	}

	for i := 0; i < 5; i++ {
		if _, err := m.refreshNodesIfStale(); err != nil {
			t.Fatalf("复用缓存失败: %v", err)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("缓存没过期不该再拉，实际拉了 %d 次", got)
	}

	// 把时间戳拨到过期之前，下一次就该重新拉了
	m.mu.Lock()
	m.fetched = time.Now().Add(-nodeCacheTTL - time.Second)
	m.mu.Unlock()
	if _, err := m.refreshNodesIfStale(); err != nil {
		t.Fatalf("过期后刷新失败: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("缓存过期应当重新拉，实际 %d 次", got)
	}
}

// 并发调用只该打上游一次：其余的等第一个拉完直接复用。
func TestRefreshNodesIfStaleCoalescesConcurrentCalls(t *testing.T) {
	m := NewManager(4, t.TempDir())
	var calls int32
	release := make(chan struct{})
	m.fetchNodes = func(time.Duration) ([]Node, error) {
		atomic.AddInt32(&calls, 1)
		<-release // 卡住，让其他调用者都堵在锁上
		return []Node{{HostName: "jp-01", CountryCode: "JP"}}, nil
	}

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = m.refreshNodesIfStale()
		}()
	}
	time.Sleep(80 * time.Millisecond) // 让所有 goroutine 都跑到锁前
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("%d 个并发调用只该打上游 1 次，实际 %d 次", n, got)
	}
}

// 上游抽风返回空列表时不能把手里能用的节点全冲掉。
func TestRefreshNodesKeepsCacheOnEmptyUpstream(t *testing.T) {
	m := NewManager(4, t.TempDir())
	var calls int32
	m.fetchNodes = stubNodes(&calls, []Node{{HostName: "jp-01", CountryCode: "JP"}})
	if _, err := m.RefreshNodes(); err != nil {
		t.Fatalf("首次刷新失败: %v", err)
	}

	m.fetchNodes = stubNodes(&calls, nil)
	if _, err := m.RefreshNodes(); err == nil {
		t.Fatal("上游返回空列表应当报错，而不是静默清空缓存")
	}
	got := m.NodesOnly()
	if len(got) != 1 || got[0].HostName != "jp-01" {
		t.Fatalf("空列表不该把缓存冲掉，实际 %v", got)
	}
}

// 拉取失败时也不该动缓存。
func TestRefreshNodesKeepsCacheOnFetchError(t *testing.T) {
	m := NewManager(4, t.TempDir())
	var calls int32
	m.fetchNodes = stubNodes(&calls, []Node{{HostName: "jp-01", CountryCode: "JP"}})
	if _, err := m.RefreshNodes(); err != nil {
		t.Fatalf("首次刷新失败: %v", err)
	}

	m.fetchNodes = func(time.Duration) ([]Node, error) { return nil, errStub }
	if _, err := m.RefreshNodes(); err == nil {
		t.Fatal("拉取失败应当把错误抛出来")
	}
	if got := m.NodesOnly(); len(got) != 1 {
		t.Fatalf("拉取失败不该动缓存，实际 %v", got)
	}
}

// 节点列表新鲜度判定：空列表一律算不新鲜（否则永远拉不到东西）。
func TestNodesFreshRequiresContent(t *testing.T) {
	m := NewManager(4, t.TempDir())
	if m.nodesFresh() {
		t.Fatal("还没拉过就不该算新鲜")
	}
	m.mu.Lock()
	m.nodes = []Node{{HostName: "jp-01"}}
	m.fetched = time.Now()
	m.mu.Unlock()
	if !m.nodesFresh() {
		t.Fatal("刚拉到的列表应当算新鲜")
	}
}

var errStub = errStubType("拉取失败")

type errStubType string

func (e errStubType) Error() string { return string(e) }
