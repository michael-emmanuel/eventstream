package fault

import (
	"errors"
	"testing"
)

func TestFaultsOffByDefault(t *testing.T) {
	in := New()
	if in.BeforeDiskWrite() != nil || in.BeforeReplicaFetch() != nil || in.ControllerUnreachable() || in.CrashOnProduce() {
		t.Fatal("fault active by default")
	}
}

func TestFaultsFire(t *testing.T) {
	in := New()
	in.Set(Config{DiskWriteErrorRate: 1, IsolateFromBrokers: true, CrashOnNextProduce: true})
	if !errors.Is(in.BeforeDiskWrite(), ErrInjected) || !errors.Is(in.BeforeReplicaFetch(), ErrInjected) {
		t.Fatal("faults did not fire")
	}
	if !in.CrashOnProduce() || in.CrashOnProduce() {
		t.Fatal("crash fault must fire exactly once")
	}
	if in.Injected() < 2 {
		t.Fatal("counter")
	}
}
