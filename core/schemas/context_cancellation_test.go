package schemas

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestScopedCancelPropagatesToStandardChild(t *testing.T) {
	root := NewBifrostContext(context.Background(), NoDeadline)
	name := "synthetic-plugin"
	scope := root.WithPluginScope(&name)
	scope.Cancel()

	// Creating a child after cancellation makes the closed Done / nil Err
	// defect deterministic: the standard library observes both synchronously.
	child, cancel := context.WithTimeout(root, time.Second)
	defer cancel()
	if root.Err() != context.Canceled || scope.Err() != context.Canceled || child.Err() != context.Canceled {
		t.Fatal("scope cancellation did not publish the same error to request and child")
	}
	if root.Done() != scope.Done() {
		t.Fatal("scope detached cancellation from the request")
	}
	root.Cancel()
	scope.Cancel()
	if root.Err() != context.Canceled {
		t.Fatal("repeated cancellation changed the published error")
	}
}

func TestScopedCancelWithActiveObserver(t *testing.T) {
	for _, parentKind := range []string{"root", "scope", "nested scope"} {
		t.Run(parentKind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root := NewBifrostContext(context.Background(), NoDeadline)
				name := "synthetic-plugin"
				scope := root.WithPluginScope(&name)
				nested := scope.WithPluginScope(&name)
				var parent context.Context = root
				if parentKind == "scope" {
					parent = scope
				} else if parentKind == "nested scope" {
					parent = nested
				}
				child, cancel := context.WithTimeout(parent, time.Second)
				defer cancel()
				scope.ReleasePluginScope()
				nested.ReleasePluginScope()
				nested.Cancel()
				synctest.Wait()
				if root.Err() != context.Canceled || scope.Err() != context.Canceled || nested.Err() != context.Canceled || child.Err() != context.Canceled {
					t.Fatal("released or nested scope lost request cancellation")
				}
				root.Cancel()
				scope.Cancel()
				nested.Cancel()
			})
		})
	}
}

func TestChildTimeoutPreservesRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root := NewBifrostContext(context.Background(), NoDeadline)
		name := "synthetic-plugin"
		scope := root.WithPluginScope(&name)
		child, cancel := context.WithTimeout(scope.Root(), 250*time.Millisecond)
		defer cancel()
		time.Sleep(250 * time.Millisecond)
		synctest.Wait()
		if child.Err() != context.DeadlineExceeded || root.Err() != nil || scope.Err() != nil {
			t.Fatal("child deadline canceled the request")
		}
		select {
		case <-root.Done():
			t.Fatal("request Done closed after child timeout")
		default:
		}
		root.Cancel()
		if scope.Err() != context.Canceled || child.Err() != context.DeadlineExceeded {
			t.Fatal("request cancellation changed an already published child error")
		}
	})
}

func TestCancellationKeepsFirstError(t *testing.T) {
	for _, first := range []string{"local", "parent deadline"} {
		t.Run(first, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancelParent := context.WithTimeout(context.Background(), time.Second)
				defer cancelParent()
				root := NewBifrostContext(parent, NoDeadline)
				var want error = context.DeadlineExceeded
				if first == "local" {
					want = context.Canceled
					root.Cancel()
				}
				time.Sleep(time.Second)
				synctest.Wait()
				root.Cancel()
				if root.Err() != want || context.Cause(root) != want {
					t.Fatalf("first cancellation changed: Err=%v Cause=%v want=%v", root.Err(), context.Cause(root), want)
				}
			})
		})
	}
}

func TestCancellationCausePreservesFirstCancellation(t *testing.T) {
	for _, first := range []string{"local", "parent"} {
		t.Run(first, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				type metadataKey struct{}
				metadata := &struct{ value string }{"synthetic-request-metadata"}
				parent, cancelParent := context.WithCancelCause(context.WithValue(context.Background(), metadataKey{}, metadata))
				defer cancelParent(nil)
				deadline := time.Now().Add(time.Minute)
				root := NewBifrostContext(parent, deadline)
				defer root.Cancel()
				name := "synthetic-plugin"
				scope := root.WithPluginScope(&name)
				child, cancelChild := context.WithCancel(scope)
				defer cancelChild()
				parentCause := errors.New("synthetic-parent-shutdown")
				var wantCause error = context.Canceled
				if first == "local" {
					root.Cancel()
					synctest.Wait()
					if context.Cause(root) != context.Canceled || context.Cause(child) != context.Canceled {
						t.Fatal("local cancellation did not publish its own cause")
					}
					cancelParent(parentCause)
				} else {
					wantCause = parentCause
					cancelParent(parentCause)
					synctest.Wait()
					root.Cancel()
				}
				synctest.Wait()
				lateChild, cancelLateChild := context.WithCancel(root)
				defer cancelLateChild()
				for _, ctx := range []context.Context{root, scope, child, lateChild} {
					if ctx.Err() != context.Canceled || context.Cause(ctx) != wantCause {
						t.Fatalf("first cancellation changed: Err=%v Cause=%v wantCause=%v", ctx.Err(), context.Cause(ctx), wantCause)
					}
					if ctx.Value(metadataKey{}) != metadata {
						t.Fatal("cancellation metadata isolation lost ordinary parent values")
					}
					if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
						t.Fatal("cancellation metadata isolation lost the request deadline")
					}
				}
			})
		})
	}
}

func TestCancellationPreservesParentTimeoutCause(t *testing.T) {
	for _, deadlineKind := range []string{"inherited", "equal", "later", "earlier"} {
		t.Run(deadlineKind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parentCause := errors.New("synthetic-parent-timeout")
				parent, cancelParent := context.WithTimeoutCause(context.Background(), time.Second, parentCause)
				defer cancelParent()
				inherited, _ := parent.Deadline()
				deadline := NoDeadline
				wantDeadline := inherited
				var wantCause error = parentCause
				switch deadlineKind {
				case "equal":
					deadline = inherited
				case "later":
					deadline = inherited.Add(time.Second)
				case "earlier":
					deadline = inherited.Add(-500 * time.Millisecond)
					wantDeadline = deadline
					wantCause = context.DeadlineExceeded
				}
				root := NewBifrostContext(parent, deadline)
				defer root.Cancel()
				name := "synthetic-plugin"
				scope := root.WithPluginScope(&name)
				child, cancelChild := context.WithCancel(scope)
				defer cancelChild()
				time.Sleep(time.Second)
				synctest.Wait()
				lateChild, cancelLateChild := context.WithCancel(root)
				defer cancelLateChild()
				for _, ctx := range []context.Context{root, scope, child, lateChild} {
					if ctx.Err() != context.DeadlineExceeded || context.Cause(ctx) != wantCause {
						t.Fatalf("timeout cause changed: Err=%v Cause=%v wantCause=%v", ctx.Err(), context.Cause(ctx), wantCause)
					}
					if got, ok := ctx.Deadline(); !ok || !got.Equal(wantDeadline) {
						t.Fatal("timeout inheritance changed the effective deadline")
					}
				}
			})
		})
	}
}
