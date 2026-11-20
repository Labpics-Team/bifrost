package vectorstore

import (
	"context"
	"errors"
	"net"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrNamespaceNotFound identifies a lost namespace that must be recreated
// before cache operations can resume. It is distinct from a missing point.
var ErrNamespaceNotFound = errors.New("vectorstore: namespace not found")

// IsUnavailable reports transport failures or lost namespaces that allow
// optional vector-store initialization to be deferred or retried. Schema,
// request, and access errors remain actionable failures.
func IsUnavailable(err error) bool {
	if errors.Is(err, ErrNamespaceNotFound) {
		return true
	}
	// A known service status takes precedence over a network wrapper, which
	// may also carry a schema or authorization response from a reachable store.
	if serviceStatus, ok := status.FromError(err); ok {
		switch serviceStatus.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
			return true
		default:
			return false
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var networkErr net.Error
	return errors.As(err, &networkErr)
}
