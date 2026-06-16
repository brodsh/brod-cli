package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// This file holds the best-effort read-only credential probe behind
// `--require-readonly` (A-5 / F1.2-lite). It uses ONLY a DescribeACLs read; it
// does not — and cannot — change anything. Full credential enforcement is a
// SaaS concern (F1.2); here we surface obvious over-privilege when the broker
// lets us see the ACLs, and otherwise say so honestly.

// ReadOnlyCheck is the outcome of the ACL probe.
type ReadOnlyCheck struct {
	// Supported is false when ACLs couldn't be described (no authorizer, or the
	// principal lacks Describe on the cluster). Then Violations is empty and the
	// caller should treat the result as "unverified", not "clean".
	Supported bool
	// Violations lists detected grants that imply more than read-only access:
	// any WRITE/ALTER/DELETE/CREATE, or topic READ (which permits consuming
	// message payloads). Each entry is human-readable.
	Violations []string
}

// CheckReadOnly issues a single DescribeACLs request for ALLOW grants and
// flags any that exceed read-only metadata access. It never mutates the
// cluster. A nil error with Supported=false means the cluster didn't expose
// ACLs (common: no authorizer configured) — that is not a failure.
func (c *Client) CheckReadOnly(ctx context.Context) (ReadOnlyCheck, error) {
	// Describe every ALLOW ACL across all resource types/operations.
	b := kadm.NewACLs().
		AnyResource().
		ResourcePatternType(kadm.ACLPatternAny).
		Operations(kadm.OpAny).
		Allow().
		AllowHosts().
		Deny().
		DenyHosts()

	results, err := c.adm.DescribeACLs(ctx, b)
	if err != nil {
		// Most clusters without an authorizer reject this. Treat as unverified
		// rather than fatal — the banner already documents read-only intent.
		return ReadOnlyCheck{Supported: false}, nil
	}

	chk := ReadOnlyCheck{Supported: true}
	for _, res := range results {
		if res.Err != nil {
			// A filter that errored means we can't fully verify.
			chk.Supported = false
			continue
		}
		for _, acl := range res.Described {
			if acl.Permission != kmsg.ACLPermissionTypeAllow {
				continue
			}
			if v := writeishViolation(acl); v != "" {
				chk.Violations = append(chk.Violations, v)
			}
		}
	}
	return chk, nil
}

// writeishViolation returns a description if the ACL grants more than read-only
// metadata access, else "". Topic READ is flagged because READ permits Fetch
// (consuming message payloads) — exactly what brod must never be able to do.
func writeishViolation(a kadm.DescribedACL) string {
	res := fmt.Sprintf("%s %q", a.Type, a.Name)
	switch a.Operation {
	case kadm.OpAll:
		return fmt.Sprintf("ALL granted on %s (includes write+consume)", res)
	case kadm.OpWrite:
		return fmt.Sprintf("WRITE granted on %s", res)
	case kadm.OpAlter, kadm.OpAlterConfigs:
		return fmt.Sprintf("ALTER granted on %s", res)
	case kadm.OpDelete:
		return fmt.Sprintf("DELETE granted on %s", res)
	case kadm.OpCreate:
		return fmt.Sprintf("CREATE granted on %s", res)
	case kadm.OpRead:
		// READ on a topic permits consuming message data; READ on a group is
		// benign (it's how committed-offset reads work).
		if a.Type == kmsg.ACLResourceTypeTopic {
			return fmt.Sprintf("topic READ granted on %s (permits consuming message payloads)", res)
		}
	}
	return ""
}
