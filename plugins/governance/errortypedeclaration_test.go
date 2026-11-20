package governance

import (
	"context"
	"fmt"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/grant"
)

// Every Decision that refuses a request. Hand-kept: Go cannot enumerate constants.
// A missed entry surfaces at runtime as schemas.ErrorTypeOther, which is alarmed on.
var allRefusalDecisions = []Decision{
	DecisionAccessNotFound,
	DecisionAccessBlocked,
	DecisionAccessUnresolved,
	DecisionModelBlocked,
	DecisionProviderBlocked,
	DecisionRateLimited,
	DecisionTokenLimited,
	DecisionRequestLimited,
	DecisionBudgetExceeded,
	DecisionMCPToolBlocked,
}

// The status code cannot tell these apart: three decisions answer 403, and our 429
// differs from an upstream's.
func TestEveryRefusalDeclaresAnErrorType(t *testing.T) {
	for _, decision := range allRefusalDecisions {
		t.Run(string(decision), func(t *testing.T) {
			// decide is nil-safe on ctx and touches no plugin state when refusing.
			_, bifrostErr := (&GovernancePlugin{}).decide(nil, &EvaluationResult{
				Decision: decision,
				Reason:   "refused for test",
			})
			if bifrostErr == nil {
				t.Fatalf("decision %q must refuse the request with an error", decision)
			}
			if bifrostErr.ExtraFields.ErrorType == "" {
				t.Fatalf("decision %q declared no ErrorType; it will be counted as %q",
					decision, schemas.ErrorTypeOther)
			}
			got := schemas.ClassifyErrorType(bifrostErr, schemas.ChatCompletionRequest)
			if got == schemas.ErrorTypeOther {
				t.Errorf("decision %q classified as %q", decision, schemas.ErrorTypeOther)
			}
			if got != bifrostErr.ExtraFields.ErrorType {
				t.Errorf("declared %q but classified as %q; the declaration must win",
					bifrostErr.ExtraFields.ErrorType, got)
			}
		})
	}
}

// Not merely non-empty: a copy-paste declaring everything policy_access_denied would
// pass the test above while making the metric useless.
func TestRefusalsDeclareTheIntendedErrorType(t *testing.T) {
	for decision, want := range map[Decision]schemas.ErrorType{
		DecisionModelBlocked:    schemas.ErrorTypePolicyModelBlocked,
		DecisionProviderBlocked: schemas.ErrorTypePolicyProviderBlocked,
		DecisionAccessNotFound:  schemas.ErrorTypePolicyAccessDenied,
		DecisionAccessBlocked:   schemas.ErrorTypePolicyAccessDenied,
		DecisionBudgetExceeded:  schemas.ErrorTypePolicyBudgetExceeded,
		DecisionRateLimited:     schemas.ErrorTypePolicyRateLimited,
		DecisionTokenLimited:    schemas.ErrorTypePolicyRateLimited,
		DecisionRequestLimited:  schemas.ErrorTypePolicyRateLimited,
		DecisionMCPToolBlocked:  schemas.ErrorTypePolicyToolBlocked,
		// Not a policy refusal: the deployment is misassembled, not the caller.
		DecisionAccessUnresolved: schemas.ErrorTypeBifrostInternal,
	} {
		_, bifrostErr := (&GovernancePlugin{}).decide(nil, &EvaluationResult{Decision: decision})
		if bifrostErr == nil {
			t.Fatalf("decision %q must refuse the request with an error", decision)
		}
		if got := bifrostErr.ExtraFields.ErrorType; got != want {
			t.Errorf("decision %q declared %q, want %q", decision, got, want)
		}
	}
}

// Type still carries the raw decision, so clients matching on it keep working.
func TestRefusalStillCarriesRawDecisionInType(t *testing.T) {
	for _, decision := range allRefusalDecisions {
		_, bifrostErr := (&GovernancePlugin{}).decide(nil, &EvaluationResult{Decision: decision})
		if bifrostErr == nil || bifrostErr.Type == nil {
			t.Fatalf("decision %q lost its wire-level Type", decision)
		}
		if *bifrostErr.Type != string(decision) {
			t.Errorf("decision %q surfaced as Type %q", decision, *bifrostErr.Type)
		}
	}
}

type rateLimitFeedbackStore struct {
	GovernanceStore
	decision Decision
	err      error
}

func (s rateLimitFeedbackStore) CheckRateLimits(context.Context, []schemas.Limit, map[string]int64, map[string]int64) (Decision, error) {
	return s.decision, s.err
}

func TestRateLimitFeedbackResolver(t *testing.T) {
	resolver := &BudgetResolver{store: rateLimitFeedbackStore{
		decision: DecisionRequestLimited,
		err: fmt.Errorf("wrapped: %w", &rateLimitRefusal{
			reason: "counter full", retryAfter: 8000,
		}),
	}}
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	result := resolver.evaluateLimits(ctx, &EvaluationRequest{}, grant.NewLimits(nil, []schemas.Limit{{ID: "counter"}}))
	if result.Decision != DecisionRequestLimited || result.RetryAfter != 8000 {
		t.Fatalf("store feedback must reach the refusal unchanged: %#v", result)
	}
	_, refusal := (&GovernancePlugin{}).decide(nil, result)
	if refusal == nil || refusal.StatusCode == nil || *refusal.StatusCode != 429 ||
		refusal.ExtraFields.RetryAfter != 8000 {
		t.Fatalf("resolver feedback must reach the wire fields: %#v", refusal)
	}
}

func TestRateLimitFeedbackRefusal(t *testing.T) {
	for _, decision := range allRefusalDecisions {
		t.Run(string(decision), func(t *testing.T) {
			_, refusal := (&GovernancePlugin{}).decide(nil, &EvaluationResult{
				Decision: decision, RetryAfter: 8000,
			})
			if refusal == nil {
				t.Fatal("expected a refusal")
			}
			if isRateLimitViolation(decision) {
				if refusal.ExtraFields.RetryAfter != 8000 {
					t.Fatalf("rate-limit feedback was dropped: %#v", refusal.ExtraFields)
				}
				if refusal.Type == nil || *refusal.Type != string(decision) || refusal.StatusCode == nil || *refusal.StatusCode != 429 {
					t.Fatal("existing machine decision and HTTP status must stay intact")
				}
			} else if refusal.ExtraFields.RetryAfter != 0 {
				t.Fatal("access, budget, and internal failures must not inherit rate-limit feedback")
			}
		})
	}
	_, allowed := (&GovernancePlugin{}).decide(nil, &EvaluationResult{
		Decision: DecisionAllow, RetryAfter: 8000,
	})
	if allowed != nil {
		t.Fatal("an allowed request must not acquire an error")
	}
}
