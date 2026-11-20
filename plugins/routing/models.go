package routing

import (
	"context"
	"slices"
	"sort"

	"github.com/google/cel-go/common"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/parser"
	"github.com/maximhq/bifrost/core/schemas"
)

// ListModels publishes global, terminal model-name rules. This is discovery of
// declared names, not a prediction of conditional or scoped request routing.
// Admission belongs to the caller; access only filters the declared targets.
func (p *RoutingPlugin) ListModels(ctx context.Context, access schemas.Access) ([]schemas.Model, error) {
	models := make([]schemas.Model, 0)
	parser, err := parser.NewParser()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, rule := range p.rules.GetScopedRules(ctx, "global", "") {
		if rule.ChainRule {
			continue
		}
		parsed, errs := parser.Parse(common.NewTextSource(rule.CelExpression))
		if parsed == nil || errs != nil && len(errs.GetErrors()) > 0 {
			continue
		}
		name := routingModelName(parsed.Expr())
		_, requestModel := schemas.ParseModelString(name, "")
		if name == "" || requestModel != name || seen[name] {
			continue
		}
		positive := false
		for _, target := range rule.Targets {
			positive = positive || target.Weight > 0
		}
		allowed, selectable := true, false
		for _, target := range rule.Targets {
			// The routing engine picks uniformly when all nonnegative weights are zero.
			if target.Weight < 0 || positive && target.Weight == 0 {
				continue
			}
			selectable = true
			if target.Provider == nil || *target.Provider == "" {
				allowed = false // Its provider depends on a concrete inference request.
				break
			}
			model := name
			if target.Model != nil && *target.Model != "" {
				model = *target.Model
			}
			if access != nil {
				if !access.IsModelAllowed(*target.Provider, model) {
					allowed = false
					break
				}
				keys, restricted := access.KeysForModel(*target.Provider, model)
				if restricted && (len(keys) == 0 || target.KeyID != nil && *target.KeyID != "" && !slices.Contains(keys, *target.KeyID)) {
					allowed = false
					break
				}
			}
		}
		if !selectable {
			continue // The routing engine skips rules without a usable target.
		}
		seen[name] = true // A denied first declaration cannot reveal a later one.
		if allowed {
			models = append(models, schemas.Model{ID: name})
		}
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	return models, nil
}

func routingModelName(expr celast.Expr) string {
	if expr.Kind() != celast.CallKind {
		return ""
	}
	call := expr.AsCall()
	args := call.Args()
	if call.FunctionName() != operators.Equals || len(args) != 2 {
		return ""
	}
	for i, arg := range args {
		other := args[1-i]
		if arg.Kind() == celast.IdentKind && arg.AsIdent() == "model" && other.Kind() == celast.LiteralKind {
			name, _ := other.AsLiteral().Value().(string)
			return name
		}
	}
	return ""
}
