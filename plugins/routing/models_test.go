package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/grant"
	"github.com/maximhq/bifrost/plugins/routing/rules"
	"github.com/stretchr/testify/require"
)

func TestRoutingModelsLiveDeclaration(t *testing.T) {
	ctx := context.Background()
	store, err := rules.NewLocalStore(ctx, bifrost.NewNoOpLogger(), nil)
	require.NoError(t, err)
	p := &RoutingPlugin{rules: store}
	rule := &tables.TableRoutingRule{ID: "base", Scope: "global", CelExpression: `model == "Public/name"`, Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: bifrost.Ptr("physical-a"), Weight: 1}}}
	require.NoError(t, store.UpsertRule(ctx, rule))
	access := routingModelsAccess(schemas.ProviderPermit{Provider: "openai", AllowedModels: schemas.WhiteList{"physical-a", "physical-b"}, KeyIDs: schemas.WhiteList{"*"}})
	for _, target := range []string{"physical-a", "physical-b"} {
		edited := *rule
		edited.Targets = []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: &target, Weight: 1}}
		require.NoError(t, store.UpsertRule(ctx, &edited))
		got, err := p.ListModels(ctx, access)
		require.NoError(t, err)
		require.Equal(t, []schemas.Model{{ID: "Public/name"}}, got)
		wire, err := json.Marshal(got)
		require.NoError(t, err)
		require.NotContains(t, string(wire), "physical")
		require.NotContains(t, string(wire), "openai")
	}
	require.NoError(t, store.DeleteRule(ctx, rule.ID))
	got, err := p.ListModels(ctx, access)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestRoutingModelsDeclarationAndTargetPermissions(t *testing.T) {
	allowed := schemas.ProviderPermit{Provider: "openai", AllowedModels: schemas.WhiteList{"allowed"}, KeyIDs: schemas.WhiteList{"key-a"}}
	access := routingModelsAccess(allowed)
	target := func(model string, weight float64) tables.TableRoutingTarget {
		return tables.TableRoutingTarget{Provider: bifrost.Ptr("openai"), Model: &model, Weight: weight}
	}
	for _, tc := range []struct {
		name string
		expr string
		edit func(*tables.TableRoutingRule)
		want bool
	}{
		{name: "exact", expr: `model == "Public"`, want: true},
		{name: "native provider prefix", expr: `model == "openai/Public"`},
		{name: "reversed", expr: `"Public" == model`, want: true},
		{name: "parentheses", expr: `(model == 'Public')`, want: true},
		{name: "conditional", expr: `model == "Public" && provider == "openai"`},
		{name: "list", expr: `model in ["Public"]`},
		{name: "literal trap", expr: `"model == Public" == "model == Public"`},
		{name: "scoped", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) { r.Scope = "team"; r.ScopeID = bifrost.Ptr("team") }},
		{name: "disabled", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) { r.Enabled = bifrost.Ptr(false) }},
		{name: "chained", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) { r.ChainRule = true }},
		{name: "unknown provider", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) { r.Targets[0].Provider = nil }},
		{name: "denied model", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) { r.Targets[0].Model = bifrost.Ptr("denied") }},
		{name: "denied key", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) { r.Targets[0].KeyID = bifrost.Ptr("key-b") }},
		{name: "allowed key", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) { r.Targets[0].KeyID = bifrost.Ptr("key-a") }, want: true},
		{name: "partial weighted access", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) {
			r.Targets = []tables.TableRoutingTarget{target("allowed", .5), target("denied", .5)}
		}},
		{name: "zero probability denied", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) {
			r.Targets = []tables.TableRoutingTarget{target("allowed", 1), target("denied", 0)}
		}, want: true},
		{name: "uniform zero weights denied", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) {
			r.Targets = []tables.TableRoutingTarget{target("allowed", 0), target("denied", 0)}
		}},
		{name: "uniform zero weights allowed", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) { r.Targets = []tables.TableRoutingTarget{target("allowed", 0)} }, want: true},
		{name: "no targets", expr: `model == "Public"`, edit: func(r *tables.TableRoutingRule) { r.Targets = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := rules.NewLocalStore(context.Background(), bifrost.NewNoOpLogger(), nil)
			require.NoError(t, err)
			rule := &tables.TableRoutingRule{ID: "rule", Scope: "global", CelExpression: tc.expr, Targets: []tables.TableRoutingTarget{target("allowed", 1)}}
			if tc.edit != nil {
				tc.edit(rule)
			}
			require.NoError(t, store.UpsertRule(context.Background(), rule))
			got, err := (&RoutingPlugin{rules: store}).ListModels(context.Background(), access)
			require.NoError(t, err)
			require.Equal(t, tc.want, len(got) == 1)
		})
	}
	for _, permit := range []schemas.ProviderPermit{
		{Provider: "openai", AllowedModels: schemas.WhiteList{"*"}},
		{Provider: "openai", AllowedModels: schemas.WhiteList{"*"}, KeyIDs: schemas.WhiteList{"*"}, BlacklistedModels: schemas.BlackList{"allowed"}},
	} {
		store, err := rules.NewLocalStore(context.Background(), bifrost.NewNoOpLogger(), nil)
		require.NoError(t, err)
		require.NoError(t, store.UpsertRule(context.Background(), &tables.TableRoutingRule{ID: "rule", Scope: "global", CelExpression: `model == "Public"`, Targets: []tables.TableRoutingTarget{target("allowed", 1)}}))
		got, err := (&RoutingPlugin{rules: store}).ListModels(context.Background(), routingModelsAccess(permit))
		require.NoError(t, err)
		require.Empty(t, got)
	}
}

func TestRoutingModelsFirstDeclarationAndSortedNames(t *testing.T) {
	store, err := rules.NewLocalStore(context.Background(), bifrost.NewNoOpLogger(), nil)
	require.NoError(t, err)
	for i, entry := range []struct{ name, model string }{{"Public", "denied"}, {"Public", "allowed"}, {"Z", "allowed"}, {"A", "allowed"}} {
		require.NoError(t, store.UpsertRule(context.Background(), &tables.TableRoutingRule{ID: fmt.Sprint(i), Scope: "global", Priority: i, CelExpression: fmt.Sprintf("model == %q", entry.name), Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: bifrost.Ptr(entry.model), Weight: 1}}}))
	}
	got, err := (&RoutingPlugin{rules: store}).ListModels(context.Background(), routingModelsAccess(schemas.ProviderPermit{Provider: "openai", AllowedModels: schemas.WhiteList{"allowed"}, KeyIDs: schemas.WhiteList{"*"}}))
	require.NoError(t, err)
	require.Equal(t, []schemas.Model{{ID: "A"}, {ID: "Z"}}, got)
}

func routingModelsAccess(permits ...schemas.ProviderPermit) schemas.Access {
	return grant.NewAccess([]schemas.Permit{grant.NewPermit(grant.PermitVirtualKey, "caller", "Caller", true, false, permits, nil)}, nil, "", nil)
}

func TestRoutingModelsFollowsNativeProviderNamespace(t *testing.T) {
	store, err := rules.NewLocalStore(context.Background(), bifrost.NewNoOpLogger(), nil)
	require.NoError(t, err)
	const provider schemas.ModelProvider = "catalog-test-custom-provider"
	require.NoError(t, store.UpsertRule(context.Background(), &tables.TableRoutingRule{ID: "slash", Scope: "global", CelExpression: `model == "catalog-test-custom-provider/Public"`, Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: bifrost.Ptr("physical"), Weight: 1}}}))
	p := &RoutingPlugin{rules: store}
	before, err := p.ListModels(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, []schemas.Model{{ID: "catalog-test-custom-provider/Public"}}, before)
	schemas.RegisterKnownProvider(provider)
	t.Cleanup(func() { schemas.UnregisterKnownProvider(provider) })
	after, err := p.ListModels(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, after, "native inference strips a registered provider prefix before matching RR")
	schemas.UnregisterKnownProvider(provider)
	restored, err := p.ListModels(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, before, restored)
}
