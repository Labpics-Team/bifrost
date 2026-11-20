package schemas

import (
	"reflect"
	"testing"
)

func TestListModelsPaginationLargePage(t *testing.T) {
	response := &BifrostListModelsResponse{Data: []Model{{ID: "A"}, {ID: "B"}, {ID: "C"}}}
	first := response.ApplyPagination(1, "")
	if first.NextPageToken == "" {
		t.Fatal("first page must have a continuation")
	}
	last := response.ApplyPagination(int(^uint(0)>>1), first.NextPageToken)
	if !reflect.DeepEqual(last.Data, response.Data[1:]) || last.NextPageToken != "" {
		t.Fatalf("large page must contain the remaining models: %#v", last)
	}
}
