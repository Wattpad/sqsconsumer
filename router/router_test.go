package router

import (
	"testing"

	"github.com/Wattpad/sqsconsumer"
	"golang.org/x/net/context"
)

func TestTypeAdd(t *testing.T) {
	r := New()
	n := len(r)

	// adding one route should increase the length
	r.Add("type1", func(ctx context.Context, msg string) error { return nil })
	if len(r) != n+1 {
		t.Fatalf("Add(type1) len = %d; want %d", len(r), n+1)
	}

	// adding the same route should replace it so no length change
	r.Add("type1", func(ctx context.Context, msg string) error { return nil })
	if len(r) != n+1 {
		t.Fatalf("Add(type1) len = %d; want %d", len(r), n+1)
	}

	// adding another route should increase length further
	n = len(r)
	r.Add("type2", func(ctx context.Context, msg string) error { return nil })
	if len(r) != n+1 {
		t.Fatalf("Add(type2) len = %d; want %d", len(r), n+1)
	}
}

func TestTypeHandler(t *testing.T) {
	capturer := func(s *string) sqsconsumer.MessageHandlerFunc {
		return func(_ context.Context, msg string) error {
			*s = msg
			return nil
		}
	}

	var a, b string
	r := New()
	r.Add("a", capturer(&a))
	r.Add("b", capturer(&b))

	testCases := []struct {
		msg    string
		isErr  bool
		result *string
	}{
		{`{"type":"a","code":1}`, false, &a},
		{`{"type":"b","code":2}`, false, &b},
		{`{"Type":"a","code":3}`, false, &a},
		{`{"code":3}`, true, nil},            // missing type
		{`{"type":"c","code":3}`, true, nil}, // unknown type
		{`}`, true, nil},                     // invalid json
	}

	for _, tc := range testCases {
		err := r.Handler(context.Background(), tc.msg)
		if tc.isErr && err == nil {
			t.Fatalf("Handler(%s) err nil but should have been non-nil", tc.msg)
		}
		if !tc.isErr && err != nil {
			t.Fatalf("Handler(%s) = Error(%s); want nil", tc.msg, err)
		}
		if tc.result != nil && *tc.result != tc.msg {
			t.Fatalf("Handler(%s) result = %s; want %s", tc.msg, *tc.result, tc.msg)
		}
	}
}
