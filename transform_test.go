package statistica

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_UnionItemsResponse(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name     string
		input    []*ItemsResponse
		expected *ItemsResponse
	}{
		{
			name: "empty",
		},
	}

	for i := range tt {
		tc := tt[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := UnionItemsResponse(tc.input...)
			require.Equal(t, tc.expected, c)
		})
	}
}

func Test_UnionValuesResponse(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name     string
		input    []*ValuesResponse
		expected *ValuesResponse
	}{
		{
			name: "empty",
		},
	}

	for i := range tt {
		tc := tt[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := UnionValuesResponse(tc.input...)
			require.Equal(t, tc.expected, c)
		})
	}
}

func Test_unionValueResponse(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name     string
		a, b     *ValueResponse
		expected *ValueResponse
	}{
		{
			name: "empty",
		},
	}

	for i := range tt {
		tc := tt[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			unionValueResponse(tc.a, tc.b)
			require.Equal(t, tc.expected, tc.a)
		})
	}
}

func Test_unionRowResponse(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name     string
		a, b     *ItemRow
		expected *ItemRow
	}{
		{
			name: "empty",
		},
		{
			name: "merge all",
			a: &ItemRow{
				Metrics: map[string]ValueNumber{
					"a": 100,
					"c": 400,
				},
			},
			b: &ItemRow{
				Metrics: map[string]ValueNumber{
					"a": 100,
					"b": 300,
				},
			},
			expected: &ItemRow{
				Metrics: map[string]ValueNumber{
					"a": 200,
					"b": 300,
					"c": 400,
				},
			},
		},
	}

	for i := range tt {
		tc := tt[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			unionRowResponse(tc.a, tc.b)
			require.Equal(t, tc.expected, tc.a)
		})
	}
}
