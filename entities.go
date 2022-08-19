package statistica

import "time"

type ValueResponse struct {
	Name  []interface{} `json:"name"`
	Key   []interface{} `json:"key"`
	Count interface{}   `json:"count"`
}

type ValuesResponse struct {
	Values []*ValueResponse `json:"values"`
}

type ItemResponse struct {
	Dimensions map[string]interface{}
	Metrics    map[string]interface{}
}

type ItemsResponse struct {
	Rows  []*ItemResponse
	Total uint64
}

type ItemsRequestFilter struct {
	Key       string
	Values    []interface{}
	Condition string
}

type ItemsRequestOrder struct {
	Key       string
	Direction string
}

type ItemsRequest struct {
	DateFrom time.Time
	DateTo   time.Time
	Limit    int
	Offset   int
	SortBy   []*ItemsRequestOrder
	Groups   []string
	Metrics  []string
	Filters  []*ItemsRequestFilter
}

type ValueType string

type valueTypeDB string

type DimensionKey string

type Metric struct {
	Key         string
	ValueType   ValueType
	ValueTotal  string
	ValueTypeDB valueTypeDB
	// TODO rename to Expression.
	ValueDB string
}

type Dimension struct {
	Key         DimensionKey
	ValueType   ValueType
	ValueTypeDB valueTypeDB
	// TODO rename to Expression.
	KeyDB string
}
