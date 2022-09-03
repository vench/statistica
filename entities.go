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

// ItemRow this struct represent one row of statistic.
type ItemRow struct {
	Dimensions map[string]interface{}
	Metrics    map[string]interface{}
}

type ItemsResponse struct {
	Rows  []*ItemRow
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

	Limit   int
	Offset  int
	SortBy  []*ItemsRequestOrder
	Groups  []string
	Metrics []string
	Filters []*ItemsRequestFilter
}

// Metric this struct describe metrics model.
type Metric struct {
	// Name contains name for represent metric.
	Name string

	// Description contains description of metric.
	Description string

	// Expression contains sql expression for computed statistic metric.
	Expression string
}

// DimensionKey special type for represent dimensions key.
type DimensionKey string

// Dimension this struct describe dimensions model.
type Dimension struct {
	// Name contains name for represent column.
	Name DimensionKey

	// Description contains description of column.
	Description string

	// Expression contains sql expression for column.
	Expression string
}
