package statistica

// ValueResponse this struct represents value response.
type ValueResponse struct {
	// Name value from query.
	Name []interface{} `json:"name"`

	// Key value from DB.
	Key []interface{} `json:"key"`

	//	Count size by value Key.
	Count interface{} `json:"count"`
}

// ValuesResponse this struct represents values response.
type ValuesResponse struct {
	Values []*ValueResponse `json:"values"`
}

// ItemsResponse this struct represents group response.
type ItemsResponse struct {
	Rows  []*ItemRow
	Total uint64
}

// ItemRow this struct represent one row of statistic.
type ItemRow struct {
	Dimensions map[string]interface{}
	Metrics    map[string]interface{}
}

// ItemsRequestFilter this struct represents request filter.
type ItemsRequestFilter struct {
	Key       string
	Values    []interface{}
	Condition Condition
}

// ItemsRequestOrder this struct represents request order options.
type ItemsRequestOrder struct {
	Key       string
	Direction string
}

// ItemsRequest this struct represents request query.
type ItemsRequest struct {
	Limit  int
	Offset int

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
