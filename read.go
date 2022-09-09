package statistica

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"

	"go.uber.org/zap"
)

// ReadRepository common read interface.
type ReadRepository interface {
	// Total returns total rows by query conditions.
	Total(req *ItemsRequest) (uint64, error)
	// Values returns list of allowed values with size by query conditions.
	Values(req *ItemsRequest) ([]*ValueResponse, error)
	// Grouped returns rows metrics by group filtered by query conditions.
	Grouped(req *ItemsRequest) ([]*ItemRow, error)
	// Metrics returns list of allowed metrics.
	Metrics() ([]*Metric, error)
}

// SQLRepository sql implementation of ReadRepository.
type SQLRepository struct {
	conn *sql.DB

	mapDimensions map[DimensionKey]*Dimension
	metrics       []*Metric

	// contains table name or sql expression like table.
	table string

	// contains name column for total value.
	totalColumnName string

	logger *zap.Logger
}

// SQLRepositoryOption todo.
type SQLRepositoryOption func(*SQLRepository)

// LoggerSQLRepositoryOption todo.
func LoggerSQLRepositoryOption(logger *zap.Logger) SQLRepositoryOption {
	return func(repository *SQLRepository) {
		repository.logger = logger
	}
}

// NewSQLRepository returns new instance of SQLRepository.
func NewSQLRepository(
	connection *sql.DB, table string, dimensions []*Dimension, metrics []*Metric, options ...SQLRepositoryOption,
) *SQLRepository {
	mDimensions := make(map[DimensionKey]*Dimension, len(dimensions))
	for i := range dimensions {
		mDimensions[dimensions[i].Name] = dimensions[i]
	}

	r := &SQLRepository{
		conn:          connection,
		table:         table,
		mapDimensions: mDimensions,
		metrics:       metrics,
		logger:        zap.NewNop(),
	}

	for i := range options {
		options[i](r)
	}

	return r
}

// Metrics returns allowed metrics.
func (r *SQLRepository) Metrics() ([]*Metric, error) {
	return r.metrics, nil
}

func makeDestFromTypes(types []*sql.ColumnType) []interface{} {
	dest := make([]interface{}, len(types))
	for i, item := range types {
		if item.DatabaseTypeName() != "" {
			dest[i] = reflect.New(item.ScanType()).Interface()
		} else {
			dest[i] = new(interface{})
		}
	}

	return dest
}

// Total returns total rows by query ItemsRequest.
func (r *SQLRepository) Total(req *ItemsRequest) (uint64, error) {
	r.logger.Debug("request", zap.Reflect("request", req))

	query := ""
	params := make([]interface{}, 0)

	r.applySelectTotal(req, &query)
	query += fmt.Sprintf(" FROM %s", r.table)
	r.applyWhere(req, &query, &params)

	r.logger.Debug("total query SQL", zap.String("query", query))

	rows, err := r.conn.Query(query, params...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}

	r.logger.Debug("types", zap.Reflect("types", types))

	if rows.Next() {
		dest := makeDestFromTypes(types)

		if err = rows.Scan(dest...); err != nil {
			return 0, err
		}

		for i := range dest {
			value := safeNaN(unwrapPointerInterface(dest[i]))
			r.logger.Debug("value", zap.Reflect("value", value))

			if total, ok := castUInt64(value); ok {
				return total, nil
			}

			if total, ok := castInt64(value); ok {
				return uint64(total), nil
			}
		}
	}

	return 0, nil
}

// Values returns values ValueResponse by query ItemsRequest.
func (r *SQLRepository) Values(req *ItemsRequest) ([]*ValueResponse, error) {
	query := ""
	params := make([]interface{}, 0)

	r.applySelectValue(req, &query)
	query += fmt.Sprintf(" FROM %s ", r.table)
	r.applyWhere(req, &query, &params)
	r.applyGroup(req, &query)
	r.applyOrder(req, &query)
	r.applyLimit(req, &query)

	rows, err := r.conn.Query(query, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to exec query: %w, query: %s, params: %v", err, query, params)
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	response := make([]*ValueResponse, 0)
	for rows.Next() {
		dest := makeDestFromTypes(types)

		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}

		itemResp := &ValueResponse{
			Key:   make([]interface{}, len(req.Groups)),
			Name:  make([]interface{}, len(req.Groups)),
			Count: 0,
		}

		for i := range types {
			if len(req.Groups) > i {
				itemResp.Name[i] = req.Groups[i]
				itemResp.Key[i] = unwrapPointerInterface(dest[i])
			} else {
				itemResp.Count = safeNaN(unwrapPointerInterface(dest[i]))
			}
		}
		response = append(response, itemResp)
	}

	return response, nil
}

func (r *SQLRepository) Grouped(req *ItemsRequest) ([]*ItemRow, error) {
	r.logger.Debug("request ItemsRequest", zap.Reflect("request", req))

	query := ""
	params := make([]interface{}, 0)

	r.applySelect(req, &query)
	query += fmt.Sprintf(" FROM %s ", r.table)
	r.applyWhere(req, &query, &params)
	r.applyGroup(req, &query)
	r.applyOrder(req, &query)
	r.applyLimit(req, &query)

	r.logger.Debug("grouped query", zap.String("query", query))

	rows, err := r.conn.Query(query, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to exec query: %w, query: %s, params: %v", err, query, params)
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	r.logger.Debug("types", zap.Reflect("types", types))

	response := make([]*ItemRow, 0)
	for rows.Next() {
		dest := makeDestFromTypes(types)

		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		itemResp := &ItemRow{
			Dimensions: make(map[string]interface{}),
			Metrics:    make(map[string]interface{}),
		}

		for i := range types {
			if len(req.Groups) > i {
				itemResp.Dimensions[req.Groups[i]] = unwrapPointerInterface(dest[i])
			} else {
				itemResp.Metrics[types[i].Name()] = safeNaN(unwrapPointerInterface(dest[i]))
			}
		}
		response = append(response, itemResp)
	}

	return response, nil
}

func (r *SQLRepository) applyGroup(req *ItemsRequest, query *string) {
	dimGroup := make([]string, 0)
	for _, item := range req.Groups {
		field, exists := r.getDimension(DimensionKey(item))
		if !exists {
			continue
		}
		dimGroup = append(dimGroup, field.Expression)
	}
	if len(dimGroup) > 0 {
		*query += ` GROUP BY  ` + strings.Join(dimGroup, ",")
	}
}

func (r *SQLRepository) applyOrder(req *ItemsRequest, query *string) {
	sortBy := make([]string, 0)
	if len(req.SortBy) > 0 {
		for _, item := range req.SortBy {
			field, exists := r.getDimension(DimensionKey(item.Key))
			if !exists {
				continue
			}

			sort := fmt.Sprintf("%s %s", field.Expression, item.Direction)
			sortBy = append(sortBy, sort)
		}
	}

	if len(sortBy) > 0 {
		*query += " ORDER BY " + strings.Join(sortBy, ",")
	}
}

func (r *SQLRepository) Ping() error {
	_, err := r.conn.Exec(`SELECT 1`)
	return err
}

func (r *SQLRepository) getDimension(key DimensionKey) (*Dimension, bool) {
	if dim, ok := r.mapDimensions[key]; ok {
		return dim, true
	}
	return nil, false
}

func (r *SQLRepository) applyWhere(req *ItemsRequest, query *string, params *[]interface{}) {
	where := ""

	if len(req.Filters) > 0 {
		for _, filter := range req.Filters {
			field, exists := r.getDimension(DimensionKey(filter.Key))
			if !exists {
				continue
			}

			key := field.Expression

			if len(key) > 0 && len(filter.Values) > 0 {
				*params = append(*params, filter.Values...)

				if len(where) > 0 {
					where += " AND "
				}

				switch filter.Condition {
				case CondEq, CondEq2:
					in := strings.TrimRight(strings.Repeat("?,", len(filter.Values)), ",")
					where += fmt.Sprintf("%s IN (%s)", key, in)

				case CondNotEq, CondNotEq2:
					in := strings.Repeat("?,", len(filter.Values))
					where += fmt.Sprintf("%s NOT IN (%s)", key, in)

				case CondLike:
					where += fmt.Sprintf("%s LIKE '%s'", key, "%?%")

				case CondGreater:
					where += fmt.Sprintf("%s > ?", key)

				case CondGreaterOrEq:
					where += fmt.Sprintf("%s >= ?", key)

				case CondLess:
					where += fmt.Sprintf("%s < ?", key)

				case CondLessOrEq:
					where += fmt.Sprintf("%s <= ?", key)

				default:
					in := strings.TrimRight(strings.Repeat("?,", len(filter.Values)), ",")
					where += fmt.Sprintf(`%s IN (%s)`, key, in)
				}
			}
		}
	}

	if len(where) > 0 {
		*query += fmt.Sprintf(" WHERE %s", where)
	}
}

func (r *SQLRepository) applySelectTotal(req *ItemsRequest, query *string) {
	if len(req.Groups) > 0 {
		dimGroup := make([]string, 0)
		for _, item := range req.Groups {
			dim, exists := r.getDimension(DimensionKey(item))
			if !exists {
				continue
			}
			dimGroup = append(dimGroup, dim.Expression)
		}

		// TODO remove uniq
		*query += fmt.Sprintf("SELECT uniq(%s) AS %s", strings.Join(dimGroup, ","), r.getTotalColumnName())
		return
	}

	*query += fmt.Sprintf("SELECT count(*) AS %s", r.getTotalColumnName())
}

func (r *SQLRepository) getTotalColumnName() string {
	if r.totalColumnName == "" {
		return "total"
	}

	return r.totalColumnName
}

func (r *SQLRepository) applySelectValue(req *ItemsRequest, query *string) {
	*query += "SELECT "

	if len(req.Groups) > 0 {
		dimGroup := make([]string, 0)
		for _, item := range req.Groups {
			field, exists := r.getDimension(DimensionKey(item))
			if !exists {
				continue
			}
			dimGroup = append(dimGroup, field.Expression)
		}
		*query += strings.Join(dimGroup, ",") + ", "
	}
	*query += fmt.Sprintf("count(*) AS %s", r.getTotalColumnName())
}

func (r *SQLRepository) applySelect(req *ItemsRequest, query *string) {
	*query += "SELECT "

	if len(req.Groups) > 0 {
		dimGroup := make([]string, 0)
		for _, item := range req.Groups {
			field, exists := r.getDimension(DimensionKey(item))
			if !exists {
				continue
			}
			dimGroup = append(dimGroup, field.Expression)
		}
		*query += strings.Join(dimGroup, ",") + ", "
	}

	metrics := make([]string, 0, len(r.metrics))
	for i := range r.metrics {
		m := r.metrics[i]
		metrics = append(metrics, m.Expression+` AS `+m.Name)
	}

	*query += strings.Join(metrics, ",")
}

func (r *SQLRepository) applyLimit(req *ItemsRequest, query *string) {
	if req.Limit > 0 && req.Offset > 0 {
		*query += fmt.Sprintf(" LIMIT %d, %d", req.Offset, req.Limit)
	} else if req.Limit > 0 {
		*query += fmt.Sprintf(" LIMIT %d", req.Limit)
	}
}

func unwrapPointerInterface(i interface{}) interface{} {
	if pv, ok := i.(*interface{}); ok {
		return *pv
	}

	return i
}

func safeNaN(i interface{}) interface{} {
	if iv, ok := i.(float64); ok && (math.IsNaN(iv) || math.IsInf(iv, 0)) {
		return 0
	}

	return i
}

func castUInt64(value interface{}) (uint64, bool) {
	switch t := value.(type) {
	case uint64:
		return t, true
	case uint32:
		return uint64(t), true
	case uint16:
		return uint64(t), true
	case uint8:
		return uint64(t), true
	case uint:
		return uint64(t), true
	}

	return 0, false
}

func castInt64(value interface{}) (int64, bool) {
	switch t := value.(type) {
	case int64:
		return t, true
	case int32:
		return int64(t), true
	case int16:
		return int64(t), true
	case int8:
		return int64(t), true
	case int:
		return int64(t), true
	}

	return 0, false
}
