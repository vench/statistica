package statistica

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"
)

const (
	CondEq    = "eq"
	CondNotEq = "neq"
	CondLike  = "like"

	dateFormat = "2006-01-02"
)

// ReadRepository common read interface.
type ReadRepository interface {
	// Total returns total rows by query conditions.
	Total(req *ItemsRequest) (uint64, error)
	// Values returns list of allowed values with size by query conditions.
	Values(req *ItemsRequest) ([]*ValueResponse, error)
	// Grouped returns rows metrics by group filtered by query conditions.
	Grouped(req *ItemsRequest) ([]*ItemResponse, error)
	// Metrics returns list of allowed metrics.
	Metrics() ([]*Metric, error)
}

// SQLRepository sql implementation of ReadRepository.
type SQLRepository struct {
	conn *sql.DB

	mapDimensions map[DimensionKey]*Dimension
	metrics       []*Metric

	// contains table name or sql expression like table.
	table           string
	totalColumnName string
}

// NewSQLRepository returns new instance of SQLRepository.
func NewSQLRepository(
	connection *sql.DB, table string, dimensions []*Dimension, metrics []*Metric,
) *SQLRepository {
	mDimensions := make(map[DimensionKey]*Dimension, len(dimensions))
	for i := range dimensions {
		mDimensions[dimensions[i].Name] = dimensions[i]
	}

	return &SQLRepository{
		conn:          connection,
		table:         table,
		mapDimensions: mDimensions,
		metrics:       metrics,
	}
}

func (r *SQLRepository) Metrics() ([]*Metric, error) {
	return r.metrics, nil
}

func (r *SQLRepository) Total(req *ItemsRequest) (uint64, error) {
	query := ""
	params := make([]interface{}, 0)

	r.applySelectTotal(req, &query)
	query += fmt.Sprintf(` FROM %s`, r.table)
	r.applyWhere(req, &query, &params)

	rows, err := r.conn.Query(query, params...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}

	if rows.Next() {
		dest := make([]interface{}, 0)
		for _, item := range types {
			v := reflect.New(item.ScanType()).Interface()
			dest = append(dest, v)
		}

		if err := rows.Scan(dest...); err != nil {
			return 0, err
		}

		for i, item := range types {
			if item.Name() != r.getTotalColumnName() {
				continue
			}
			if vi, ok := dest[i].(*uint64); ok {
				return *vi, nil
			}
		}
	}

	return 0, nil
}

func (r *SQLRepository) Values(req *ItemsRequest) ([]*ValueResponse, error) {
	query := ``
	params := make([]interface{}, 0)

	r.applySelectValue(req, &query)
	query += fmt.Sprintf(` FROM %s `, r.table)
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
		dest := make([]interface{}, 0)
		for _, item := range types {
			v := reflect.New(item.ScanType()).Interface()
			dest = append(dest, v)
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		itemResp := &ValueResponse{
			Key:   make([]interface{}, 0),
			Name:  make([]interface{}, 0),
			Count: 0,
		}
		for i, item := range types {
			_ = item

			pv, ok := dest[i].(*interface{})
			if ok {
				dest[i] = *pv
			}

			if len(req.Groups) > i {
				itemResp.Name = append(itemResp.Name, req.Groups[i])
				itemResp.Key = append(itemResp.Key, dest[i])
			} else {
				itemResp.Count = SafeNaN(dest[i])
			}
		}
		response = append(response, itemResp)
	}

	return response, nil
}

func (r *SQLRepository) Grouped(req *ItemsRequest) ([]*ItemResponse, error) {
	query := ""
	params := make([]interface{}, 0)

	r.applySelect(req, &query)
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

	response := make([]*ItemResponse, 0)
	for rows.Next() {
		dest := make([]interface{}, 0)
		for _, item := range types {
			v := reflect.New(item.ScanType()).Interface()
			dest = append(dest, v)
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		itemResp := &ItemResponse{
			Dimensions: make(map[string]interface{}),
			Metrics:    make(map[string]interface{}),
		}

		for i, item := range types {
			pv, ok := dest[i].(*interface{})
			if ok {
				dest[i] = *pv
			}

			if len(req.Groups) > i {
				itemResp.Dimensions[req.Groups[i]] = dest[i]
			} else {
				itemResp.Metrics[item.Name()] = SafeNaN(dest[i])
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

			sort := fmt.Sprintf(`%s %s`, field.Expression, item.Direction)
			sortBy = append(sortBy, sort)
		}
	}

	if len(sortBy) > 0 {
		*query += ` ORDER BY ` + strings.Join(sortBy, `,`)
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

	// TODO remove
	if !req.DateFrom.IsZero() && !req.DateTo.IsZero() {
		where += "AND EventDate BETWEEN ? AND ? "
		*params = append(*params, req.DateFrom.Format(dateFormat), req.DateTo.Format(dateFormat))
	}

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
					where += ` AND `
				}

				switch filter.Condition {
				case CondEq:
					in := strings.TrimRight(strings.Repeat("?,", len(filter.Values)), ",")
					where += fmt.Sprintf(`%s IN (%s)`, key, in)

				case CondNotEq:
					in := strings.Repeat(`?,`, len(filter.Values))
					where += fmt.Sprintf(`%s NOT IN (%s)`, key, in)

				case CondLike:
					where += fmt.Sprintf(`%s LIKE '%s)`, key, `%?%'`)

				default:
					in := strings.TrimRight(strings.Repeat("?,", len(filter.Values)), ",")
					where += fmt.Sprintf(`%s IN (%s)`, key, in)
				}
			}
		}
	}

	if len(where) > 0 {
		*query += fmt.Sprintf(` WHERE %s`, where)
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

		*query += fmt.Sprintf(`SELECT uniq(%s) AS %s`, strings.Join(dimGroup, `,`), r.getTotalColumnName())
		return
	}

	*query += fmt.Sprintf(`SELECT count(*) AS %s`, r.getTotalColumnName())
}

func (r *SQLRepository) getTotalColumnName() string {
	if r.totalColumnName == "" {
		return "total"
	}

	return r.totalColumnName
}

func (r *SQLRepository) applySelectValue(req *ItemsRequest, query *string) {
	*query += `SELECT `

	if len(req.Groups) > 0 {
		dimGroup := make([]string, 0)
		for _, item := range req.Groups {
			field, exists := r.getDimension(DimensionKey(item))
			if !exists {
				continue
			}
			dimGroup = append(dimGroup, field.Expression)
		}
		*query += strings.Join(dimGroup, `,`) + `, `
	}
	*query += fmt.Sprintf(`count(*) AS %s`, r.getTotalColumnName())
}

func (r *SQLRepository) applySelect(req *ItemsRequest, query *string) {
	*query += `SELECT `

	if len(req.Groups) > 0 {
		dimGroup := make([]string, 0)
		for _, item := range req.Groups {
			field, exists := r.getDimension(DimensionKey(item))
			if !exists {
				continue
			}
			dimGroup = append(dimGroup, field.Expression)
		}
		*query += strings.Join(dimGroup, `,`) + `, `
	}

	metrics := make([]string, 0, len(r.metrics))
	for i := range r.metrics {
		m := r.metrics[i]
		metrics = append(metrics, m.Expression+` AS `+m.Name)
	}

	*query += strings.Join(metrics, `,`)
}

func (r *SQLRepository) applyLimit(req *ItemsRequest, query *string) {
	if req.Limit > 0 && req.Offset > 0 {
		*query += fmt.Sprintf(` LIMIT %d,%d`, req.Offset, req.Limit)
	} else if req.Limit > 0 {
		*query += fmt.Sprintf(` LIMIT %d`, req.Limit)
	}
}

func SafeNaN(i interface{}) interface{} {
	if iv, ok := i.(*float64); ok && (math.IsNaN(*iv) || math.IsInf(*iv, 0)) {
		*iv = 0
		return iv
	}

	return i
}
