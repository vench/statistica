package statistica

import (
	"database/sql"
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

const (
	testTable = "test_table"
)

var testQuery = &ItemsRequest{
	Groups: []string{
		"user_id",
		"geo_id",
	},
	Filters: []*ItemsRequestFilter{
		{
			Key:    "geo_id",
			Values: []interface{}{1, 2, 4},
		},
	},
	SortBy: []*ItemsRequestOrder{
		{
			Key:       "user_id",
			Direction: "desc",
		},
	},
	Limit:  100,
	Offset: 1000,
}

func testRepository(t *testing.T, db *sql.DB) *SQLRepository {
	t.Helper()

	return NewSQLRepository(db, testTable,
		[]*Dimension{
			{
				Name:       "user_id",
				Expression: "user_id",
			},
			{
				Name:       "geo_id",
				Expression: "geo_id",
			},
		},
		[]*Metric{
			{
				Name:       "total",
				Expression: "count(*)",
			},
		},
	)
}

func TestRepository_Ping(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectExec(
		"^" + regexp.QuoteMeta("SELECT 1") + "$",
	).WillReturnResult(sqlmock.NewResult(0, 0))

	r := testRepository(t, db)
	err = r.Ping()
	require.NoError(t, err)
}

func TestRepository_Values(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	result := []*ValueResponse{
		{
			Name: []interface{}{
				"user_id", "geo_id",
			},
			Key: []interface{}{
				int64(100), int64(10),
			},
			Count: int64(100),
		},
		{
			Name: []interface{}{
				"user_id", "geo_id",
			},
			Key: []interface{}{
				int64(100), int64(20),
			},
			Count: int64(200),
		},
		{
			Name: []interface{}{
				"user_id", "geo_id",
			},
			Key: []interface{}{
				int64(200), int64(10),
			},
			Count: int64(170),
		},
	}

	columns := []string{"user_id", "geo_id", "total"}
	rows := sqlmock.NewRows(columns)
	for i := range result {
		r := result[i]
		values := make([]driver.Value, len(columns))
		j := 0
		for ; j < len(r.Key); j++ {
			values[j] = r.Key[j]
		}
		values[j] = r.Count
		rows.AddRow(values...)
	}

	mock.
		ExpectQuery(
			"^"+regexp.QuoteMeta(
				"SELECT user_id,geo_id, count(*) AS total "+
					"FROM test_table WHERE geo_id IN (?,?,?) GROUP BY user_id,geo_id  "+
					"ORDER BY user_id desc LIMIT 1000, 100")+"$",
		).
		WithArgs(1, 2, 4).
		WillReturnRows(rows)

	r := testRepository(t, db)
	values, err := r.Values(testQuery)
	require.NoError(t, err)
	require.Equal(t, result, values)
}

func TestRepository_Total(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	result := []string{
		"100",
	}

	mock.
		ExpectQuery(
			"^"+regexp.QuoteMeta(
				"SELECT uniq(user_id,geo_id) AS total FROM test_table WHERE geo_id IN (?,?,?)")+"$",
		).
		WithArgs(1, 2, 4).
		WillReturnRows(sqlmock.NewRows(result))

	r := testRepository(t, db)
	total, err := r.Total(testQuery)
	require.NoError(t, err)
	require.Equal(t, total, total)
}

func TestRepository_Metrics(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	r := testRepository(t, db)
	list, err := r.Metrics()
	require.NoError(t, err)

	metrics := []*Metric{
		{
			Name:       "total",
			Expression: "count(*)",
		},
	}
	require.Equal(t, metrics, list)
}

func TestRepository_Grouped(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	result := []*ItemRow{
		{
			Dimensions: map[string]interface{}{
				"user_id": int64(1000),
				"geo_id":  int64(512),
			},
			Metrics: map[string]interface{}{
				"total": float64(2000),
			},
		},
		{
			Dimensions: map[string]interface{}{
				"user_id": int64(1001),
				"geo_id":  int64(512),
			},
			Metrics: map[string]interface{}{
				"total": float64(4000),
			},
		},
	}

	dimensions := []string{"user_id", "geo_id"}
	metrics := []string{"total"}

	rows := sqlmock.NewRows(append(dimensions, metrics...))
	for i := range result {
		r := result[i]
		values := make([]driver.Value, len(dimensions)+len(metrics))
		for j, name := range dimensions {
			values[j] = r.Dimensions[name]
		}

		offset := len(dimensions)
		for j, name := range metrics {
			values[j+offset] = r.Metrics[name]
		}

		rows.AddRow(values...)
	}

	mock.
		ExpectQuery(
			"^"+regexp.QuoteMeta(
				"SELECT user_id,geo_id, count(*) AS total "+
					"FROM test_table WHERE geo_id IN (?,?,?) GROUP BY user_id,geo_id  "+
					"ORDER BY user_id desc LIMIT 1000, 100")+"$",
		).
		WithArgs(1, 2, 4).
		WillReturnRows(rows)

	r := testRepository(t, db)

	list, err := r.Grouped(testQuery)
	require.NoError(t, err)
	require.Equal(t, result, list)
}
