//go:build integration
// +build integration

package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"

	"github.com/vench/statistica"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	setupNameDB     = "test_db"
	setupUserDB     = "default"
	setupPasswordDB = ""

	setupHostDB string
	setupPortDB nat.Port
)

func setupClickHouse(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image: "clickhouse/clickhouse-server",
		Env: map[string]string{
			"CLICKHOUSE_DB":       setupNameDB,
			"CLICKHOUSE_USER":     setupUserDB,
			"CLICKHOUSE_PASSWORD": setupPasswordDB,
		},
		ExposedPorts: []string{
			"8123/tcp",
			"9000/tcp",
		},
		WaitingFor: wait.ForAll(
			wait.ForHTTP("/ping").WithPort("8123/tcp").WithStatusCodeMatcher(
				func(status int) bool {
					return status == http.StatusOK
				},
			),
		),
	}

	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generic container: %w", err)
	}

	setupHostDB, err = chContainer.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get host: %w", err)
	}

	setupPortDB, err = chContainer.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return nil, fmt.Errorf("failed to get port: %w", err)
	}

	return chContainer, nil
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	cont, err := setupClickHouse(ctx)
	if err != nil {
		log.Fatalf("failed to setup clickhouse: %v", err)

		return
	}

	if err = initClickHouseDB(ctx); err != nil {
		log.Fatalf("failed to init DB clickhouse: %v", err)

		return
	}

	exitVal := m.Run()

	cont.Terminate(ctx)

	os.Exit(exitVal)
}

func dataSourceNameDB() string {
	return fmt.Sprintf(
		"tcp://%s:%d?debug=true&database=%s&username=%s&password=%s",
		setupHostDB, setupPortDB.Int(), setupNameDB, setupUserDB, setupPasswordDB)
}

func TestClickhouse_SQLRepository(t *testing.T) {
	t.Parallel()

	s := dataSourceNameDB()
	conn, err := sql.Open("clickhouse", s)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, conn.Close())
	}()

	repo := initRepository(conn)
	require.NoError(t, repo.Ping())

	m, err := repo.Metrics()
	require.NoError(t, err)
	require.Equal(t, []*statistica.Metric{
		{
			Name:       "total",
			Expression: "count(*)",
		},
		{
			Name:       "cost",
			Expression: "sum(price)",
		},
		{
			Name:       "cpm",
			Expression: "sum(price)/count(*)",
		},
	}, m)

	// check total
	total, err := repo.Total(&statistica.ItemsRequest{})
	require.NoError(t, err)
	require.Equal(t, uint64(6), total)

	// check values
	values, err := repo.Values(&statistica.ItemsRequest{
		Groups: []string{"ip"},
	})
	require.NoError(t, err)

	require.Equal(t, []*statistica.ValueResponse{
		{
			Name: []interface{}{"ip"}, Key: []interface{}{"192.168.1.1"}, Count: statistica.ValueNumber(2),
		},
		{
			Name: []interface{}{"ip"}, Key: []interface{}{"127.0.0.1"}, Count: statistica.ValueNumber(4),
		},
	}, values)

	// group
	grouped, err := repo.Grouped(&statistica.ItemsRequest{
		Groups:  []string{"ip"},
		Metrics: []string{"cost", "cpm"},
		SortBy: []*statistica.ItemsRequestOrder{
			{
				Key: "cost", Direction: "desc",
			},
		},
	})
	require.NoError(t, err)

	require.Equal(t, []*statistica.ItemRow{
		{
			Dimensions: map[string]interface{}{"ip": "192.168.1.1"},
			Metrics:    map[string]statistica.ValueNumber{"cost": 3000, "cpm": 1500, "total": 2},
		},
		{
			Dimensions: map[string]interface{}{"ip": "127.0.0.1"},
			Metrics:    map[string]statistica.ValueNumber{"cost": 6600, "cpm": 1650, "total": 4},
		},
	}, grouped)
}

func initRepository(db *sql.DB) *statistica.SQLRepository {
	return statistica.NewSQLRepository(db, "events",
		[]*statistica.Dimension{
			{
				Name:       "ip",
				Expression: "ip",
			},
			{
				Name:       "event_type",
				Expression: "etype",
			},
			{
				Name:       "created",
				Expression: "created",
			},
		},
		[]*statistica.Metric{
			{
				Name:       "total",
				Expression: "count(*)",
			},
			{
				Name:       "cost",
				Expression: "sum(price)",
			},
			{
				Name:       "cpm",
				Expression: "sum(price)/count(*)",
			},
		},
	)
}

func initClickHouseDB(ctx context.Context) error {
	_ = ctx
	s := dataSourceNameDB()
	db, err := sql.Open("clickhouse", s)
	if err != nil {
		return fmt.Errorf("failed to open DB: %w", err)
	}

	if _, err = db.Exec(`DROP TABLE IF EXISTS events`); err != nil {
		return fmt.Errorf("failed to drop table `events`: %w", err)
	}

	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS events (
        eid UInt32,
        ip String,
        etype UInt32,
        price UInt32 DEFAULT 0,
        created Date
    )
    ENGINE = MergeTree()
    ORDER BY (created)`); err != nil {
		return fmt.Errorf("failed to create table `events`: %w", err)
	}

	events := []struct {
		IP      string
		etype   int
		price   int
		created time.Time
	}{
		{
			IP:      "192.168.1.1",
			etype:   100,
			price:   1000,
			created: time.Date(2022, 10, 1, 12, 0, 0, 0, time.Local),
		},
		{
			IP:      "192.168.1.1",
			etype:   101,
			price:   2000,
			created: time.Date(2022, 10, 1, 12, 0, 0, 0, time.Local),
		},
		{
			IP:      "127.0.0.1",
			etype:   101,
			price:   2000,
			created: time.Date(2022, 10, 1, 12, 0, 0, 0, time.Local),
		},
		{
			IP:      "127.0.0.1",
			etype:   101,
			price:   2000,
			created: time.Date(2022, 10, 2, 12, 0, 0, 0, time.Local),
		},
		{
			IP:      "127.0.0.1",
			etype:   101,
			price:   1500,
			created: time.Date(2022, 10, 3, 12, 0, 0, 0, time.Local),
		},
		{
			IP:      "127.0.0.1",
			etype:   102,
			price:   1100,
			created: time.Date(2022, 10, 3, 12, 0, 0, 0, time.Local),
		},
	}

	scope, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := scope.Prepare("INSERT INTO events(ip, etype, price, created) values(?,?,?,?)")
	if err != nil {
		return fmt.Errorf("failed to prepare insert into `events`: %w", err)
	}
	defer stmt.Close()

	for i := range events {
		e := events[i]
		if _, err = stmt.Exec(e.IP, e.etype, e.price, e.created); err != nil {
			return fmt.Errorf("failed to execute query insert `events`: %w", err)
		}
	}

	if err = scope.Commit(); err != nil {
		return fmt.Errorf("failed to commit scope `events`: %w", err)

	}

	return nil
}
