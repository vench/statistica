package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/vench/statistica"
	"go.uber.org/zap"
)

func main() {
	dataSourceName := flag.String("source", "tcp://127.0.0.1:19090?debug=true&database=&username=default", "")
	flag.Parse()

	conn, err := sql.Open("clickhouse", *dataSourceName)
	if err != nil {
		log.Fatalf("failed to open clickhouse connection: %v", err)
	}

	repository := initRepository(conn)
	err = repository.Ping()
	if err != nil {
		log.Fatalf("failed to ping connection: %v", err)
	}

	log.Println("ping success")
	printTotal(repository)
	log.Println()
	printMetrics(repository)
	log.Println()
	printValues(repository)
	log.Println()
	printGrouped(repository)
	log.Println()
}

func printValues(repository *statistica.SQLRepository) {
	request := &statistica.ItemsRequest{
		Limit:   10,
		Offset:  0,
		Groups:  []string{"ip"},
		Filters: make([]*statistica.ItemsRequestFilter, 0),
	}

	values, err := repository.Values(request)
	if err != nil {
		log.Fatalf("failed to get repository values: %v", err)
	}

	log.Println("print values")
	for i := range values {
		js, err := json.Marshal(values[i])
		if err != nil {
			log.Fatalf("failed to json marshal: %v", err)
		}

		log.Printf("json row:' %s \n", js)
	}
}

func printTotal(repository *statistica.SQLRepository) {
	request := &statistica.ItemsRequest{
		Limit:   10,
		Offset:  0,
		Filters: make([]*statistica.ItemsRequestFilter, 0),
	}

	total, err := repository.Total(request)
	if err != nil {
		log.Fatalf("failed to get repository values: %v", err)
	}

	log.Printf("print total: %d \n", total)
}

func printGrouped(repository *statistica.SQLRepository) {
	request := &statistica.ItemsRequest{
		Limit:   10,
		Offset:  0,
		Groups:  []string{"ip"},
		Metrics: []string{"cost", "cpm"},
		Filters: make([]*statistica.ItemsRequestFilter, 0),
	}

	grouped, err := repository.Grouped(request)
	if err != nil {
		log.Fatalf("failed to get repository grouped: %v", err)
	}

	log.Println("print grouped")
	for i := range grouped {
		js, err := json.Marshal(grouped[i])
		if err != nil {
			log.Fatalf("failed to json marshal: %v", err)
		}

		log.Printf("json row:' %s \n", js)
	}
}

func printMetrics(repository *statistica.SQLRepository) {
	metrics, err := repository.Metrics()
	if err != nil {
		log.Fatalf("failed to get repository metrics: %v", err)
	}

	log.Println("print metrics")
	for i := range metrics {
		log.Printf("name:' %s', expression: %s \n", metrics[i].Name, metrics[i].Expression)
	}
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
		statistica.LoggerSQLRepositoryOption(zap.NewExample()),
	)
}
