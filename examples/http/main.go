package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"github.com/vench/statistica"
)

func main() {
	addr := flag.String("addr", ":8080", "")
	flag.Parse()

	// todo pass connection
	repository := initRepository(nil)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("hello world"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		list, err := repository.Metrics()
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		data, err := json.Marshal(list)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		w.Write(data)
	})

	log.Printf("start http server: %s\n", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("failed to up http server: %v", err)
	}
}

func initRepository(db *sql.DB) *statistica.SQLRepository {
	return statistica.NewSQLRepository(db, "users",
		[]*statistica.Dimension{
			{
				Name:       "user_id",
				Expression: "user_id",
			},
			{
				Name:       "geo_id",
				Expression: "geo_id",
			},
		},
		[]*statistica.Metric{
			{
				Name:       "total",
				Expression: "count(*)",
			},
		},
	)
}
