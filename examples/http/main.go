package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vench/statistica"
)

const dateFormat = "2006-01-02"

var formatsDateType = []string{dateFormat}

type dateType struct {
	time.Time
}

func (d *dateType) UnmarshalJSON(v []byte) error {
	s := strings.Trim(string(v), "\"")

	for i := range formatsDateType {
		if t, err := time.Parse(formatsDateType[i], s); err == nil {
			d.Time = t

			break
		}
	}

	return nil
}

type requestQuery struct {
	DateFrom dateType `json:"date_from"`
	DateTo   dateType `json:"date_to"`

	Limit   int            `json:"limit"`
	Offset  int            `json:"offset"`
	SortBy  []interface{}  `json:"sort_by"`
	Groups  []string       `json:"groups"`
	Metrics []string       `json:"metrics"`
	Filters []*interface{} `json:"filters"`
}

func requestFromQuery(r *http.Request) (*statistica.ItemsRequest, error) {
	rQuery := &requestQuery{}
	query := r.URL.Query().Get("query")
	if query == "" {
		query = "{}"
	}

	if err := json.Unmarshal([]byte(query), rQuery); err != nil {
		return nil, err
	}

	request := &statistica.ItemsRequest{
		Limit:   rQuery.Limit,
		Offset:  rQuery.Offset,
		Groups:  rQuery.Groups,
		Metrics: rQuery.Metrics,
		Filters: make([]*statistica.ItemsRequestFilter, 0),
	}

	if !rQuery.DateFrom.IsZero() {
		request.Filters = append(request.Filters, &statistica.ItemsRequestFilter{
			Key:       "created",
			Condition: ">=",
			Values:    []interface{}{rQuery.DateFrom.Format(dateFormat)},
		})
	}
	if !rQuery.DateTo.IsZero() {
		request.Filters = append(request.Filters, &statistica.ItemsRequestFilter{
			Key:       "created",
			Condition: "<=",
			Values:    []interface{}{rQuery.DateTo.Format(dateFormat)},
		})
	}

	return request, nil
}

func main() {
	addr := flag.String("addr", ":8080", "")
	sqlitePath := flag.String("sqlite_path", "./foo.db", "")
	flag.Parse()

	db, err := sql.Open("sqlite3", *sqlitePath)
	if err != nil {
		log.Fatalf("failed to open sqlite3 connection: %v", err)
	}
	defer db.Close()

	if err = initDB(db); err != nil {
		log.Fatalf("failed to init DB: %v", err)
	}

	repository := initRepository(db)

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

	mux.HandleFunc("/total", func(w http.ResponseWriter, r *http.Request) {
		request, err := requestFromQuery(r)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		total, err := repository.Total(request)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		if _, err := fmt.Fprintf(w, "{\"total\":%d}", total); err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}
	})

	mux.HandleFunc("/grouped", func(w http.ResponseWriter, r *http.Request) {
		request, err := requestFromQuery(r)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		rows, err := repository.Grouped(request)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		body, err := json.Marshal(rows)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		w.Write(body)
	})

	mux.HandleFunc("/values", func(w http.ResponseWriter, r *http.Request) {
		request, err := requestFromQuery(r)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		rows, err := repository.Values(request)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		body, err := json.Marshal(rows)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		w.Write(body)
	})

	log.Printf("start http server: %s\n", *addr)
	if err = http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("failed to up http server: %v", err)
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

func initDB(db *sql.DB) error {
	var err error
	if _, err = db.Exec(`DROP TABLE IF EXISTS events`); err != nil {
		return fmt.Errorf("failed to drop table `events`: %w", err)
	}

	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS events (
        eid INTEGER PRIMARY KEY AUTOINCREMENT,
        ip VARCHAR(16) NULL,
        etype INTEGER NULL,
        price INTEGER NOT NULL DEFAULT 0,
        created DATE NULL
    )`); err != nil {
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

	stmt, err := db.Prepare("INSERT INTO events(ip, etype, price, created) values(?,?,?,?)")
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

	return nil
}
