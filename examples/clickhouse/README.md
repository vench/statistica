# Example integration with ClickHouse

Run docker image ClickHouse database
```shell
$ mkdir $HOME/some_clickhouse_database
$ docker run -d --name some-clickhouse-server --ulimit nofile=262144:262144 --volume=$HOME/some_clickhouse_database:/var/lib/clickhouse yandex/clickhouse-server 
```

Then create database and table
```sql
CREATE DATABASE develop;

CREATE TABLE IF NOT EXISTS events (
	eid UInt32,
	ip String,
	etype UInt32 default 0,
	price UInt64 default 0,
	created DATETIME default now()
)
ENGINE = MergeTree()
ORDER BY created
;

INSERT INTO events (eid, ip, etype, price, created)
VALUES
	(1, '192.168.1.1', 100, 1000000, '2022-10-01 12:00:00'),
	(2, '127.0.0.1', 100, 1000000, '2022-10-02 12:00:00'),
	(3, '192.168.1.1', 100, 1000000, '2022-10-03 12:00:00'),
	(4, '127.0.0.1', 200, 1000000, '2022-10-01 12:00:00'),
	(5, '192.168.1.1', 200, 1000000, '2022-10-02 12:00:00'),
	(6, '127.0.0.1', 200, 1000000, '2022-10-03 12:00:00'),
	(7, '192.168.1.1', 300, 1000000, '2022-10-01 12:00:00'),
	(8, '127.0.0.1', 300, 1000000, '2022-10-02 12:00:00'),
	(9, '192.168.1.1', 300, 1000000, '2022-10-04 12:00:00')
;
```

Run example
```shell
go run examples/clickhouse/main.go 
```
