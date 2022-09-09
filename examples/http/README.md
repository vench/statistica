# Example HTTP server

This example starts an HTTP server with a sqlite database.
Run example
```shell
go run examples/http/main.go 
```


Grouped query example

```shell
curl http://127.0.0.1:8080/grouped?query={%22limit%22:10,%22date_from%22:%222022-09-11%22}
```
