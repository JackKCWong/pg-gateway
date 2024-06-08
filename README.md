# pg-gateway

A gateway for proxying to remote PostgreSQL DB. Useful when you want to connect a PG client (e.g. PSQL) to a remote backend that use custom credential management. (e.g. dynamic password)

## features

* support custom credential management

## usage

```go
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

    // use custom credential management
	gw := pggw.NewGateway(func(ctx context.Context, params map[string]string) (pggw.RemotePgBackend, error) {
		return pggw.RemotePgBackend{
			Host:     "localhost",
			Port:     5432,
			User:     params["user"],
			Database: params["database"],
			Password: "foobar",
		}, nil
	})

    // start gateway
	go gw.Serve(context.Background(), "localhost:8888")
	defer gw.Close()

    // connect to gateway via any pg driver
	conn, err := pgx.Connect(ctx, "postgres://localhost:8888/postgres?sslmode=disable")
```


## TODO

[x] support SASL SCRAM-SHA-256

[] support SSL

