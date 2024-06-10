# pg-gateway

A gateway for proxying to remote PostgreSQL DB. Useful when you want to connect a PG client (e.g. PSQL) to a remote backend that use custom credential management during development. (e.g. dynamic password)

This is NOT meant to be used in PRODUCTION.

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
	go gw.Serve(ctx, "localhost:8888")
	defer gw.Close()

    // connect to gateway via any pg driver
	conn, err := pgx.Connect(context.TODO(), "postgres://localhost:8888/postgres?user=foobar&sslmode=disable")
```


## TODO

[x] support SASL SCRAM-SHA-256


[] remove dependency on `lib/pg`

