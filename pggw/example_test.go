package pggw_test

import (
	"context"
	"testing"
	"time"

	"github.com/JackKCWong/pg-gateway/pggw"
	"github.com/jackc/pgx/v5"
)

func TestGateway(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	gw := pggw.NewGateway(func(ctx context.Context, params map[string]string) (pggw.RemotePgBackend, error) {
		return pggw.RemotePgBackend{
			Host:     "localhost",
			Port:     5432,
			User:     params["user"],
			Password: "fasfsa8fydsofiasoflfas",
			Database: params["database"],
		}, nil
	})

	go gw.Serve(context.Background(), "localhost:8888")
	defer gw.Close()

	conn, err := pgx.Connect(ctx, "postgres://localhost:8888/postgres?user=pg&sslmode=disable")
	if err != nil {
		t.Fatalf("Unable to connect to database: %v", err)
	}

	rs, err := conn.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("Unable to query database: %v", err)
	}

	if !rs.Next() {
		t.Fatalf("no query result: %v", err)
	}

	if v, err := rs.Values(); err == nil {
		t.Logf("Result: %v", v)
	} else {
		t.Fatalf("Unable to get query result: %v", err)
	}

}

func TestDirectConnect(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, "postgres://pg:fasfsa8fydsofiasoflfas@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("Unable to connect to database: %v", err)
	}

	rows, err := conn.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("Unable to query database: %v", err)
	}

	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("Unable to retrieve result from database: %v", err)
	}

	r, err := rows.Values()
	if err != nil {
		t.Fatalf("Unable to retrieve result from database: %v", err)
	}

	t.Logf("Result: %v", r)
}
