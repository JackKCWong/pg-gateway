package pggw

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/sqids/sqids-go"
)

type RemotePgBackend struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Params   map[string]string
}

// ConnectionResolver resolves a local postgres connection string to a remote backend.
type ConnectionResolver func(ctx context.Context, params map[string]string) (RemotePgBackend, error)

type Gateway struct {
	resolve  ConnectionResolver
	mux      sync.Mutex
	incoming net.Listener
	sessions []Session
	idgen    *sqids.Sqids
}

type GatewayOption func(gw *Gateway)

func NewGateway(resolver ConnectionResolver, opts ...GatewayOption) *Gateway {
	s, _ := sqids.New(sqids.Options{
		MinLength: 5,
	})

	return &Gateway{
		mux:     sync.Mutex{},
		idgen:   s,
		resolve: resolver,
	}
}

func (gw *Gateway) Serve(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("failed to start listener", "err", err)
		return err
	}

	slog.Debug("listening", "address", ln.Addr())

	gw.incoming = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("failed to accept connection", "err", err)
			return err
		}

		go gw.startSession(ctx, conn)
	}
}

func (gw *Gateway) startSession(ctx context.Context, conn net.Conn) {
	gw.mux.Lock()

	seq := len(gw.sessions)
	sid, _ := gw.idgen.Encode([]uint64{uint64(seq)})
	slog.Debug("new session", "sid", sid)
	ses := GatedSession{
		id:         sid,
		resolve:    gw.resolve,
		clientConn: conn,
		left:       pgproto3.NewBackend(conn, conn),
		logger:     slog.Default().With("sid", sid),
	}

	gw.sessions = append(gw.sessions, &ses)
	gw.mux.Unlock()

	ses.Run(ctx)
}

func (gw *Gateway) Close() error {
    gw.incoming.Close()
	for i := range gw.sessions {
		gw.sessions[i].Close()
	}

	return nil
}