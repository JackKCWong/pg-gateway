package pggw

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/lib/pq/scram"
)

type Session interface {
	Run(ctx context.Context)
}

type GatedSession struct {
	id      string
	resolve ConnectionResolver

	clientConn net.Conn
	left       *pgproto3.Backend

	serverConn net.Conn
	right      *pgproto3.Frontend
	saslFinal  []byte

	closing sync.Once

	logger *slog.Logger
}

func (s *GatedSession) Run(ctx context.Context) {
	su, err := s.waitForClientStartup(ctx)
	if err != nil {
		s.logger.Error("failed to startup session", "err", err)
		return
	}

	remote, err := s.resolve(ctx, su.Parameters)
	if err != nil {
		s.logger.Error("failed to authenticate session", "err", err)
		return
	}

	s.logger.Debug("resolved", "host", remote.Host, "port", remote.Port, "user", remote.User, "database", remote.Database, "params", remote.Params)

	err = s.connectToBackend(remote)
	if err != nil {
		s.logger.Error("failed to establish remote session", "err", err)
		return
	}

	err = s.authHandshake(remote.User, remote.Password)
	if err != nil {
		s.logger.Error("failed to authenticate session", "err", err)
		return
	}

	err = s.copySteadyState(ctx)
	if err != nil {
		s.logger.Error("failed to copy session", "err", err)
		return
	}
}

func (s *GatedSession) waitForClientStartup(ctx context.Context) (*pgproto3.StartupMessage, error) {
	s.logger.Debug("waiting for client startup")
	for {
		select {
		    case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled")
			
		    default:
				break
		}

		startUpMsg, err := s.left.ReceiveStartupMessage()
		if err != nil {
			s.logger.Error("failed to receive startup message", "err", err)
			return nil, err
		}

		switch startUpMsg := startUpMsg.(type) {
		case *pgproto3.StartupMessage:
			s.logger.Debug("client startup received")
			return startUpMsg, nil

		case *pgproto3.SSLRequest:
			_, err = s.clientConn.Write([]byte("N"))
			if err != nil {
				return nil, fmt.Errorf("error sending deny SSL request: %w", err)
			}

			continue
		default:
			return nil, fmt.Errorf("unsupported startup message: %#v", startUpMsg)
		}
	}
}

func (s *GatedSession) connectToBackend(remote RemotePgBackend) error {
	s.logger.Debug("connecting to remote", "host", remote.Host, "port", remote.Port, "user", remote.User, "database", remote.Database, "params", remote.Params)
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", remote.Host, remote.Port))
	if err != nil {
		return fmt.Errorf("error connecting to remote: %w", err)
	}

	s.serverConn = conn
	s.right = pgproto3.NewFrontend(conn, conn)
	params := map[string]string{
		"user":     remote.User,
		"database": remote.Database,
	}

	if remote.Params != nil {
		for k, v := range remote.Params {
			params[k] = v
		}
	}

	su := pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      params,
	}

	s.right.Send(&su)

	err = s.right.Flush()
	if err != nil {
		return fmt.Errorf("error sending startup message: %w", err)
	}

	s.logger.Debug("startup message sent")
	return nil
}

func (s *GatedSession) authHandshake(user, pass string) error {
	s.logger.Debug("waiting for auth method negotiate")
	msg, err := s.right.Receive()
	if err != nil {
		return fmt.Errorf("error receiving auth message: %w", err)
	}

	switch msg := msg.(type) {
	case *pgproto3.AuthenticationSASL:
		s.logger.Debug("SASL received")
		if !slices.Contains(msg.AuthMechanisms, "SCRAM-SHA-256") {
			return fmt.Errorf("unsupported SASL mechanisms: %#v", msg)
		}

		sc := scram.NewClient(sha256.New, user, pass)
		sc.Step(nil)

		s.right.Send(&pgproto3.SASLInitialResponse{
			AuthMechanism: "SCRAM-SHA-256",
			Data:          sc.Out(),
		})
		if err := s.right.Flush(); err != nil {
			return fmt.Errorf("error sending SASL initial response: %w", err)
		}

		next, err := s.right.Receive()
		if err != nil {
			return fmt.Errorf("error receiving SASL continue message: %w", err)
		}

		contMsg, ok := next.(*pgproto3.AuthenticationSASLContinue)
		if !ok {
			return fmt.Errorf("expected SASL continue message, got %#v", contMsg)
		}

		sc.Step(contMsg.Data)
		s.right.Send(&pgproto3.SASLResponse{
			Data: sc.Out(),
		})
		if err := s.right.Flush(); err != nil {
			return fmt.Errorf("error sending SASL response: %w", err)
		}

		next, err = s.right.Receive()
		if err != nil {
			return fmt.Errorf("error receiving SASL final message: %w", err)
		}

		finalMsg, ok := next.(*pgproto3.AuthenticationSASLFinal)
		if !ok {
			return fmt.Errorf("expected SASL final message, got %#v", finalMsg)
		}

		s.saslFinal = finalMsg.Data

		return nil

	default:
		return fmt.Errorf("unsupported auth method: %#v", msg)
	}
}

func (s *GatedSession) copySteadyState(ctx context.Context) error {
	s.logger.Debug("steady state")
	go func() {
		for {
			select {
			case <-ctx.Done():
				s.close()
				return
			default:
				msg, err := s.right.Receive()
				if err != nil {
					s.logger.Error("error receiving message from remote", "err", err)
					return
				}
				s.left.Send(msg)
				if err := s.left.Flush(); err != nil {
					s.logger.Error("error sending message to client", "err", err, "msg", msg)
					return
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				s.close()
				return
			default:
				msg, err := s.left.Receive()
				if err != nil {
					s.logger.Error("error receiving message from client", "err", err)
					return
				}
				s.right.Send(msg)
				if err := s.right.Flush(); err != nil {
					s.logger.Error("error sending message to remote", "err", err, "msg", msg)
					return
				}
			}
		}
	}()

	return nil
}

func (s *GatedSession) close() {
	s.closing.Do(func() {
		s.logger.Debug("closing session")
		s.left.Flush()
		s.clientConn.Close()
		s.right.Flush()
		s.serverConn.Close()
	})
}
