package conn

import (
	"errors"
	"log/slog"
	"net"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

type UpstreamConnEntry struct {
	*C
	F           *pgproto3.Frontend
	isClosing   bool
	isSwitching bool
	logger      *slog.Logger
}


type UpstreamMessageHandler func(pgproto3.BackendMessage) error

func NewUpstream(params *ConnectionParams) (*UpstreamConnEntry, error) {
	conn := connect()
	conn.Params = params

	entry := &UpstreamConnEntry{
		C:      conn,
		F:      pgproto3.NewFrontend(conn, conn),
		logger: slog.With("conn", "upstream", "addr", conn.LocalAddr().String()),
	}

	if err := entry.Startup(); err != nil {
		return nil, err
	}

	return entry, nil
}

func connect() *Conn {
	pgHost := os.Getenv("PGHOST")
	pgPort := os.Getenv("PGPORT")
	if pgHost == "" || pgPort == "" {
		pgHost = "localhost"
		pgPort = "5432"
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", pgHost, pgPort))
	return NewConn(conn)
}

func (u *UpstreamConnEntry) Close() error {
	u.isClosing = true
	return u.Conn.Close()
}

func (u *UpstreamConnEntry) CloseForSwitch() error {
	u.isSwitching = true
	return u.Close()
}

func (u *UpstreamConnEntry) Listen(handler UpstreamMessageHandler) {
	u.logger.Debug("upstream listening")
	for {
		msg, err := u.F.Receive()
		if err != nil {
			if u.isClosing && strings.Contains(err.Error(), "closed network connection") {
				if u.isSwitching {
					u.Term <- ErrExpectedSwitch
				} else {
					u.Term <- ErrExpectedClose
				}
				return
			}
			u.Term <- err
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.ErrorResponse:
			if msg.Severity == "FATAL" && slices.Contains([]string{"57P01", "57P02", "57P03"}, msg.Code) {
				u.logger.Debug("upstream is terminating, dropping message")
				continue
			}
		}

		if err := handler(msg); err != nil {
			u.logger.Error("upstream message handler error", "err", err.Error())
			u.Term <- err
			return
		}
	}
}

func (u *UpstreamConnEntry) Send(msg pgproto3.FrontendMessage) error {
	u.F.Send(msg)
	return u.F.Flush()
}

func (u *UpstreamConnEntry) Startup() error {
	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      u.Params.PGParameters,
	}
	err := u.Send(startupMsg)
	if err != nil {
		u.logger.Error("failed to send startup message", "err", err.Error())
		return err
	}

	resp, err := u.F.Receive()
	if err != nil {
		u.logger.Error("failed to receive startup response", "err", err.Error())
		return err
	}
	u.logger.Debug("received startup response", "payload", resp)

	switch resp := resp.(type) {
	case *pgproto3.AuthenticationOk:
		// Do nothing
		break
	default:
		// TODO: Handle other auth methods
		pwdMsg := &pgproto3.PasswordMessage{
			Password: u.Params.Password,
		}
		err = u.Send(pwdMsg)
		if err != nil {
			u.logger.Error("failed to send password message", "err", err.Error())
			return err
		}

		resp, err = u.F.Receive()
		if err != nil {
			u.logger.Error("failed to receive password response", "err", err.Error())
			return err
		}
		u.logger.Debug("received password response", "payload", resp)

		switch resp := resp.(type) {
		case *pgproto3.AuthenticationOk:
			// Do nothing
			break
		default:
			u.logger.Error("unsupported auth method", "payload", resp)
			return nil
		}
	}

	/**
	RFQ SEQUENCE
	**/

	isReadyForQuery := false
	for !isReadyForQuery {
		msg, err := u.F.Receive()
		if err != nil {
			u.logger.Error("failed to receive initial params messages", "err", err.Error())
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			isReadyForQuery = true
		case *pgproto3.BackendKeyData:
			u.Pid = msg.ProcessID
			u.SetKey(msg.SecretKey)
		}
	}

	u.logger.Debug("ready for query", "pid", us.Pid)
	return nil
}

func (u *UpstreamConnEntry) Replay(d *DownstreamConnEntry) error {
	for query := range d.Queries().Each() {
		query := query.Value
		u.logger.Debug("replaying session query", "query", query.Query)
		switch query.Kind {
		case Prepare, Set, Listen:
			if err := u.Send(&pgproto3.Query{String: query.Query}); err != nil {
				return err
			}
		case Parse:
			if err := u.Send(&pgproto3.Parse{Name: query.Ident, Query: query.Query, ParameterOIDs: query.OIDs}); err != nil {
				return err
			}
			if err := u.Send(&pgproto3.Sync{}); err != nil {
				return err
			}
		}
		if err := readUntilReady(u); err != nil {
			return err
		}
	}
	return nil
}

func readUntilReady(u *UpstreamConnEntry) error {
	for {
		msg, err := u.F.Receive()
		if err != nil {
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return nil
		case *pgproto3.ErrorResponse:
			return errors.New(msg.Message)
		}
	}
}
