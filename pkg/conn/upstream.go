package conn

import (
	"log/slog"
	"net"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

type UpstreamConnEntry struct {
	*C
	F         *pgproto3.Frontend
	Data      chan pgproto3.BackendMessage
	isClosing bool
}

func NewUpstreamEntry(conn net.Conn) *UpstreamConnEntry {
	return &UpstreamConnEntry{
		C:    NewConn(conn),
		F:    pgproto3.NewFrontend(conn, conn),
		Data: make(chan pgproto3.BackendMessage, 100),
	}
}

func (u *UpstreamConnEntry) Close() error {
	u.isClosing = true
	return u.Conn.Close()
}

func (u *UpstreamConnEntry) Listen() {
	slog.Debug("upstream listening", "addr", u.Conn.RemoteAddr().String())
	for {
		msg, err := u.F.Receive()
		if err != nil {
			if u.isClosing && strings.Contains(err.Error(), "closed network connection") {
				u.Term <- ErrExpectedClose
				return
			}
			u.Term <- err
			return
		}
		slog.Debug("upstream recv", "msg", msg)

		switch msg := msg.(type) {
		case *pgproto3.ErrorResponse:
			if msg.Severity == "FATAL" && slices.Contains([]string{"57P01", "57P02", "57P03"}, msg.Code) {
				slog.Debug("upstream is terminating, dropping message")
				continue
			}
		}

		u.Data <- msg
	}
}

func (u *UpstreamConnEntry) Send(msg pgproto3.FrontendMessage) error {
	u.F.Send(msg)
	return u.F.Flush()
}

func (u *UpstreamConnEntry) Startup(d *DownstreamConnEntry) error {
	return handleUpstreamStartup(d, u)
}

func (u *UpstreamConnEntry) Replay(d *DownstreamConnEntry) error {
	for _, query := range d.Queries() {
		slog.Debug("replaying session query", "query", query.Query)
	}
	return nil
}
