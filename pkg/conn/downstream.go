package conn

import (
	"log/slog"
	"math/rand"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

type DownstreamConnEntry struct {
	C
	B              *pgproto3.Backend
	Data           chan pgproto3.FrontendMessage
	Parameters     map[string]string
	Password       string
	SessionQueries []string
	paused         bool
	unpause        chan bool
}

func NewDownstreamEntry(conn net.Conn) *DownstreamConnEntry {
	if cc, ok := conn.(*net.TCPConn); ok {
		if err := cc.SetKeepAlivePeriod(30 * time.Second); err != nil {
			return nil
		}
		if err := cc.SetKeepAlive(true); err != nil {
			return nil
		}
	}

	// Temporary PID for mapping purposes until we get the real PID from the upstream
	pid := rand.Uint32()

	return &DownstreamConnEntry{
		C: C{
			Term: make(chan error, 1),
			Conn: conn,
			Pid:  pid,
		},
		B:       pgproto3.NewBackend(conn, conn),
		Data:    make(chan pgproto3.FrontendMessage, 100),
		unpause: make(chan bool, 1),
	}
}

func (d *DownstreamConnEntry) Close() error {
	return d.Conn.Close()
}
func (d *DownstreamConnEntry) Listen() {
	for {
		msg, err := d.B.Receive()
		if err != nil {
			d.Term <- err
			return
		}
		slog.Debug("downstream recv", "msg", msg)
		if d.paused {
			slog.Debug("downstream paused")
			<-d.unpause
			slog.Debug("downstream unpaused")
		}
		d.Data <- msg
	}
}

func (d *DownstreamConnEntry) Send(msg pgproto3.BackendMessage) error {
	d.B.Send(msg)
	return d.B.Flush()
}

func (d *DownstreamConnEntry) Pause() {
	if !d.paused {
		d.unpause = make(chan bool, 1)
		d.paused = true
	}
}

func (d *DownstreamConnEntry) Resume() {
	if d.paused {
		d.paused = false
		d.unpause <- true
	}
}

// func (d *DownstreamConnEntry) AnalyzeMsg(msg pgproto3.FrontendMessage) {
// 	switch msg := (msg).(type) {
// 	case *pgproto3.Query:
// 		if msg.String
// 	}
// }
