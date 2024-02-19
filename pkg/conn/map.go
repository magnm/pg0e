package conn

import (
	"errors"
	"math/rand"
	"net"
	"time"
)

var ErrExpectedClose = errors.New("expected close")
var ErrExpectedSwitch = errors.New("expected switch")

type C struct {
	Term chan error
	Pid  uint32
	key  uint32

	Conn net.Conn
}

type UpToDownMap map[uint32]*DownstreamConnEntry
type DownToUpMap map[uint32]*UpstreamConnEntry

func NewConn(conn net.Conn) *C {
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

	return &C{
		Term: make(chan error, 1),
		Conn: conn,
		Pid:  pid,
	}
}

func (c *C) SetKey(key uint32) {
	c.key = key
}

func (c *C) SetPid(pid uint32) {
	c.Pid = pid
}
