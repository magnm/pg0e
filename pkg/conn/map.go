package conn

import (
	"errors"
	"net"
)

var ErrExpectedClose = errors.New("expected close")

type C struct {
	Term chan error
	Pid  uint32
	key  uint32

	Conn net.Conn
}

type UpToDownMap map[uint32]*DownstreamConnEntry
type DownToUpMap map[uint32]*UpstreamConnEntry

func (c *C) SetKey(key uint32) {
	c.key = key
}

func (c *C) SetPid(pid uint32) {
	c.Pid = pid
}
