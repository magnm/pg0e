package conn

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Server struct {
	l net.Listener

	upMap   UpToDownMap
	downMap DownToUpMap
}

func New(listener net.Listener) *Server {
	return &Server{
		l:       listener,
		upMap:   make(UpToDownMap),
		downMap: make(DownToUpMap),
	}
}

func (s *Server) Listen() {
	e := make(chan error, 1)
	go func() {
		for {
			conn, err := s.l.Accept()
			if err != nil {
				e <- err
				return
			}

			go s.handleConn(conn)
		}
	}()

	slog.Info("listening", "addr", s.l.Addr().String())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
		slog.Info("quitting")
		s.l.Close()
		// TODO: Close servers
	case err := <-e:
		slog.Error("error", "err", err.Error())
		s.l.Close()
	}
}

func (s *Server) Map(ds *DownstreamConnEntry, us *UpstreamConnEntry) {
	s.upMap[us.Pid] = ds
	s.downMap[ds.Pid] = us
}

func (s *Server) UnMap(ds *DownstreamConnEntry, us *UpstreamConnEntry) {
	delete(s.upMap, us.Pid)
	delete(s.downMap, ds.Pid)
}

func (s *Server) InitiateSwitch() {
	slog.Debug("preparing for switch", "downstreams", len(s.downMap))
	control := NewControl()
	if control == nil {
		slog.Error("failed to connect to control, we'll do what we can")
	}
	control.AttachLock()
	timeStart := time.Now()

	pauseCb := make(chan bool, len(s.downMap))
	for _, ds := range s.upMap {
		ds.Pause(pauseCb)
	}
	timeout := time.After(5 * time.Second)
	allPaused := false
	for !allPaused {
		select {
		case <-timeout:
			slog.Info("timeout, have to kill hanging connections")
			for _, ds := range s.upMap {
				if !ds.IsPaused() {
					ds.SendTerminalError()
					ds.Close()
				}
			}
			allPaused = true
		case <-pauseCb:
			allPaused = true
			for _, ds := range s.upMap {
				if !ds.IsPaused() {
					allPaused = false
					break
				}
			}
		}
	}
	slog.Info("ready for switch")

	// Make sure at least 2 seconds have passed,
	// otherwise sleep until 2s. Just to try to make sure all
	// other clients have had a chance to prepare.
	totalTime := time.Since(timeStart)
	if totalTime < 2*time.Second {
		time.Sleep(2*time.Second - totalTime)
	}

	control.ReleaseLock()

	// If there are now no more clients, we can let the switch continue
	if control.GetControlCount() != 0 {
		slog.Info("clients still connected, leaving switch to someone else")
		return
	}
	slog.Info("no more clients, acquiring exclusivity for switch")
	if err := control.AcquireExclusivity(); err != nil {
		if !errors.Is(err, ErrNoConnection) && !errors.Is(err, ErrFailedExclusivity) {
			slog.Error("failed to acquire exclusivity", "err", err.Error())
		}
		return
	}
	slog.Info("exclusivity acquired, switching!")

	// TODO: Trigger switch
}

func (s *Server) UnpauseAll() {
	for _, ds := range s.upMap {
		ds.Resume()
	}
}

func (s *Server) handleConn(downstreamConn net.Conn) {
	downstream := NewDownstreamEntry(downstreamConn)
	defer downstream.Close()
	slog.Info("new downstream", "addr", downstreamConn.RemoteAddr().String())

	upstream := connectUpstream(0)
	if upstream == nil {
		slog.Error("failed to connect to upstream")
		return
	}
	defer upstream.Close()

	if err := s.handleRegularStartup(downstream, upstream); err != nil {
		if !errors.Is(err, ErrExpectedClose) {
			slog.Error("failed to handle startup", "err", err.Error())
		}
		return
	}
	s.Map(downstream, upstream)

	go downstream.Listen()
	go upstream.Listen()

	for {
		select {
		case msg := <-downstream.Data:
			upstream.Send(msg)
		case msg := <-upstream.Data:
			go downstream.AnalyzeResponseMsg(msg)
			downstream.Send(msg)

		case err := <-downstream.Term:
			if errors.Is(err, io.ErrUnexpectedEOF) {
				slog.Info("downstream closed")
				s.UnMap(downstream, upstream)
				return
			}
			slog.Error("downstream error", "err", err.Error())
			s.UnMap(downstream, upstream)
			return
		case err := <-upstream.Term:
			if errors.Is(err, ErrExpectedClose) {
				slog.Info("upstream closed")
				s.UnMap(downstream, upstream)
				return
			}
			downstream.Pause(nil)
			if downstream.State.Tx || !downstream.readyForQuery {
				downstream.SendTerminalError()
			}
			slog.Error("upstream error", "err", err.Error())
			s.UnMap(downstream, upstream)
			upstream.Close()
			time.Sleep(2 * time.Second) // TODO: More intelligent
			upstream = connectUpstream(0)
			if upstream == nil {
				slog.Error("failed to reconnect to upstream")
				return
			}
			err = upstream.Startup(downstream)
			if err != nil {
				slog.Error("failed to restart upstream", "err", err.Error())
				return
			}
			err = upstream.Replay(downstream)
			if err != nil {
				slog.Error("failed to replay upstream", "err", err.Error())
				return
			}

			// Ready
			s.Map(downstream, upstream)
			go upstream.Listen()
			downstream.Resume()
		}
	}
}

func connectUpstream(totalSlept float32) *UpstreamConnEntry {
	if totalSlept > 15_000 {
		return nil
	}
	pgHost := os.Getenv("PGHOST")
	pgPort := os.Getenv("PGPORT")
	if pgHost == "" || pgPort == "" {
		pgHost = "localhost"
		pgPort = "5432"
	}
	upstreamConn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", pgHost, pgPort))
	if err != nil {
		slog.Error("failed to connect to upstream", "err", err.Error())
		sleep := rand.Float32() * 2 * 1000
		time.Sleep(time.Duration(sleep * float32(time.Millisecond)))
		return connectUpstream(totalSlept + sleep)
	}
	return NewUpstreamEntry(upstreamConn)
}
