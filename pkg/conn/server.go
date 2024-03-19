package conn

import (
	"context"
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

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/magnm/pg0e/pkg/interfaces"
	"github.com/magnm/pg0e/pkg/metrics"
)

type Server struct {
	l         net.Listener
	paused    bool
	onUnpause chan bool

	upMap   UpToDownMap
	downMap DownToUpMap

	orchestrator interfaces.Orchestrator
}

func New(listener net.Listener) *Server {
	return &Server{
		l:         listener,
		upMap:     make(UpToDownMap),
		downMap:   make(DownToUpMap),
		onUnpause: make(chan bool, 1),
	}
}

func (s *Server) SetOrchestrator(orchestrator interfaces.Orchestrator) {
	s.orchestrator = orchestrator
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

	metrics.ConnGauge(func(ctx context.Context) int64 {
		return int64(len(s.upMap))
	})
	metrics.UniqQueryGauge(func(ctx context.Context) int64 {
		totalMap := make(map[uint32]bool)
		for _, ds := range s.upMap {
			for k := range ds.instrument.UniqQueries {
				totalMap[k] = true
			}
		}
		return int64(len(totalMap))
	})

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
	control.AttachPreparationLock()
	timeStart := time.Now()

	s.paused = true
	defer func() {
		s.paused = false
		s.onUnpause <- true
	}()

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
	if totalTime < time.Second {
		time.Sleep(time.Second - totalTime)
	}

	control.ReleasePreparationLock()

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

	if err := s.orchestrator.TriggerSwitchover(); err != nil {
		slog.Error("failed to trigger switchover", "err", err.Error())
	} else {
		// When control gets disconnected, the switch will be in progress
		// and other upstreams won't be able to connect until new primary is up.
		// So when we exit this function, s.paused will go false and general
		// functionality will resume, with upstreams attempting to reconnect.
		control.WaitForDisconnect()
	}
}

func (s *Server) UnpauseAll() {
	for _, ds := range s.upMap {
		ds.Resume()
	}
}

func (s *Server) handleConn(downstreamConn net.Conn) {
	downstream := NewDownstreamEntry(downstreamConn)
	defer downstream.Close()
	downstream.logger.Info("new downstream", "total", len(s.downMap))

	if s.paused {
		<-s.onUnpause
	}

	upstream := startUpstream(0)
	if upstream == nil {
		downstream.logger.Error("failed to connect to upstream")
		return
	}
	defer upstream.Close()
	upstream.logger.Info("new upstream", "downstream", downstreamConn.RemoteAddr().String())

	if err := s.handleRegularStartup(downstream, upstream); err != nil {
		if !errors.Is(err, ErrExpectedClose) {
			upstream.logger.Error("failed to handle startup", "err", err.Error())
		}
		return
	}

	go downstream.AnalyzeMessages()
	go downstream.Listen(func(msg pgproto3.FrontendMessage) error {
		downstream.MessageQueue <- msg
		return upstream.Send(msg)
	})
	s.connectAndMap(downstream, upstream)

	for {
		select {
		case err := <-downstream.Term:
			if errors.Is(err, io.ErrUnexpectedEOF) {
				downstream.logger.Info("downstream closed")
			} else {
				downstream.logger.Error("downstream error", "err", err.Error())
			}
			s.UnMap(downstream, upstream)
			return
		case err := <-upstream.Term:
			if errors.Is(err, ErrExpectedClose) {
				upstream.logger.Info("upstream closed")
				s.UnMap(downstream, upstream)
				return
			}
			downstream.Pause(nil)
			if downstream.State.Tx || !downstream.readyForQuery {
				downstream.SendTerminalError()
			}
			upstream.logger.Error("upstream error", "err", err.Error())
			s.UnMap(downstream, upstream)
			upstream.Close()
			time.Sleep(2 * time.Second) // TODO: More intelligent
			upstream = startUpstream(0)
			if upstream == nil {
				downstream.logger.Error("failed to reconnect to upstream")
				return
			}
			upstream.logger.Info("reconnected to upstream", "downstream", downstreamConn.RemoteAddr().String())
			err = upstream.Startup(downstream)
			if err != nil {
				upstream.logger.Error("failed to restart upstream", "err", err.Error())
				return
			}
			err = upstream.Replay(downstream)
			if err != nil {
				upstream.logger.Error("failed to replay upstream", "err", err.Error())
				return
			}

			// Ready
			s.connectAndMap(downstream, upstream)
			downstream.Resume()
		}
	}
}

func (s *Server) connectAndMap(ds *DownstreamConnEntry, us *UpstreamConnEntry) {
	s.Map(ds, us)

	go us.Listen(func(msg pgproto3.BackendMessage) error {
		ds.MessageQueue <- msg
		return ds.Send(msg)
	})
	metrics.IncConn()
}

func startUpstream(totalSlept float32) *UpstreamConnEntry {
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
		return startUpstream(totalSlept + sleep)
	}
	return NewUpstreamEntry(upstreamConn)
}
