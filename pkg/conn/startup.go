package conn

import (
	"errors"
	"io"
	"log/slog"

	"github.com/jackc/pgx/v5/pgproto3"
)

func (s *Server) handleRegularStartup(ds *DownstreamConnEntry, us *UpstreamConnEntry) error {
	err := handlePostgresStartup(s, ds, us)
	if err != nil {
		return err
	}

	// Handle everything until ReadyForQuery
	for !ds.readyForQuery {
		msg, err := us.F.Receive()
		if err != nil {
			us.logger.Error("failed to receive initial params messages", "err", err.Error())
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			ds.readyForQuery = true
		case *pgproto3.BackendKeyData:
			us.Pid = msg.ProcessID
			us.SetKey(msg.SecretKey)
			ds.Pid = msg.ProcessID
			ds.SetKey(msg.SecretKey)
		}

		err = ds.Send(msg)
		if err != nil {
			ds.logger.Error("failed to send initial params messages", "err", err.Error())
			return err
		}
	}

	ds.logger.Debug("ready for query", "upstreamPid", us.Pid, "downstreamPid", ds.Pid)

	return nil
}



func handlePostgresStartup(s *Server, ds *DownstreamConnEntry, us *UpstreamConnEntry) error {
	rStartupMsg, err := ds.B.ReceiveStartupMessage()
	if err != nil {
		slog.Error("failed to receive startup message", "err", err.Error())
		return err
	}
	ds.logger.Debug("received startup message", "msg", rStartupMsg)

	var startupMsg *pgproto3.StartupMessage
	switch rStartupMsg := rStartupMsg.(type) {
	case *pgproto3.CancelRequest:
		// Find actual upstream for the requested processId and forward request
		if realUpstream, ok := s.downMap[rStartupMsg.ProcessID]; ok {
			if err := us.Send(&pgproto3.CancelRequest{
				ProcessID: realUpstream.Pid,
				SecretKey: realUpstream.key,
			}); err != nil {
				ds.logger.Error("failed to send cancel request", "err", err.Error())
				return err
			}
			return ErrExpectedClose
		} else {
			ds.logger.Warn("failed to find upstream for cancel request", "processId", rStartupMsg.ProcessID)
			return ErrExpectedClose
		}
	case *pgproto3.StartupMessage:
		startupMsg = rStartupMsg
	case *pgproto3.SSLRequest:
		_, err = ds.Conn.Write([]byte{byte('N')})
		if err != nil {
			ds.logger.Error("failed to send SSL denial response", "err", err.Error())
			return err
		}
		// Client should redo the startup after getting denied
		return handlePostgresStartup(s, ds, us)
	}

	// Store startup params for the downstream for later
	ds.Parameters = startupMsg.Parameters

	// Forward startup to upstream
	err = us.Send(startupMsg)
	if err != nil {
		ds.logger.Error("failed to upstream startup message", "err", err.Error())
		return err
	}

	fResp, err := us.F.Receive()
	if err != nil {
		ds.logger.Error("failed to receive upstream message", "err", err.Error())
		return err
	}
	ds.logger.Debug("received upstream startup message", "msg", fResp)

	switch fResp := fResp.(type) {
	case *pgproto3.AuthenticationOk:
		// Do nothing
		ds.Send(fResp)
		return nil
	}

	// Ask client for cleartext password
	ds.Send(&pgproto3.AuthenticationCleartextPassword{})

	// Receive password
	resp, err := ds.B.Receive()
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// Close upstream since we'll be waiting for a new connection with password
			us.Close()
			return ErrExpectedClose
		}

		ds.logger.Error("failed to receive password response", "err", err.Error())
		return err
	}

	switch resp := resp.(type) {
	case *pgproto3.PasswordMessage:
		ds.logger.Debug("received password response", "msg", resp)
		ds.Password = resp.Password
	}

	// TODO: Support other auth methods

	// Forward password to upstream
	err = us.Send(resp)
	if err != nil {
		ds.logger.Error("failed to upstream password", "err", err.Error())
		return err
	}

	fResp, err = us.F.Receive()
	if err != nil {
		ds.logger.Error("failed to receive upstream password response", "err", err.Error())
		return err
	}
	ds.logger.Debug("received upstream password response", "msg", fResp)

	ds.Send(fResp)
	return nil
}
