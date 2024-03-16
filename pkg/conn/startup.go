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
			slog.Error("failed to receive initial params messages", "err", err.Error())
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
			slog.Error("failed to send initial params messages", "err", err.Error())
			return err
		}
	}

	slog.Debug("ready for query", "upstreamPid", us.Pid, "downstreamPid", ds.Pid)

	return nil
}

func handleUpstreamStartup(ds *DownstreamConnEntry, us *UpstreamConnEntry) error {
	/**
	STARTUP SEQUENCE
	**/
	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      ds.Parameters,
	}
	err := us.Send(startupMsg)
	if err != nil {
		slog.Error("failed to send startup message", "err", err.Error())
		return err
	}

	fResp, err := us.F.Receive()
	if err != nil {
		slog.Error("failed to receive startup response", "err", err.Error())
		return err
	}
	slog.Debug("received startup response", "msg", fResp)

	switch fResp := fResp.(type) {
	case *pgproto3.AuthenticationOk:
		// Do nothing
		break
	default:
		// TODO: Handle other auth methods
		pwdMsg := &pgproto3.PasswordMessage{
			Password: ds.Password,
		}
		err = us.Send(pwdMsg)
		if err != nil {
			slog.Error("failed to send password message", "err", err.Error())
			return err
		}

		fResp, err = us.F.Receive()
		if err != nil {
			slog.Error("failed to receive password response", "err", err.Error())
			return err
		}
		slog.Debug("received password response", "msg", fResp)

		switch fResp := fResp.(type) {
		case *pgproto3.AuthenticationOk:
			// Do nothing
			break
		default:
			slog.Error("unsupported auth method", "msg", fResp)
			return nil
		}
	}

	/**
	RFQ SEQUENCE
	**/

	isReadyForQuery := false
	for !isReadyForQuery {
		msg, err := us.F.Receive()
		if err != nil {
			slog.Error("failed to receive initial params messages", "err", err.Error())
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			isReadyForQuery = true
		case *pgproto3.BackendKeyData:
			us.Pid = msg.ProcessID
			us.SetKey(msg.SecretKey)
		}
	}

	slog.Debug("ready for query", "upstreamPid", us.Pid, "downstreamPid", ds.Pid)
	return nil
}

func handlePostgresStartup(s *Server, ds *DownstreamConnEntry, us *UpstreamConnEntry) error {
	rStartupMsg, err := ds.B.ReceiveStartupMessage()
	if err != nil {
		slog.Error("failed to receive startup message", "err", err.Error())
		return err
	}
	slog.Debug("received startup message", "msg", rStartupMsg)

	var startupMsg *pgproto3.StartupMessage
	switch rStartupMsg := rStartupMsg.(type) {
	case *pgproto3.CancelRequest:
		// Find actual upstream for the requested processId and forward request
		if realUpstream, ok := s.downMap[rStartupMsg.ProcessID]; ok {
			if err := us.Send(&pgproto3.CancelRequest{
				ProcessID: realUpstream.Pid,
				SecretKey: realUpstream.key,
			}); err != nil {
				slog.Error("failed to send cancel request", "err", err.Error())
				return err
			}
			return ErrExpectedClose
		} else {
			slog.Warn("failed to find upstream for cancel request", "processId", rStartupMsg.ProcessID)
			return ErrExpectedClose
		}
	case *pgproto3.StartupMessage:
		startupMsg = rStartupMsg
	case *pgproto3.SSLRequest:
		_, err = ds.Conn.Write([]byte{byte('N')})
		if err != nil {
			slog.Error("failed to send SSL denial response", "err", err.Error())
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
		slog.Error("failed to upstream startup message", "err", err.Error())
		return err
	}

	fResp, err := us.F.Receive()
	if err != nil {
		slog.Error("failed to receive upstream message", "err", err.Error())
		return err
	}
	slog.Debug("received upstream startup message", "msg", fResp)

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

		slog.Error("failed to receive password response", "err", err.Error())
		return err
	}

	switch resp := resp.(type) {
	case *pgproto3.PasswordMessage:
		slog.Debug("received password response", "msg", resp)
		ds.Password = resp.Password
	}

	// TODO: Support other auth methods

	// Forward password to upstream
	err = us.Send(resp)
	if err != nil {
		slog.Error("failed to upstream password", "err", err.Error())
		return err
	}

	fResp, err = us.F.Receive()
	if err != nil {
		slog.Error("failed to receive upstream password response", "err", err.Error())
		return err
	}
	slog.Debug("received upstream password response", "msg", fResp)

	ds.Send(fResp)
	return nil
}
