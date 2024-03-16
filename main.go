package main

import (
	"io"
	"log/slog"
	"net"
	"os"

	"github.com/magnm/pg0e/pkg/api"
	"github.com/magnm/pg0e/pkg/conn"
	"github.com/magnm/pg0e/pkg/interfaces"
	"github.com/magnm/pg0e/pkg/kube/cnpg"
	"github.com/magnm/pg0e/pkg/local"
)

func init() {
	writer := io.Writer(os.Stdout)
	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(handler))
}

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":6432"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("failed to start pg0e", "addr", addr)
	}

	server := conn.New(listener)

	var orchestrator interfaces.Orchestrator
	switch os.Getenv("ORCHESTRATOR") {
	case "cnpg":
		orchestrator = cnpg.New(server)
	default:
		slog.Warn("no known orchestrator specified, defaulting to local")
		orchestrator = local.NewLocalOrchestrator(server)
	}

	server.SetOrchestrator(orchestrator)

	go api.StartApiServer()
	go orchestrator.Start()
	server.Listen()
}
