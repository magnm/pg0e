package main

import (
	"io"
	"log/slog"
	"net"
	"os"

	"github.com/magnm/pg0e/pkg/conn"
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

	server.Listen()
}
