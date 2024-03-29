package local

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/magnm/pg0e/pkg/interfaces"
)

type LocalOrchestrator struct {
	app    *fiber.App
	server interfaces.Server
	logger *slog.Logger
}

func NewLocalOrchestrator(server interfaces.Server) *LocalOrchestrator {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Get("/switch", func(c *fiber.Ctx) error {
		go server.InitiateSwitch()
		return c.SendString("ok")
	})
	app.Get("/unpause", func(c *fiber.Ctx) error {
		go server.UnpauseAll()
		return c.SendString("ok")
	})

	return &LocalOrchestrator{
		app:    app,
		server: server,
		logger: slog.With("orch", "local"),
	}
}

func (l *LocalOrchestrator) Start() error {
	port, ok := os.LookupEnv("API_PORT")
	if !ok {
		port = "3000"
	}

	return l.app.Listen(fmt.Sprintf(":%s", port))
}

func (l *LocalOrchestrator) Stop() error {
	return l.app.Shutdown()
}

func (l *LocalOrchestrator) GetServer() interfaces.Server {
	return l.server
}

func (l *LocalOrchestrator) TriggerSwitchover() error {
	exec.Command("sudo", "systemctl", "stop", "postgresql").Run()
	time.Sleep(5 * time.Second)
	return exec.Command("sudo", "systemctl", "start", "postgresql").Run()
}
