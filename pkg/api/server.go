package api

import (
	"fmt"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/magnm/pg0e/pkg/conn"
)

type APIServer struct {
	app *fiber.App
}

func NewAPIServer(server *conn.Server) *APIServer {
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

	return &APIServer{app: app}
}

func (s *APIServer) Listen() {
	port, ok := os.LookupEnv("API_PORT")
	if !ok {
		port = "3000"
	}

	s.app.Listen(fmt.Sprintf(":%s", port))
}
