package api

import (
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/magnm/pg0e/pkg/metrics"
)

func StartApiServer() {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Get("/metrics", metrics.SlashMetricsHandler())

	port := "5000"
	if p, ok := os.LookupEnv("API_PORT"); ok {
		port = p
	}

	app.Listen(":" + port)
}
