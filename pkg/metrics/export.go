package metrics

import (
	"context"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var exporter, _ = prometheus.New()

func GetMetrics() (*metricdata.ResourceMetrics, error) {
	out := &metricdata.ResourceMetrics{}

	err := exporter.Collect(context.Background(), out)
	return out, err
}

func SlashMetricsHandler() fiber.Handler {
	return adaptor.HTTPHandler(promhttp.Handler())
}
