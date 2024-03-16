package metrics

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

type Int64GaugeCallback func(context.Context) int64

var provider *metric.MeterProvider
var meter api.Meter
var querySendCounter, queryRecvCounter, queryErrCounter, connCounter api.Int64Counter
var queryTimeHistogram api.Float64Histogram

var useConnAttr bool

func init() {
	resourceAttrs := []attribute.KeyValue{attribute.String("service_name", "pg0e")}
	resource, _ := resource.New(
		context.Background(),
		resource.WithAttributes(resourceAttrs...),
		resource.WithHost(),
		resource.WithTelemetrySDK(),
	)
	provider = metric.NewMeterProvider(metric.WithReader(exporter), metric.WithResource(resource))
	meter = provider.Meter("pg0e", api.WithInstrumentationVersion("1.0.0"))

	querySendCounter, _ = meter.Int64Counter("query.send", api.WithDescription("Number of queries sent"))
	queryRecvCounter, _ = meter.Int64Counter("query.recv", api.WithDescription("Number of query responses"))
	queryErrCounter, _ = meter.Int64Counter("query.err", api.WithDescription("Number of query errors"))
	connCounter, _ = meter.Int64Counter("connection", api.WithDescription("Lifetime number of connections"))
	queryTimeHistogram, _ = meter.Float64Histogram(
		"query.time",
		api.WithExplicitBucketBoundaries(0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1, 2, 5),
		api.WithDescription("Query time in seconds"),
		api.WithUnit("s"),
	)

	useConnAttr = os.Getenv("METRICS_PER_CONN") == "true"
}

func ConnGauge(cb Int64GaugeCallback) {
	meter.Int64ObservableGauge("connection", api.WithInt64Callback(func(ctx context.Context, io api.Int64Observer) error {
		io.Observe(cb(ctx))
		return nil
	}), api.WithDescription("Current number of connections"))
}

func UniqQueryGauge(cb Int64GaugeCallback) {
	meter.Int64ObservableGauge("query.uniq", api.WithInt64Callback(func(ctx context.Context, io api.Int64Observer) error {
		io.Observe(cb(ctx))
		return nil
	}), api.WithDescription("Number of unique queries seen"))
}

func IncQuerySend(connId string) {
	var attrs []attribute.KeyValue
	if useConnAttr {
		attrs = []attribute.KeyValue{attribute.String("conn_id", connId)}
	}
	querySendCounter.Add(context.Background(), 1, api.WithAttributes(attrs...))
}

func IncQueryRecv(connId string) {
	var attrs []attribute.KeyValue
	if useConnAttr {
		attrs = []attribute.KeyValue{attribute.String("conn_id", connId)}
	}
	queryRecvCounter.Add(context.Background(), 1, api.WithAttributes(attrs...))
}

func IncQueryErr(connId string) {
	var attrs []attribute.KeyValue
	if useConnAttr {
		attrs = []attribute.KeyValue{attribute.String("conn_id", connId)}
	}
	queryErrCounter.Add(context.Background(), 1, api.WithAttributes(attrs...))
}

func IncConn() {
	connCounter.Add(context.Background(), 1)
}

func RecQueryTime(dur float64) {
	queryTimeHistogram.Record(context.Background(), dur)
}
