// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package prometheusremotewriteexporter implements an exporter that sends Prometheus remote write requests.
package clickhousemetricsexporter

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/prometheus/prometheus/prompb"

	"github.com/SigNoz/signoz-otel-collector/exporter/clickhousemetricsexporter/base"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const maxBatchByteSize = 90000000

// PrwExporter converts OTLP metrics to Prometheus remote write TimeSeries and sends them to a remote endpoint.
type PrwExporter struct {
	namespace       string
	externalLabels  map[string]string
	endpointURL     *url.URL
	client          *http.Client
	wg              *sync.WaitGroup
	closeChan       chan struct{}
	concurrency     int
	userAgentHeader string
	clientSettings  *confighttp.HTTPClientSettings
	settings        component.TelemetrySettings
	ch              base.Storage
}

// NewPrwExporter initializes a new PrwExporter instance and sets fields accordingly.
// client parameter cannot be nil.
func NewPrwExporter(cfg *Config, set component.ExporterCreateSettings) (*PrwExporter, error) {

	sanitizedLabels, err := validateAndSanitizeExternalLabels(cfg.ExternalLabels)
	if err != nil {
		return nil, err
	}

	endpointURL, err := url.ParseRequestURI(cfg.HTTPClientSettings.Endpoint)
	if err != nil {
		return nil, errors.New("invalid endpoint")
	}

	userAgentHeader := fmt.Sprintf("%s/%s", strings.ReplaceAll(strings.ToLower(set.BuildInfo.Description), " ", "-"), set.BuildInfo.Version)

	params := &ClickHouseParams{
		DSN:                  cfg.HTTPClientSettings.Endpoint,
		DropDatabase:         false,
		MaxOpenConns:         75,
		MaxTimeSeriesInQuery: 50,
	}
	ch, err := NewClickHouse(params)
	if err != nil {
		zap.S().Error("couldn't create instance of clickhouse")
	}

	return &PrwExporter{
		namespace:       cfg.Namespace,
		externalLabels:  sanitizedLabels,
		endpointURL:     endpointURL,
		wg:              new(sync.WaitGroup),
		closeChan:       make(chan struct{}),
		userAgentHeader: userAgentHeader,
		concurrency:     cfg.RemoteWriteQueue.NumConsumers,
		clientSettings:  &cfg.HTTPClientSettings,
		settings:        set.TelemetrySettings,
		ch:              ch,
	}, nil
}

// Start creates the prometheus client
func (prwe *PrwExporter) Start(_ context.Context, host component.Host) (err error) {
	prwe.client, err = prwe.clientSettings.ToClient(host.GetExtensions(), prwe.settings)
	return err
}

// Shutdown stops the exporter from accepting incoming calls(and return error), and wait for current export operations
// to finish before returning
func (prwe *PrwExporter) Shutdown(context.Context) error {
	close(prwe.closeChan)
	prwe.wg.Wait()
	return nil
}

// PushMetrics converts metrics to Prometheus remote write TimeSeries and send to remote endpoint. It maintain a map of
// TimeSeries, validates and handles each individual metric, adding the converted TimeSeries to the map, and finally
// exports the map.
func (prwe *PrwExporter) PushMetrics(ctx context.Context, md pmetric.Metrics) error {
	prwe.wg.Add(1)
	defer prwe.wg.Done()

	select {
	case <-prwe.closeChan:
		return errors.New("shutdown has been called")
	default:
		start := time.Now()
		tsMap := map[string]*prompb.TimeSeries{}
		dropped := 0
		var errs error
		resourceMetricsSlice := md.ResourceMetrics()
		for i := 0; i < resourceMetricsSlice.Len(); i++ {
			resourceMetrics := resourceMetricsSlice.At(i)
			resource := resourceMetrics.Resource()
			scopeMetricsSlice := resourceMetrics.ScopeMetrics()
			// TODO: add resource attributes as labels, probably in next PR
			for j := 0; j < scopeMetricsSlice.Len(); j++ {
				scopeMetrics := scopeMetricsSlice.At(j)
				metricSlice := scopeMetrics.Metrics()

				// TODO: decide if scope information should be exported as labels
				for k := 0; k < metricSlice.Len(); k++ {
					metric := metricSlice.At(k)

					// check for valid type and temporality combination and for matching data field and type
					if ok := validateMetrics(metric); !ok {
						dropped++
						errs = multierr.Append(errs, consumererror.NewPermanent(errors.New("invalid temporality and type combination")))
						serviceName, found := resource.Attributes().Get("service.name")
						if !found {
							serviceName = pcommon.NewValueString("<missing-svc>")
						}
						metricType := metric.DataType()
						var numDataPoints int
						var temporality pmetric.MetricAggregationTemporality
						switch metricType {
						case pmetric.MetricDataTypeGauge:
							numDataPoints = metric.Gauge().DataPoints().Len()
						case pmetric.MetricDataTypeSum:
							numDataPoints = metric.Sum().DataPoints().Len()
							temporality = metric.Sum().AggregationTemporality()
						case pmetric.MetricDataTypeHistogram:
							numDataPoints = metric.Histogram().DataPoints().Len()
							temporality = metric.Histogram().AggregationTemporality()
						case pmetric.MetricDataTypeSummary:
							numDataPoints = metric.Summary().DataPoints().Len()
						default:
						}
						zap.S().Errorf("dropped %d number of metric data points of type %d with temporality %d for a service %s", numDataPoints, metricType, temporality, serviceName.AsString())
						continue
					}

					// handle individual metric based on type
					switch metric.DataType() {
					case pmetric.MetricDataTypeGauge:
						dataPoints := metric.Gauge().DataPoints()
						if err := prwe.addNumberDataPointSlice(dataPoints, tsMap, resource, metric); err != nil {
							dropped++
							errs = multierr.Append(errs, err)
						}
					case pmetric.MetricDataTypeSum:
						dataPoints := metric.Sum().DataPoints()
						if err := prwe.addNumberDataPointSlice(dataPoints, tsMap, resource, metric); err != nil {
							dropped++
							errs = multierr.Append(errs, err)
						}
					case pmetric.MetricDataTypeHistogram:
						dataPoints := metric.Histogram().DataPoints()
						if dataPoints.Len() == 0 {
							dropped++
							errs = multierr.Append(errs, consumererror.NewPermanent(fmt.Errorf("empty data points. %s is dropped", metric.Name())))
						}
						for x := 0; x < dataPoints.Len(); x++ {
							addSingleHistogramDataPoint(dataPoints.At(x), resource, metric, prwe.namespace, tsMap, prwe.externalLabels)
						}
					case pmetric.MetricDataTypeSummary:
						dataPoints := metric.Summary().DataPoints()
						if dataPoints.Len() == 0 {
							dropped++
							errs = multierr.Append(errs, consumererror.NewPermanent(fmt.Errorf("empty data points. %s is dropped", metric.Name())))
						}
						for x := 0; x < dataPoints.Len(); x++ {
							addSingleSummaryDataPoint(dataPoints.At(x), resource, metric, prwe.namespace, tsMap, prwe.externalLabels)
						}
					default:
						dropped++
						errs = multierr.Append(errs, consumererror.NewPermanent(errors.New("unsupported metric type")))
					}
				}
			}
		}

		if exportErrors := prwe.export(ctx, tsMap); len(exportErrors) != 0 {
			dropped = md.MetricCount()
			errs = multierr.Append(errs, multierr.Combine(exportErrors...))
		}

		fmt.Printf("PushMetrics took %s", time.Since(start))
		if dropped != 0 {
			return errs
		}

		return nil
	}
}

func validateAndSanitizeExternalLabels(externalLabels map[string]string) (map[string]string, error) {
	sanitizedLabels := make(map[string]string)
	for key, value := range externalLabels {
		if key == "" || value == "" {
			return nil, fmt.Errorf("prometheus remote write: external labels configuration contains an empty key or value")
		}

		// Sanitize label keys to meet Prometheus Requirements
		if len(key) > 2 && key[:2] == "__" {
			key = "__" + sanitize(key[2:])
		} else {
			key = sanitize(key)
		}
		sanitizedLabels[key] = value
	}

	return sanitizedLabels, nil
}

func (prwe *PrwExporter) addNumberDataPointSlice(dataPoints pmetric.NumberDataPointSlice, tsMap map[string]*prompb.TimeSeries, resource pcommon.Resource, metric pmetric.Metric) error {
	if dataPoints.Len() == 0 {
		return consumererror.NewPermanent(fmt.Errorf("empty data points. %s is dropped", metric.Name()))
	}
	for x := 0; x < dataPoints.Len(); x++ {
		addSingleNumberDataPoint(dataPoints.At(x), resource, metric, prwe.namespace, tsMap, prwe.externalLabels)
	}
	return nil
}

// export sends a Snappy-compressed WriteRequest containing TimeSeries to a remote write endpoint in order
func (prwe *PrwExporter) export(ctx context.Context, tsMap map[string]*prompb.TimeSeries) []error {
	start := time.Now()
	var errs []error
	// Calls the helper function to convert and batch the TsMap to the desired format
	requests, err := batchTimeSeries(tsMap, maxBatchByteSize)
	if err != nil {
		errs = append(errs, consumererror.NewPermanent(err))
		return errs
	}

	input := make(chan *prompb.WriteRequest, len(requests))
	for _, request := range requests {
		input <- request
	}
	close(input)

	var mu sync.Mutex
	var wg sync.WaitGroup

	concurrencyLimit := int(math.Min(float64(prwe.concurrency), float64(len(requests))))
	wg.Add(concurrencyLimit) // used to wait for workers to be finished

	// Run concurrencyLimit of workers until there
	// is no more requests to execute in the input channel.
	for i := 0; i < concurrencyLimit; i++ {
		go func() {
			defer wg.Done()

			for request := range input {
				err := prwe.ch.Write(ctx, request)
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	fmt.Printf("Exporting %d TimeSeries took %s", len(tsMap), time.Since(start))

	return errs
}
