// Copyright  The OpenTelemetry Authors
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

package prometheusremotewritereceiver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/obsreport"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const (
	receiverFormat = "protobuf"
)

var reg = regexp.MustCompile(`(\w+)_(\w+)_(\w+)\z`)

// PrometheusRemoteWriteReceiver - remote write
type PrometheusRemoteWriteReceiver struct {
	params       component.ReceiverCreateSettings
	host         component.Host
	nextConsumer consumer.Metrics

	mu         sync.Mutex
	startOnce  sync.Once
	stopOnce   sync.Once
	shutdownWG sync.WaitGroup

	server  *http.Server
	config  *Config
	logger  *zap.Logger
	obsrecv *obsreport.Receiver
}

// NewReceiver - remote write
func NewReceiver(params component.ReceiverCreateSettings, config *Config, consumer consumer.Metrics) (*PrometheusRemoteWriteReceiver, error) {
	zr := &PrometheusRemoteWriteReceiver{
		params:       params,
		nextConsumer: consumer,
		config:       config,
		logger:       params.Logger,
		obsrecv: obsreport.NewReceiver(obsreport.ReceiverSettings{
			ReceiverID:             config.ID(),
			ReceiverCreateSettings: params,
		}),
	}
	return zr, nil
}

// Start - remote write
func (rec *PrometheusRemoteWriteReceiver) Start(_ context.Context, host component.Host) error {
	if host == nil {
		return errors.New("nil host")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var err = component.ErrNilNextConsumer
	rec.startOnce.Do(func() {
		err = nil
		rec.host = host
		rec.server, err = rec.config.HTTPServerSettings.ToServer(host, rec.params.TelemetrySettings, rec)
		var listener net.Listener
		listener, err = rec.config.HTTPServerSettings.ToListener()
		if err != nil {
			return
		}
		rec.shutdownWG.Add(1)
		go func() {
			defer rec.shutdownWG.Done()
			if errHTTP := rec.server.Serve(listener); errHTTP != http.ErrServerClosed {
				host.ReportFatalError(errHTTP)
			}
		}()
	})
	return err
}

func (rec *PrometheusRemoteWriteReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	ctx := rec.obsrecv.StartMetricsOp(r.Context())
	req, err := remote.DecodeWriteRequest(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	timeThreshold := time.Now().Add(-time.Hour * time.Duration(rec.config.TimeThreshold))
	pms := pmetric.NewMetrics()

	for _, ts := range req.Timeseries {
		pm := pms.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()

		metricName, err := finaName(ts.Labels)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		pm.SetName(metricName)
		rec.logger.Debug("Metric name", zap.String("metric_name", pm.Name()))

		match := reg.FindStringSubmatch(metricName)
		metricsType := ""
		if len(match) > 1 {
			lastSuffixInMetricName := match[len(match)-1]
			if IsValidSuffix(lastSuffixInMetricName) {
				metricsType = lastSuffixInMetricName
				if len(match) > 2 {
					secondSuffixInMetricName := match[len(match)-2]
					if IsValidUnit(secondSuffixInMetricName) {
						pm.SetUnit(secondSuffixInMetricName)
					}
				}
			} else if IsValidUnit(lastSuffixInMetricName) {
				pm.SetUnit(lastSuffixInMetricName)
			}
		}
		rec.logger.Debug("Metric unit", zap.String("metric name", pm.Name()), zap.String("metric_unit", pm.Unit()))

		for _, s := range ts.Samples {
			ppoint := pmetric.NewNumberDataPoint()
			ppoint.SetDoubleVal(s.Value)
			ppoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, s.Timestamp*int64(time.Millisecond))))
			if ppoint.Timestamp().AsTime().Before(timeThreshold) {
				rec.logger.Debug("Metric older than the threshold", zap.String("metric name", pm.Name()), zap.Time("metric_timestamp", ppoint.Timestamp().AsTime()))
				continue
			}
			for _, l := range ts.Labels {
				labelName := l.Name
				if l.Name == "__name__" {
					labelName = "key_name"
				}
				ppoint.Attributes().UpsertString(labelName, l.Value)
			}
			if IsValidCumulativeSuffix(metricsType) {
				pm.SetDataType(pmetric.MetricDataTypeSum)
				pm.Sum().SetIsMonotonic(true)
				pm.Sum().SetAggregationTemporality(pmetric.MetricAggregationTemporalityCumulative)
				ppoint.CopyTo(pm.Sum().DataPoints().AppendEmpty())
			} else {
				pm.SetDataType(pmetric.MetricDataTypeGauge)
				ppoint.CopyTo(pm.Gauge().DataPoints().AppendEmpty())
			}
			rec.logger.Debug("Metric sample",
				zap.String("metric_name", pm.Name()),
				zap.String("metric_unit", pm.Unit()),
				zap.Float64("metric_value", ppoint.DoubleVal()),
				zap.Time("metric_timestamp", ppoint.Timestamp().AsTime()),
				zap.String("metric_labels", fmt.Sprintf("%#v", ppoint.Attributes())),
			)
		}
	}

	metricCount := pms.ResourceMetrics().Len()
	dataPointCount := pms.DataPointCount()
	if metricCount != 0 {
		err = rec.nextConsumer.ConsumeMetrics(ctx, pms)
	}
	rec.obsrecv.EndMetricsOp(ctx, receiverFormat, dataPointCount, err)
	w.WriteHeader(http.StatusAccepted)
}

// Shutdown - remote write
func (rec *PrometheusRemoteWriteReceiver) Shutdown(context.Context) error {
	var err = component.ErrNilNextConsumer
	rec.stopOnce.Do(func() {
		err = rec.server.Close()
		rec.shutdownWG.Wait()
	})
	return err
}

func finaName(labels []prompb.Label) (ret string, err error) {
	for _, label := range labels {
		if label.Name == "__name__" {
			return label.Value, nil
		}
	}
	return "", errors.New("label name not found")
}

// IsValidSuffix - remote write
func IsValidSuffix(suffix string) bool {
	switch suffix {
	case
		"max",
		"sum",
		"count",
		"total":
		return true
	}
	return false
}

// IsValidCumulativeSuffix - remote write
func IsValidCumulativeSuffix(suffix string) bool {
	switch suffix {
	case
		"sum",
		"count",
		"total":
		return true
	}
	return false
}

// IsValidUnit - remote write
func IsValidUnit(unit string) bool {
	switch unit {
	case
		"seconds",
		"bytes":
		return true
	}
	return false
}
