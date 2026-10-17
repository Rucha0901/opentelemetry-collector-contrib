// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregator // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector/internal/aggregator"

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// gaugeDP is a data point for gauge metrics.
type gaugeDP struct {
	attrs     pcommon.Map
	startTime time.Time
	val       any
}

func newGaugeDP(attrs pcommon.Map) *gaugeDP {
	return &gaugeDP{
		attrs:     attrs,
		startTime: time.Now(),
	}
}

func (dp *gaugeDP) Aggregate(v any) {
	if dp.startTime.IsZero() {
		dp.startTime = time.Now()
	}
	switch v := v.(type) {
	case float64, int64:
		dp.val = v
	default:
		panic("unexpected usage of gauge datapoint, only double or int value expected")
	}
}

// Copy copies the gauge data point to the destination number data point
func (dp *gaugeDP) Copy(
	timestamp time.Time,
	dest pmetric.NumberDataPoint,
) {
	dp.attrs.CopyTo(dest.Attributes())
	switch v := dp.val.(type) {
	case float64:
		dest.SetDoubleValue(v)
	case int64:
		dest.SetIntValue(v)
	}
	dest.SetStartTimestamp(pcommon.NewTimestampFromTime(dp.startTime))
	dest.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
}
