// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestExplicitHistogramDP_StartTimestamp(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("key", "val")
	bounds := []float64{1.0, 5.0, 10.0}

	before := time.Now().Truncate(time.Millisecond)
	dp := newExplicitHistogramDP(attrs, bounds)
	after := time.Now().Add(time.Millisecond)

	assert.False(t, dp.startTime.IsZero())
	assert.True(t, !dp.startTime.Before(before))
	assert.True(t, !dp.startTime.After(after))

	dp.Aggregate(3.0, 2)

	dest := pmetric.NewHistogramDataPoint()
	copyTime := time.Now().Add(time.Second)
	dp.Copy(copyTime, dest)

	assert.Equal(t, pcommon.NewTimestampFromTime(dp.startTime), dest.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), dest.Timestamp())
	assert.Equal(t, uint64(2), dest.Count())
	assert.Equal(t, 6.0, dest.Sum())
	assert.Equal(t, "val", dest.Attributes().AsRaw()["key"])
}

func TestExplicitHistogramDP_ZeroStartTimeInitializedOnAggregate(t *testing.T) {
	dp := &explicitHistogramDP{
		attrs:  pcommon.NewMap(),
		bounds: []float64{1.0, 5.0},
		counts: make([]uint64, 3),
	}
	assert.True(t, dp.startTime.IsZero())

	before := time.Now().Truncate(time.Millisecond)
	dp.Aggregate(2.0, 1)
	assert.False(t, dp.startTime.IsZero())
	assert.True(t, !dp.startTime.Before(before))

	dest := pmetric.NewHistogramDataPoint()
	copyTime := time.Now().Add(time.Second)
	dp.Copy(copyTime, dest)

	assert.Equal(t, pcommon.NewTimestampFromTime(dp.startTime), dest.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), dest.Timestamp())
}

func TestExponentialHistogramDP_StartTimestamp(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("key", "val")

	before := time.Now().Truncate(time.Millisecond)
	dp := newExponentialHistogramDP(attrs, 4)
	after := time.Now().Add(time.Millisecond)

	assert.False(t, dp.startTime.IsZero())
	assert.True(t, !dp.startTime.Before(before))
	assert.True(t, !dp.startTime.After(after))

	dp.Aggregate(4.0, 1)

	dest := pmetric.NewExponentialHistogramDataPoint()
	copyTime := time.Now().Add(time.Second)
	dp.Copy(copyTime, dest)

	assert.Equal(t, pcommon.NewTimestampFromTime(dp.startTime), dest.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), dest.Timestamp())
	assert.Equal(t, uint64(1), dest.Count())
	assert.Equal(t, 4.0, dest.Sum())
	assert.Equal(t, "val", dest.Attributes().AsRaw()["key"])
}

func TestExponentialHistogramDP_ZeroStartTimeInitializedOnAggregate(t *testing.T) {
	dp := newExponentialHistogramDP(pcommon.NewMap(), 4)
	dp.startTime = time.Time{}
	assert.True(t, dp.startTime.IsZero())

	before := time.Now().Truncate(time.Millisecond)
	dp.Aggregate(3.0, 1)
	assert.False(t, dp.startTime.IsZero())
	assert.True(t, !dp.startTime.Before(before))

	dest := pmetric.NewExponentialHistogramDataPoint()
	copyTime := time.Now().Add(time.Second)
	dp.Copy(copyTime, dest)

	assert.Equal(t, pcommon.NewTimestampFromTime(dp.startTime), dest.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), dest.Timestamp())
}

func TestGaugeDP_StartTimestamp(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("env", "prod")

	before := time.Now().Truncate(time.Millisecond)
	dp := newGaugeDP(attrs)
	after := time.Now().Add(time.Millisecond)

	assert.False(t, dp.startTime.IsZero())
	assert.True(t, !dp.startTime.Before(before))
	assert.True(t, !dp.startTime.After(after))

	dp.Aggregate(int64(42))

	dest := pmetric.NewNumberDataPoint()
	copyTime := time.Now().Add(time.Second)
	dp.Copy(copyTime, dest)

	assert.Equal(t, pcommon.NewTimestampFromTime(dp.startTime), dest.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), dest.Timestamp())
	assert.Equal(t, int64(42), dest.IntValue())
	assert.Equal(t, "prod", dest.Attributes().AsRaw()["env"])

	// Test float value
	dpDouble := newGaugeDP(attrs)
	dpDouble.Aggregate(3.14)
	destDouble := pmetric.NewNumberDataPoint()
	dpDouble.Copy(copyTime, destDouble)
	assert.Equal(t, 3.14, destDouble.DoubleValue())
	assert.Equal(t, pcommon.NewTimestampFromTime(dpDouble.startTime), destDouble.StartTimestamp())
}

func TestGaugeDP_ZeroStartTimeInitializedOnAggregate(t *testing.T) {
	dp := &gaugeDP{
		attrs: pcommon.NewMap(),
	}
	assert.True(t, dp.startTime.IsZero())

	before := time.Now().Truncate(time.Millisecond)
	dp.Aggregate(int64(10))
	assert.False(t, dp.startTime.IsZero())
	assert.True(t, !dp.startTime.Before(before))

	dest := pmetric.NewNumberDataPoint()
	copyTime := time.Now().Add(time.Second)
	dp.Copy(copyTime, dest)

	assert.Equal(t, pcommon.NewTimestampFromTime(dp.startTime), dest.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), dest.Timestamp())
}

func TestSumDP_StartTimestamp(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("service", "cart")

	// Int sum
	before := time.Now().Truncate(time.Millisecond)
	dpInt := newSumDP(attrs, false)
	after := time.Now().Add(time.Millisecond)

	assert.False(t, dpInt.startTime.IsZero())
	assert.True(t, !dpInt.startTime.Before(before))
	assert.True(t, !dpInt.startTime.After(after))

	dpInt.AggregateInt(10)
	dpInt.AggregateInt(5)

	destInt := pmetric.NewNumberDataPoint()
	copyTime := time.Now().Add(time.Second)
	dpInt.Copy(copyTime, destInt)

	assert.Equal(t, pcommon.NewTimestampFromTime(dpInt.startTime), destInt.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), destInt.Timestamp())
	assert.Equal(t, int64(15), destInt.IntValue())
	assert.Equal(t, "cart", destInt.Attributes().AsRaw()["service"])

	// Double sum
	dpDbl := newSumDP(attrs, true)
	dpDbl.AggregateDouble(1.5)
	dpDbl.AggregateDouble(2.5)

	destDbl := pmetric.NewNumberDataPoint()
	dpDbl.Copy(copyTime, destDbl)

	assert.Equal(t, pcommon.NewTimestampFromTime(dpDbl.startTime), destDbl.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), destDbl.Timestamp())
	assert.Equal(t, 4.0, destDbl.DoubleValue())
}

func TestSumDP_ZeroStartTimeInitializedOnAggregate(t *testing.T) {
	dpInt := &sumDP{
		attrs: pcommon.NewMap(),
		isDbl: false,
	}
	assert.True(t, dpInt.startTime.IsZero())

	before := time.Now().Truncate(time.Millisecond)
	dpInt.AggregateInt(7)
	assert.False(t, dpInt.startTime.IsZero())
	assert.True(t, !dpInt.startTime.Before(before))

	destInt := pmetric.NewNumberDataPoint()
	copyTime := time.Now().Add(time.Second)
	dpInt.Copy(copyTime, destInt)

	assert.Equal(t, pcommon.NewTimestampFromTime(dpInt.startTime), destInt.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), destInt.Timestamp())

	dpDbl := &sumDP{
		attrs: pcommon.NewMap(),
		isDbl: true,
	}
	assert.True(t, dpDbl.startTime.IsZero())

	beforeDbl := time.Now().Truncate(time.Millisecond)
	dpDbl.AggregateDouble(7.5)
	assert.False(t, dpDbl.startTime.IsZero())
	assert.True(t, !dpDbl.startTime.Before(beforeDbl))

	destDbl := pmetric.NewNumberDataPoint()
	dpDbl.Copy(copyTime, destDbl)

	assert.Equal(t, pcommon.NewTimestampFromTime(dpDbl.startTime), destDbl.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), destDbl.Timestamp())
}

func TestValueCountDP_CopySetsStartTimestamp(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("metric", "test")

	vcExp := &valueCountDP{
		expHistogramDP: newExponentialHistogramDP(attrs, 4),
	}
	vcExp.Aggregate(2.0, 1)

	destExp := pmetric.NewExponentialHistogram()
	copyTime := time.Now().Add(time.Second)
	vcExp.Copy(copyTime, destExp, pmetric.NewHistogram())

	require.Equal(t, 1, destExp.DataPoints().Len())
	dpExp := destExp.DataPoints().At(0)
	assert.False(t, dpExp.StartTimestamp().AsTime().IsZero())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), dpExp.Timestamp())

	vcExplicit := &valueCountDP{
		explicitHistogramDP: newExplicitHistogramDP(attrs, []float64{1.0, 5.0}),
	}
	vcExplicit.Aggregate(3.0, 2)

	destExplicit := pmetric.NewHistogram()
	vcExplicit.Copy(copyTime, pmetric.NewExponentialHistogram(), destExplicit)

	require.Equal(t, 1, destExplicit.DataPoints().Len())
	dpExplicit := destExplicit.DataPoints().At(0)
	assert.False(t, dpExplicit.StartTimestamp().AsTime().IsZero())
	assert.Equal(t, pcommon.NewTimestampFromTime(copyTime), dpExplicit.Timestamp())
}
