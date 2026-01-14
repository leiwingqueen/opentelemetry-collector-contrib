package prompb

import (
	"fmt"
	"sync"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/prometheus/prometheus/model/labels"
)

// GetWriteV2Unmarshaler returns WriteV2Unmarshaler from the pool.
//
// Return the WriteV2Unmarshaler to the pool when it is no longer needed via PutWriteV2Unmarshaler call.
func GetWriteV2Unmarshaler() *WriteV2Unmarshaler {
	v := wv2uPool.Get()
	if v == nil {
		return &WriteV2Unmarshaler{}
	}
	return v.(*WriteV2Unmarshaler)
}

// PutWriteV2Unmarshaler returns wru to the pool.
//
// The caller mustn't access wru fields after returning wru to the pool.
func PutWriteV2Unmarshaler(wru *WriteV2Unmarshaler) {
	wru.Reset()
	wv2uPool.Put(wru)
}

var wv2uPool sync.Pool

// WriteV2Request is a lightweight representation of Prometheus remote write v2 Request.
// It covers the commonly used fields: Symbols and Timeseries.
type WriteV2Request struct {
	Symbols    []string
	Timeseries []WriteV2TimeSeries
}

// WriteV2TimeSeries represents a single series in write v2.
type WriteV2TimeSeries struct {
	LabelsRefs       []uint32
	Samples          []WriteV2Sample
	CreatedTimestamp int64
	// Metadata and other fields are available but optional for now.
	Metadata *WriteV2Metadata
}

// ToLabels return model labels.Labels from timeseries' remote labels.
func (m WriteV2TimeSeries) ToLabels(b *labels.ScratchBuilder, symbols []string) (labels.Labels, error) {
	return desymbolizeLabels(b, m.LabelsRefs, symbols)
}

// WriteV2Sample represents a sample in write v2.
type WriteV2Sample struct {
	Value     float64
	Timestamp int64
}

// WriteV2Metadata is a minimal representation of the v2 Metadata message.
type WriteV2Metadata struct {
	Type    uint32
	HelpRef uint32
	UnitRef uint32
}

// WriteV2Unmarshaler parses Protobuf-encoded write v2 Request messages.
// It reuses internal slices to avoid allocations between calls.
type WriteV2Unmarshaler struct {
	wr WriteV2Request

	// pools to reuse memory
	symbolsPool    []string
	timeseriesPool []WriteV2TimeSeries
	samplesPool    []WriteV2Sample
}

// Reset resets unmarshaler so it can be reused.
func (wru *WriteV2Unmarshaler) Reset() {
	wru.wr.Symbols = ResetStrings(wru.wr.Symbols)
	wru.wr.Timeseries = ResetWriteV2TimeSeries(wru.wr.Timeseries)

	// clear pools
	clear(wru.symbolsPool)
	wru.symbolsPool = wru.symbolsPool[:0]
	clear(wru.timeseriesPool)
	wru.timeseriesPool = wru.timeseriesPool[:0]
	clear(wru.samplesPool)
	wru.samplesPool = wru.samplesPool[:0]
}

// UnmarshalProtobuf parses src into an internal WriteV2Request and returns it.
// The returned pointer is valid until the next call to UnmarshalProtobuf on the same unmarshaler.
func (wru *WriteV2Unmarshaler) UnmarshalProtobuf(src []byte) (*WriteV2Request, error) {
	wru.Reset()

	var err error
	var fc easyproto.FieldContext

	symbols := wru.wr.Symbols
	ts := wru.wr.Timeseries
	symbolsPool := wru.symbolsPool
	timeseriesPool := wru.timeseriesPool
	samplesPool := wru.samplesPool

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return nil, fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 4: // symbols repeated string
			v, ok := fc.String()
			if !ok {
				return nil, fmt.Errorf("cannot read symbol string")
			}
			if len(symbols) < cap(symbols) {
				symbols = symbols[:len(symbols)+1]
			} else {
				symbols = append(symbols, "")
			}
			symbols[len(symbols)-1] = v
			// also keep in symbolsPool for reuse
			if len(symbolsPool) < cap(symbolsPool) {
				symbolsPool = symbolsPool[:len(symbolsPool)+1]
			} else {
				symbolsPool = append(symbolsPool, "")
			}
			symbolsPool[len(symbolsPool)-1] = v
		case 5: // timeseries repeated message
			data, ok := fc.MessageData()
			if !ok {
				return nil, fmt.Errorf("cannot read timeseries data")
			}
			// grow ts slice
			if len(ts) < cap(ts) {
				ts = ts[:len(ts)+1]
			} else {
				ts = append(ts, WriteV2TimeSeries{})
			}
			tsPtr := &ts[len(ts)-1]
			timeseriesPool, samplesPool, err = unmarshalWriteV2TimeSeries(data, timeseriesPool, samplesPool, tsPtr)
			if err != nil {
				return nil, fmt.Errorf("cannot unmarshal timeseries: %w", err)
			}
		}
	}

	wru.wr.Symbols = symbols
	wru.wr.Timeseries = ts
	wru.symbolsPool = symbolsPool
	wru.timeseriesPool = timeseriesPool
	wru.samplesPool = samplesPool
	return &wru.wr, nil
}

func unmarshalWriteV2TimeSeries(src []byte, tsPool []WriteV2TimeSeries, samplesPool []WriteV2Sample, out *WriteV2TimeSeries) ([]WriteV2TimeSeries, []WriteV2Sample, error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		var err error
		src, err = fc.NextField(src)
		if err != nil {
			return tsPool, samplesPool, fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1: // labels_refs packed uint32
			// read packed varints
			vals, ok := fc.UnpackFixed32s(make([]uint32, 16))
			if !ok {
				// if not packed, try single value
				v, ok2 := fc.Uint32()
				if !ok2 {
					return tsPool, samplesPool, fmt.Errorf("cannot read labels_refs")
				}
				out.LabelsRefs = append(out.LabelsRefs, v)
			} else {
				out.LabelsRefs = append(out.LabelsRefs, vals...)
			}
		case 2: // samples repeated message
			data, ok := fc.MessageData()
			if !ok {
				return tsPool, samplesPool, fmt.Errorf("cannot read sample data")
			}
			// append sample from pool
			if len(samplesPool) < cap(samplesPool) {
				samplesPool = samplesPool[:len(samplesPool)+1]
			} else {
				samplesPool = append(samplesPool, WriteV2Sample{})
			}
			s := &samplesPool[len(samplesPool)-1]
			if err := unmarshalWriteV2Sample(data, s); err != nil {
				return tsPool, samplesPool, fmt.Errorf("cannot unmarshal sample: %w", err)
			}
			out.Samples = append(out.Samples, *s)
		case 5: // metadata message
			data, ok := fc.MessageData()
			if !ok {
				return tsPool, samplesPool, fmt.Errorf("cannot read metadata data")
			}
			var mm WriteV2Metadata
			if err := unmarshalWriteV2Metadata(data, &mm); err != nil {
				return tsPool, samplesPool, fmt.Errorf("cannot unmarshal metadata: %w", err)
			}
			out.Metadata = &mm
		case 6: // created_timestamp
			v, ok := fc.Int64()
			if !ok {
				return tsPool, samplesPool, fmt.Errorf("cannot read created_timestamp")
			}
			out.CreatedTimestamp = v
		default:
			// ignore unsupported fields (histograms, exemplars, etc.)
		}
	}
	return tsPool, samplesPool, nil
}

func unmarshalWriteV2Sample(src []byte, s *WriteV2Sample) error {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		var err error
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			v, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read sample value")
			}
			s.Value = v
		case 2:
			v, ok := fc.Int64()
			if !ok {
				return fmt.Errorf("cannot read sample timestamp")
			}
			s.Timestamp = v
		}
	}
	return nil
}

func unmarshalWriteV2Metadata(src []byte, mm *WriteV2Metadata) error {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		var err error
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			v, ok := fc.Uint32()
			if !ok {
				return fmt.Errorf("cannot read metadata type")
			}
			mm.Type = v
		case 3:
			v, ok := fc.Uint32()
			if !ok {
				return fmt.Errorf("cannot read help_ref")
			}
			mm.HelpRef = v
		case 4:
			v, ok := fc.Uint32()
			if !ok {
				return fmt.Errorf("cannot read unit_ref")
			}
			mm.UnitRef = v
		}
	}
	return nil
}

// helper resetters
func ResetWriteV2TimeSeries(s []WriteV2TimeSeries) []WriteV2TimeSeries {
	for i := range s {
		s[i].LabelsRefs = ResetUint32s(s[i].LabelsRefs)
		s[i].Samples = ResetWriteV2Samples(s[i].Samples)
		s[i].Metadata = nil
		s[i].CreatedTimestamp = 0
	}
	return s[:0]
}

func ResetWriteV2Samples(s []WriteV2Sample) []WriteV2Sample {
	return s[:0]
}

// simple helpers for slices of primitive types used in multiple places
func ResetStrings(s []string) []string { return s[:0] }
func ResetUint32s(s []uint32) []uint32 { return s[:0] }

// small clear utility matching patterns used elsewhere
func clear[T any](s []T) {
	for i := range s {
		var zero T
		s[i] = zero
	}
}

// desymbolizeLabels decodes label references into model labels, with given symbols table.
func desymbolizeLabels(b *labels.ScratchBuilder, labelRefs []uint32, symbols []string) (labels.Labels, error) {
	b.Reset()
	for i := 0; i+1 < len(labelRefs); i += 2 {
		nameIdx := int(labelRefs[i])
		valIdx := int(labelRefs[i+1])
		if nameIdx < 0 || nameIdx >= len(symbols) || valIdx < 0 || valIdx >= len(symbols) {
			return labels.Labels{}, fmt.Errorf("invalid symbol reference: nameIdx=%d valIdx=%d symbolsLen=%d", nameIdx, valIdx, len(symbols))
		}
		b.Add(symbols[nameIdx], symbols[valIdx])
	}
	b.Sort()
	return b.Labels(), nil
}
