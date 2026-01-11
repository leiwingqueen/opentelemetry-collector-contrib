package prometheusremotewritereceiver

import (
	"bytes"
	"io"
	"sync"

	"github.com/gogo/protobuf/proto"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

type prwParser struct {
	bodyBufferPool     *sync.Pool
	writeRequestPool   *sync.Pool
	maxRequestBodySize int64
}

func newPrwParser(maxRequestBodySize int64) *prwParser {
	if maxRequestBodySize <= 0 {

	}
	return &prwParser{
		bodyBufferPool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate 4KiB
				return bytes.NewBuffer(make([]byte, 0, 4*1024))
			},
		},
		writeRequestPool: &sync.Pool{
			New: func() interface{} {
				return &writev2.Request{}
			},
		},
		maxRequestBodySize: maxRequestBodySize,
	}
}

func (pp *prwParser) Parse(r io.Reader, callback func(tss *writev2.Request) error) error {
	buf := pp.bodyBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer pp.bodyBufferPool.Put(buf)
	limitedReader := io.LimitReader(r, pp.maxRequestBodySize)
	if _, err := buf.ReadFrom(limitedReader); err != nil {
		return err
	}
	req := pp.writeRequestPool.Get().(*writev2.Request)
	req.Reset()
	defer pp.returnRequest(req)
	if err := proto.Unmarshal(buf.Bytes(), req); err != nil {
		return err
	}
	if err := callback(req); err != nil {
		return err
	}
	return nil
}

func (pp *prwParser) returnRequest(req *writev2.Request) {
	// 如果对象太大，不要放回池中
	if len(req.Timeseries) > 1000 || len(req.Symbols) > 10000 {
		return
	}
	pp.writeRequestPool.Put(req)
}
