package prometheusremotewritereceiver

import (
	"bytes"
	"io"
	"net/http"
	"sync"

	"github.com/gogo/protobuf/proto"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

type pooledParser struct {
	bufferPool  *sync.Pool
	requestPool *sync.Pool
	maxSize     int64
}

func newPooledParser(maxSize int64) *pooledParser {
	return &pooledParser{
		bufferPool: &sync.Pool{
			New: func() interface{} {
				// 预分配 1MB buffer
				return bytes.NewBuffer(make([]byte, 0, 1024*1024))
			},
		},
		requestPool: &sync.Pool{
			New: func() interface{} {
				return &writev2.Request{}
			},
		},
		maxSize: maxSize,
	}
}

func (pp *pooledParser) parseRequest(r *http.Request) (*writev2.Request, error) {
	// 1. 从池中获取 buffer
	buf := pp.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer pp.bufferPool.Put(buf)

	// 2. 限制读取大小
	limitedReader := io.LimitReader(r.Body, pp.maxSize)

	// 3. 复用 buffer 读取
	if _, err := buf.ReadFrom(limitedReader); err != nil {
		return nil, err
	}

	// 4. 从池中获取 Request 对象
	req := pp.requestPool.Get().(*writev2.Request)
	req.Reset() // 重置状态

	// 5. Unmarshal
	if err := proto.Unmarshal(buf.Bytes(), req); err != nil {
		pp.requestPool.Put(req) // 出错也要归还
		return nil, err
	}

	return req, nil
}

func (pp *pooledParser) returnRequest(req *writev2.Request) {
	// 如果对象太大，不要放回池中
	if len(req.Timeseries) > 1000 || len(req.Symbols) > 10000 {
		return
	}
	pp.requestPool.Put(req)
}
