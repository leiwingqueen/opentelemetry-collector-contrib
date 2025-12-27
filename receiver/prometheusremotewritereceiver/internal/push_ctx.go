package internal

import (
	"sync"

	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

// PushCtx is a context used for populating WriteRequest.
type PushCtx struct {
	// WriteRequest contains the WriteRequest, which must be pushed later to remote storage.
	//
	// The actual labels and samples for the time series are stored in Labels and Samples fields.
	WriteRequest writev2.Request
}

// Reset resets ctx.
func (ctx *PushCtx) Reset() {
	ctx.WriteRequest.Reset()
}

// GetPushCtx returns PushCtx from pool.
//
// Call PutPushCtx when the ctx is no longer needed.
func GetPushCtx() *PushCtx {
	if v := pushCtxPool.Get(); v != nil {
		return v.(*PushCtx)
	}
	return &PushCtx{}
}

// PutPushCtx returns ctx to the pool.
//
// ctx mustn't be used after returning to the pool.
func PutPushCtx(ctx *PushCtx) {
	ctx.Reset()
	pushCtxPool.Put(ctx)
}

var pushCtxPool sync.Pool
