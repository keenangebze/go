package csv

import (
	"errors"
	"sync"
)

// RowWorkerPool will manages a pool of Goroutines to process the CSV row in parallel.
// It is heavily inspired by Jason Waldrip's code from in the book "Go in Action" (2015)
// by William Kenedy with Brian Ketelsen and Erik St. Martin.
// https://learning.oreilly.com/library/view/go-in-action/9781617291784/kindle_split_015.html
type RowWorkerPool struct {
	process   func(row []string) []string
	inStream  chan []string
	outStream chan []string
	wg        sync.WaitGroup
	closed    bool
}

// ErrWorkerClosed will be returned if you feed a closed pool.
var ErrWorkerClosed = errors.New("worker pool already closed")

// NewRowWorkerPool instantiate the worker pool.
func NewRowWorkerPool(rowProcessor func(row []string) []string, poolSize uint8) *RowWorkerPool {
	pool := RowWorkerPool{
		process:   rowProcessor,
		inStream:  make(chan []string),
		outStream: make(chan []string),
	}
	pool.wg.Add(int(poolSize))
	for i := 0; i < int(poolSize); i++ {
		go func() {
			for task := range pool.inStream {
				pool.outStream <- pool.process(task)
			}
			pool.wg.Done()
		}()
	}
	return &pool
}

// Feed feeds the worker pool with CSV's row.
func (rw *RowWorkerPool) Feed(row []string) error {
	if rw.closed {
		return ErrWorkerClosed
	}
	rw.inStream <- row
	return nil
}

// Consume consumes the processed result.
func (rw *RowWorkerPool) Consume() <-chan []string {
	return rw.outStream
}

// Close closes the worker pool.
func (rw *RowWorkerPool) Close() {
	close(rw.inStream)
	rw.wg.Wait()
	close(rw.outStream)
}
