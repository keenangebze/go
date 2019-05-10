/*
Package csv contains collection of scripts to process CSV data.
*/
package csv

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"os"
	"sync"
)

// processCSVFileByRow is a wrapper of processCSVByRow that reads CSV file and output the processed CSV as a file.
func processCSVFileByRow(inputCSV string, outputCSV string, rowProcessor func([]string) []string) {
	// Open the input and output file
	inFile, err := os.Open(inputCSV)
	if err != nil {
		log.Fatalf("Cannot open file %v. Aborting.\n", inputCSV)
	}
	defer inFile.Close()

	outFile, err := os.Create(outputCSV)
	if err != nil {
		log.Fatalf("Cannot open file %v. Aborting.\n", outputCSV)
	}
	defer outFile.Close()
	processCSVByRow(inFile, outFile, rowProcessor)
}

// processCSVFileByRowParallel is like processCSVFileByRow but in parallel.
// This will not maintain CSV row ordering.
func processCSVFileByRowParallel(inputCSV string, outputCSV string, rowProcessor func([]string) []string, numberOfGoroutines uint8) {
	// Open the input and output file
	inFile, err := os.Open(inputCSV)
	if err != nil {
		log.Fatalf("Cannot open file %v. Aborting.\n", inputCSV)
	}
	defer inFile.Close()

	outFile, err := os.Create(outputCSV)
	if err != nil {
		log.Fatalf("Cannot open file %v. Aborting.\n", outputCSV)
	}
	defer outFile.Close()
	processCSVByRowParallel(inFile, outFile, rowProcessor, numberOfGoroutines)
}

// processCSVByRow reads csv row line by line, then do rowProcessor() on each row and output a new row.
func processCSVByRow(in io.Reader, out io.Writer, rowProcessor func([]string) []string) {
	// Read the input and output file as CSV
	inCSVReader := csv.NewReader(in)
	outCSVReader := csv.NewWriter(out)

	lineCount := 0
	for {
		row, err := inCSVReader.Read()
		if err == io.EOF {
			break
		}
		lineCount++
		if err != nil {
			log.Printf("Cannot read row at line %v.\n", lineCount)
			continue
		}

		result := rowProcessor(row)
		err = outCSVReader.Write(result)
		if err != nil {
			log.Printf("Cannot write row at line %v.\n", lineCount)
			continue
		}
	}
}

// processCSVByRowParallel process the CSV row by row in parallel.
// This is much faster than its processCSVByRow but doesn't maintain row ordering.
func processCSVByRowParallel(in io.Reader, out io.Writer, rowProcessor func([]string) []string, poolSize uint8) {
	// Read the input and output file as CSV
	inCSVReader := csv.NewReader(in)
	outCSVWriter := csv.NewWriter(out)

	workerPool := NewRowWorkerPool(rowProcessor, poolSize)

	// Create the writer goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	// Writer
	go func() {
		for processed := range workerPool.Consume() {
			outCSVWriter.Write(processed)
		}
		outCSVWriter.Flush()
		wg.Done()
	}()

	// Feed the CSV row to the Row Worker pool
	lineCount := 0
	for {
		row, err := inCSVReader.Read()
		if err == io.EOF {
			break
		}
		lineCount++
		if err != nil {
			log.Printf("Cannot read row at line %v.\n", lineCount)
			continue
		}
		// protect against shared row reference
		copiedRow := make([]string, len(row), len(row))
		copy(copiedRow, row)

		// feed in to the worker pool
		workerPool.Feed(copiedRow)
	}
	workerPool.Close()
	wg.Wait()
}

// The code below is an implementation of Worker Pool to process CSV rows in parallel.
// It is heavily inspired by Jason Waldrip's code from in the book "Go in Action" (2015)
// by William Kenedy with Brian Ketelsen and Erik St. Martin.
// https://learning.oreilly.com/library/view/go-in-action/9781617291784/kindle_split_015.html

// RowWorkerPool will contain a pool of Goroutines to process the CSV row in parallel.
type RowWorkerPool struct {
	process   func(row []string) []string
	inStream  chan []string
	outStream chan []string
	wg        sync.WaitGroup
	closed    bool
}

// ErrWorkerClosed will be returned if you feed a closed pool
var ErrWorkerClosed = errors.New("worker pool already closed")

// NewRowWorkerPool instantiate the worker pool
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

// Feed feeds the worker pool with CSV's row
func (rw *RowWorkerPool) Feed(row []string) error {
	if rw.closed {
		return ErrWorkerClosed
	}
	rw.inStream <- row
	return nil
}

// Consume consumes the processed result
func (rw *RowWorkerPool) Consume() <-chan []string {
	return rw.outStream
}

// Close closes the worker pool.
func (rw *RowWorkerPool) Close() {
	close(rw.inStream)
	rw.wg.Wait()
	close(rw.outStream)
}
