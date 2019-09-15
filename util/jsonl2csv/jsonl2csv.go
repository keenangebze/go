package jsonl2csv

import (
	"bufio"
	"encoding/csv"
	"errors"
	"io"
	"sync"
)

// NumOfWorker is the number of goroutine used to process the stream
var NumOfWorker = 10

// ErrInvalidNumOfWorker thrown if NumOfWorker < 1
var ErrInvalidNumOfWorker = errors.New("Invalid number of workers. Must be greater than 0.")

// Jsonl2Csv Converts JSONL stream in to CSV stream
func Jsonl2Csv(in io.Reader, out io.Writer, transform func(in []byte) ([]string, error)) error {
	if NumOfWorker <= 1 {
		return ErrInvalidNumOfWorker
	}
	jsonStream := make(chan []byte)
	csvStream := make(chan []string)
	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}
	wg3 := sync.WaitGroup{}
	// input stream
	wg1.Add(1)
	go func() {
		lineScanner := bufio.NewScanner(in)
		lineScanner.Split(bufio.ScanLines)
		for lineScanner.Scan() {
			jsonStream <- lineScanner.Bytes()
		}
		wg1.Done()
	}()

	// jsonl2csv converter
	for i := 0; i < NumOfWorker; i++ {
		wg2.Add(1)
		go func() {
			for m := range jsonStream {
				csvData, err := transform(m)
				if err != nil {
					continue
				}
				csvStream <- csvData
			}
			wg2.Done()
		}()
	}

	// output stream
	wg3.Add(1)
	go func() {
		out := csv.NewWriter(out)
		for m := range csvStream {
			out.Write(m)
			out.Flush()
		}
		wg3.Done()
	}()

	wg1.Wait()
	close(jsonStream)
	wg2.Wait()
	close(csvStream)
	wg3.Wait()

	return nil
}
