/*
Package csv contains collection of scripts to process CSV data.
*/
package csv

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"sync"
)

// ProcessCSVFileByRow is a wrapper of processCSVByRow that reads CSV file and output the processed CSV as a file.
func ProcessCSVFileByRow(inputCSV string, outputCSV string, rowProcessor func([]string) []string, skipHeader bool) {
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
	ProcessCSVByRow(inFile, outFile, rowProcessor, skipHeader)
}

// ProcessCSVFileByRowParallel is like processCSVFileByRow but in parallel.
// This will not maintain CSV row ordering.
// Please handle panic in the function accordingly.
func ProcessCSVFileByRowParallel(inputCSV string, outputCSV string, rowProcessor func([]string) []string, skipHeader bool) {
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
	ProcessCSVByRowParallel(inFile, outFile, rowProcessor, skipHeader)
}

// ProcessCSVByRow reads csv row line by line, then do rowProcessor() on each row and output a new row.
func ProcessCSVByRow(in io.Reader, out io.Writer, rowProcessor func([]string) []string, skipHeader bool) {
	// Read the input and output file as CSV
	inCSVReader := csv.NewReader(in)
	outCSVReader := csv.NewWriter(out)

	if skipHeader {
		inCSVReader.Read()
	}

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
		if result == nil {
			continue
		}
		err = outCSVReader.Write(result)
		if err != nil {
			log.Printf("Cannot write row at line %v.\n", lineCount)
			continue
		}
	}
	outCSVReader.Flush()
}

// ProcessCSVByRowParallel process the CSV row by row in parallel.
// This is much faster than its processCSVByRow but doesn't maintain row ordering.
func ProcessCSVByRowParallel(in io.Reader, out io.Writer, rowProcessor func([]string) []string, skipHeader bool) {
	// Read the input and output file as CSV
	inCSVReader := csv.NewReader(in)
	outCSVWriter := csv.NewWriter(out)

	workerPool := NewRowWorkerPool(rowProcessor)

	// Create the writer goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	// Writer
	go func() {
		for processed := range workerPool.Consume() {
			if processed != nil {
				outCSVWriter.Write(processed)
			}
		}
		outCSVWriter.Flush()
		wg.Done()
	}()

	if skipHeader {
		inCSVReader.Read()
	}

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
