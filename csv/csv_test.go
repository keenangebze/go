package csv

import (
	"bytes"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestProcessCSVByRowParallel asserts the number of lines in the input and output are the same.
func TestProcessCSVByRowParallel(t *testing.T) {
	goroutines := uint8(8)
	csvText := `
a,b,c
100,32,-3
10,3,4
25,10,1
`
	csvStream := strings.NewReader(csvText)
	csvOutStream := new(bytes.Buffer)

	// Test the count of the rows, making sure that all rows are processed and the result returned.
	actualCount := 0
	expectedCount := 0

	// Simple use case to add the value of column a, b, and c into new column.
	ProcessCSVByRowParallel(csvStream, csvOutStream, func(row []string) []string {
		a, err := strconv.Atoi(row[0])
		b, err := strconv.Atoi(row[1])
		c, err := strconv.Atoi(row[2])
		if err != nil {
			t.Fatalf("Cannot convert input strings to number.")
		}
		actualCount++
		return append(row, strconv.Itoa(a+b+c))
	}, goroutines, true)

	// Read to the output to assert it.
	ProcessCSVByRowParallel(csvOutStream, os.Stdout, func(row []string) []string {
		expectedCount++
		return nil
	}, goroutines, false)

	// Compare the result.
	if expectedCount != actualCount {
		t.Fatalf("Expected and actual processed rows differ, expected %v returned %v.\n", expectedCount, actualCount)
	}
}
