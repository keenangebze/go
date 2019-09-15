package jsonl2csv_test

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	
	"github.com/keenangebze/go/util/jsonl2csv"
)

// Simple use case
func ExampleJsonl2Csv() {

	// Emulate a JSONL input stream.
	jsonlStream := strings.NewReader(`{"title": "Akka in Action", "authors": "Raymond R., Rob B., Rob W.", "publisher": "Manning Publications", "year": 2016}
{"title": "D3 for the Impatient", "authors": "Phillip K. J.", "publisher": "O'Reilly Media Inc.", "year": 2019}
{"title": "Machine Learning Systems: Design that scale", "authors": "Jeff Smith", "publisher": "Manning Publications", "year": 2018}
{"title": "Designing Data-Intensive Application", "authors": "Martin Kleppmann", "publisher": "O'Reilly Media Inc.", "year": 2017}`)

	type Book struct {
		Title     string      `json:"title"`
		Authors   string      `json:"authors"`
		Publisher string      `json:"publisher"`
		Year      json.Number `json:"year"`
	}

	toBookCsv := func(jsonByte []byte) ([]string, error) {
		book := Book{}
		err := json.Unmarshal(jsonByte, &book)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		result := []string{book.Title, book.Authors, book.Publisher, string(book.Year)}
		return result, nil
	}

	jsonl2csv.Jsonl2Csv(jsonlStream, os.Stdout, toBookCsv)

	// Unordered output:
	//Akka in Action,"Raymond R., Rob B., Rob W.",Manning Publications,2016
	//D3 for the Impatient,Phillip K. J.,O'Reilly Media Inc.,2019
	//Machine Learning Systems: Design that scale,Jeff Smith,Manning Publications,2018
	//Designing Data-Intensive Application,Martin Kleppmann,O'Reilly Media Inc.,2017
}
