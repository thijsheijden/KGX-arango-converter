package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

var fileFlag = flag.String("file", "file.tsv", "the input tsv file in KGX format")
var collectionNameFlag = flag.String("collection", "nodes", "the collection the _from and _to nodes in the edges are in")
var threadsFlag = flag.Int("threads", 1, "number of threads to use for conversion")
var outputFlag = flag.String("output", "output.tsv", "the file to place the converted data in")

func main() {
	// Get the filepath for the file we want to translate
	flag.Parse()

	// Load in the tsv file
	tsv, err := os.Open(*fileFlag)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer tsv.Close()

	// Create a new file for the output
	output, err := os.Create(*outputFlag)
	defer output.Close()

	// Create a scanner for the input file
	scn := bufio.NewScanner(tsv)

	// Variable to store the line in
	var line string

	// Read the first line of the file (header)
	scn.Scan()
	line = scn.Text()

	// Split on tabs
	header := strings.Split(line, "\t")
	log.Println(len(header))

	// Find the index for the 'subject' field (_from)
	subjectIndex := Find(header, "subject")

	// Rename the 'subject' to '_from'
	header[subjectIndex] = "_from"

	// Find the index for the 'object' field (_to)
	objectIndex := Find(header, "object")

	// Rename 'object' to '_to'
	header[objectIndex] = "_to"

	// Write new header to output
	output.WriteString(strings.Join(header, "\t") + "\n")

	// Variable to store the line elements in
	var elems []string
	for scn.Scan() {
		// Read the line
		line = scn.Text()

		// Split on tabs
		elems = strings.Split(line, "\t")

		// Modify the _from and _to fields to include the collection name
		elems[subjectIndex] = *collectionNameFlag + "/" + elems[subjectIndex]
		elems[objectIndex] = *collectionNameFlag + "/" + elems[objectIndex]

		output.WriteString(strings.Join(elems, "\t") + "\n")
	}
}

// Find returns the smallest index i at which x == a[i],
// or len(a) if there is no such index.
func Find(a []string, x string) int {
	for i, n := range a {
		if x == n {
			return i
		}
	}
	return len(a)
}
