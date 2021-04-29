package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
)

var fileFlag = flag.String("file", "file.tsv", "the input tsv file in KGX format")
var collectionNameFlag = flag.String("collection", "nodes", "the collection the _from and _to nodes in the edges are in")
var threadsFlag = flag.Int("threads", 1, "number of threads to use for conversion")
var outputFlag = flag.String("output", "output.tsv", "the file to place the converted data in")

var objectIndex, subjectIndex int

var wg sync.WaitGroup

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
	output, err := os.Create("headers.tsv")
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

	// Find the index for the 'subject' field (_from)
	subjectIndex = find(header, "subject")

	// Rename the 'subject' to '_from'
	header[subjectIndex] = "_from"

	// Find the index for the 'object' field (_to)
	objectIndex = find(header, "object")

	// Rename 'object' to '_to'
	header[objectIndex] = "_to"

	// Write new header to output
	output.WriteString(strings.Join(header, "\t") + "\n")

	// Create buckets for the routines to use
	lines, err := lineCount(tsv)
	if err != nil {
		log.Fatal(err)
	}
	buckets := createBuckets(lines, *threadsFlag)

	wg = sync.WaitGroup{}

	// Start up a routine for every bucket
	for t, b := range buckets {
		wg.Add(1)
		go transformSegment(t, b.min, b.max)
	}

	wg.Wait()
}

func transformSegment(thread int, start int, end int) {
	// Load in the tsv file
	tsv, err := os.Open(*fileFlag)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer tsv.Close()

	// Create a new file for the output
	output, err := os.Create(fmt.Sprint(thread) + ".tsv")
	defer output.Close()

	// Create a new scanner
	scanner := bufio.NewScanner(tsv)

	// Move the scanner to the startpoint
	for i := 0; i < start+1; i++ {
		scanner.Scan()
	}

	// Variable to store the line elements in
	var elems []string
	var line string
	for i := start; i <= end; i++ {
		// Read the line
		scanner.Scan()
		line = scanner.Text()

		// Split on tabs
		elems = strings.Split(line, "\t")

		// Modify the _from and _to fields to include the collection name
		elems[subjectIndex] = *collectionNameFlag + "/" + elems[subjectIndex]
		elems[objectIndex] = *collectionNameFlag + "/" + elems[objectIndex]

		output.WriteString(strings.Join(elems, "\t") + "\n")
	}

	// Signal that we are done
	wg.Done()
}

type bucket struct {
	min int
	max int
}

func createBuckets(lineCount int, threads int) []bucket {
	res := make([]bucket, 0, threads)
	bucketSize := int(math.Floor(float64(lineCount) / float64(threads)))
	bucket := bucket{}

	for i := 0; i < lineCount; i += bucketSize + 1 {
		if i+bucketSize > lineCount {
			bucket.max = lineCount
		} else {
			bucket.max = i + bucketSize
		}
		bucket.min = i
		res = append(res, bucket)
	}

	return res
}

func lineCount(r io.Reader) (int, error) {
	var count int
	const lineBreak = '\n'

	buf := make([]byte, bufio.MaxScanTokenSize)

	for {
		bufferSize, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}

		var buffPosition int
		for {
			i := bytes.IndexByte(buf[buffPosition:], lineBreak)
			if i == -1 || bufferSize == buffPosition {
				break
			}
			buffPosition += i + 1
			count++
		}
		if err == io.EOF {
			break
		}
	}

	return count, nil
}

// find returns the smallest index i at which x == a[i],
// or len(a) if there is no such index.
func find(a []string, x string) int {
	for i, n := range a {
		if x == n {
			return i
		}
	}
	return len(a)
}
