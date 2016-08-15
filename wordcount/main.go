package main

import (
	"flag"
	"github.com/pauloaguiar/lab1-ces27/mapreduce"
	"log"
	"os"
)

var (
	mode       = flag.String("mode", "local", "Run mode: local(sequential) or distributed")
	file       = flag.String("file", "", "File to use as input")
	reduceJobs = flag.Int("reducejobs", 1, "Number of reduce jobs that should be run")
	chunkSize  = flag.Int("chunksize", 100*1024, "Size of data chunks that should be passed to map jobs(in bytes)")
)

// Entry point
func main() {
	var (
		err       error
		task      *mapreduce.Task
		fanIn     <-chan []byte
		fanOut    chan<- []mapreduce.KeyValue
		numFiles  int
		waitForIt chan bool
	)

	flag.Parse()

	log.Println("Running in", *mode, "mode.")
	log.Println("File:", *file)
	log.Println("Reduce Jobs:", *reduceJobs)
	log.Println("Chunk Size:", *chunkSize)

	_ = os.Mkdir(MAP_PATH, os.ModeDir)
	_ = os.Mkdir(RESULT_PATH, os.ModeDir)

	// Splits data into chunks with size up to chunkSize
	if numFiles, err = splitData(*file, *chunkSize); err != nil {
		log.Fatal(err)
	}

	// Create fan in and out channels for mapreduce.Task
	fanIn = fanInData(numFiles)
	fanOut, waitForIt = fanOutData()

	// Initialize mapreduce.Task object with the channels created above and functions
	// mapFunc, shufflerFunc and reduceFunc defined in wordcount.go
	task = &mapreduce.Task{
		mapFunc,
		shuffleFunc,
		reduceFunc,
		*reduceJobs,
		fanIn,
		fanOut,
	}

	// Run task based on parameters
	switch *mode {
	case "local":
		mapreduce.RunSequential(task)

		// Wait for fanOut to finish writing data to storage.
		// Legen..
		<-waitForIt
		// ..dary!
	case "distributed":
		log.Println("Distributed mode not available.")
	}
}
