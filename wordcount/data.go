package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"unicode"
)

const (
	MAP_PATH           = "map/"
	RESULT_PATH        = "result/"
	MAP_BUFFER_SIZE    = 10
	REDUCE_BUFFER_SIZE = 10
)

// fanInData will run a goroutine that reads files crated by splitData and share them with
// the mapreduce framework through the one-way channel. It'll buffer data up to
// MAP_BUFFER_SIZE (files smaller than chunkSize) and resume loading them
// after they are read on the other side of the channel (in the mapreduce package)
func fanInData(numFiles int) <-chan []byte {
	var (
		err    error
		input  chan []byte
		buffer []byte
	)

	input = make(chan []byte, MAP_BUFFER_SIZE)

	go func() {
		for i := 0; i < numFiles; i++ {
			if buffer, err = ioutil.ReadFile(mapFileName(i)); err != nil {
				close(input)
				log.Fatal(err)
			}

			log.Println("Fanning in file", mapFileName(i))
			input <- buffer
		}
		close(input)
	}()
	return input
}

// fanOutData will run a goroutine that receive data on the one-way channel and will
// proceed to store it in their final destination. The data will come out after the
// reduce phase of the mapreduce model.
func fanOutData() (chan<- []mapreduce.KeyValue, chan bool) {
	var (
		err           error
		file          *os.File
		fileEncoder   *json.Encoder
		reduceCounter int
		output        chan []mapreduce.KeyValue
		done          chan bool
	)

	output = make(chan []mapreduce.KeyValue, REDUCE_BUFFER_SIZE)
	done = make(chan bool)

	go func() {
		for v := range output {
			log.Println("Fanning out file", resultFileName(reduceCounter))
			if file, err = os.Create(resultFileName(reduceCounter)); err != nil {
				log.Fatal(err)
			}

			fileEncoder = json.NewEncoder(file)

			for _, value := range v {
				fileEncoder.Encode(value)
			}

			file.Close()
			reduceCounter++
		}

		done <- true
	}()

	return output, done
}

// Reads input file and split it into files smaller than chunkSize.
// CUTCUTCUTCUTCUT!
func splitData(fileName string, chunkSize int) (numMapFiles int, err error) {
	var (
		file           *os.File
		buffer         []byte
		bytesRead      int
		totalBytesRead int64
		lastByte       byte
		foundEOF       bool
	)

	if file, err = os.Open(fileName); err != nil {
		panic(err)
	}

	buffer = make([]byte, chunkSize)

	numMapFiles = 0
	totalBytesRead = 0
	foundEOF = false

	for !foundEOF {
		if bytesRead, err = file.Read(buffer); err != nil {
			if err == io.EOF {
				foundEOF = true
			} else {
				panic(err)
			}
		}

		if bytesRead > 0 {
			lastByte = buffer[bytesRead-1]
			for unicode.IsLetter(rune(lastByte)) || unicode.IsNumber(rune(lastByte)) {
				bytesRead -= 1
				lastByte = buffer[bytesRead-1]
			}

			buffer = buffer[:bytesRead]
			if err = ioutil.WriteFile(mapFileName(numMapFiles), buffer, 0644); err != nil {
				panic(err)
			}

			numMapFiles++
			totalBytesRead += int64(bytesRead)

			if _, err = file.Seek(totalBytesRead, 0); err != nil {
				panic(err)
			}
		}
	}

	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
