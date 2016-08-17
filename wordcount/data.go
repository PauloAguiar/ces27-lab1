package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/lab1-ces27/mapreduce"
	"io/ioutil"
    "io"
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

// fanInData will run a goroutine that reads files created by splitData and share them with
// the mapreduce framework through the one-way channel. It'll buffer data up to
// MAP_BUFFER_SIZE (files smaller than chunkSize) and resume loading them
// after they are read on the other side of the channle (in the mapreduce package)
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
    
    // Opening the file
    file, err := os.Open(fileName)
    if err != nil {
        log.Fatal(err)
    }

    // My Variables
    numMapFiles = 0
    chunkSize = chunkSize - 10
    readBuffer := make([]byte, chunkSize)
    afterBuffer := make([]byte, chunkSize)
    bytesToWrite := 0

    // Reading the file little by little and then storing them
    for {
        // Reading
        bytesRead, err := file.Read(readBuffer)
        
        // If some error occurs
        if err != nil && err != io.EOF{
            panic(err)
        }
        if bytesRead == 0 {
            break
        }

        // Creating an output file
        out, err := os.Create(mapFileName(numMapFiles))
        if err != nil {
            panic(err)
        }
        numMapFiles = numMapFiles + 1
        
        // Writing
        // Stop breaking words!
        by := readBuffer[bytesRead-1]
        newBytesToWrite := 0
        for unicode.IsLetter(rune(by)) || unicode.IsNumber(rune(by)) {
            bytesRead = bytesRead - 1
            newBytesToWrite = newBytesToWrite + 1
            by = readBuffer[bytesRead-1]
        }
        
        if _, err := out.Write(afterBuffer[:bytesToWrite]); err != nil {
            panic(err)
        }
        if _, err := out.Write(readBuffer[:bytesRead]); err != nil {
            panic(err)
        }
        
        for idx := 0; idx<newBytesToWrite; idx++ {
            afterBuffer[idx] = readBuffer[bytesRead+idx]
        }

        bytesToWrite = newBytesToWrite

        // Closing output file
        out.Close()
    }

    //Closing the file
    file.Close()

	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
