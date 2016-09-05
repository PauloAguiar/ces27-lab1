package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"io"
	"log"
	"os"
	"path/filepath"
	"unicode"
	"errors"
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
// after they are read on the other side of the channle (in the mapreduce package)
func fanInData(numFiles int) chan []byte {
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
func fanOutData() (output chan []mapreduce.KeyValue, done chan bool) {
	var (
		err           error
		file          *os.File
		fileEncoder   *json.Encoder
		reduceCounter int
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
	// 	When you are reading a file and the end-of-file is found, an error is returned.
	// 	To check for it use the following code:
	// 		if bytesRead, err = file.Read(buffer); err != nil {
	// 			if err == io.EOF {
	// 				// EOF error
	// 			} else {
	//				return 0, err
	//			}
	// 		}
	//
	// 	Use the mapFileName function to generate the name of the files!
	//
	//	Go strings are encoded using UTF-8.
	// 	This means that a character can't be handled as a byte. There's no char type.
	// 	The type that hold's a character is called a 'rune' and it can have 1-4 bytes (UTF-8).
	//	Because of that, it's not possible to index access characters in a string the way it's done in C.
	//		str[3] will return the 3rd byte, not the 3rd rune in str, and this byte may not even be a valid
	//		rune by itself.
	//		Rune handling should be done using the package 'strings' (https://golang.org/pkg/strings/)
	//
	//  For more information visit: https://blog.golang.org/strings
	//
	//	It's also important to notice that errors can be handled here or they can be passed down
	// 	to be handled by the caller as the second parameter of the return.

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////

	// This function DOES NOT support Unicode characters in the input file, only ASCII
	// If the input file contains Unicode characters, they may be broken and become invalid

	// Used this as a reference for file handling: http://www.devdungeon.com/content/working-files-go

	var (
		readError error
		cutOffPosition int
	)

	numMapFiles = 0

	file, err := os.Open(fileName)
	if err != nil {
    	log.Fatal(err)
    	return 0, err
	}

	// This buffer stores the bytes that will be written to map files
    bufferToMapFiles := make([]byte, 0)
    bytesInBuffer := 0

    // Keep reading while end-of-file is not found
    for readError != io.EOF {

    	// Read from file the amount of bytes necessary to complete the buffer
    	// The buffer contains (chunkSize + 1) bytes, so that if the last byte is a separator, 
    	// then the whole chunk can be written to file
    	// In other words, the last byte works as a "lookahead byte"
        bytesFromFile := make([]byte, chunkSize + 1 - bytesInBuffer)
        _, readError = file.Read(bytesFromFile)
        if readError != nil && readError != io.EOF {
        	return numMapFiles, readError
        }

        // Append bytes read from file to buffer
        bufferToMapFiles = append(bufferToMapFiles, bytesFromFile...)

        // Look for a separator at the end of the buffer
        // When a separator is found, all the bytes up to it can be written to a file, 
        // ensuring that the file doesn't contain broken words
        cutOffPosition = chunkSize
        for cutOffPosition > 0 && 
        	(unicode.IsLetter(rune(bufferToMapFiles[cutOffPosition])) || 
			 unicode.IsNumber(rune(bufferToMapFiles[cutOffPosition]))) {

            cutOffPosition--
        }

        // Edge case: If cutoff position reaches zero, then there is a word larger than chunkSize, 
        // therefore it is impossible to properly split the input file
        if cutOffPosition == 0 {
            err := errors.New("Error: word larger than chunkSize")
            log.Fatal(err)
            return numMapFiles, err
        }

        // If buffer is not empty (i.e. it doesn't contain only zero bytes), then write its content up to cutoff position
        // It suffices to check only the first byte in the buffer
        // This check prevents an additional empty file to be created
        if bufferToMapFiles[0] != 0 {
            
            // Prevent trailing zero bytes to be unnecessarily written to file
            for bufferToMapFiles[cutOffPosition-1] == 0 {
                cutOffPosition--
            }

            // Write buffer content up to cutoff position in mode 0666 (read/write)
            if err := ioutil.WriteFile(mapFileName(numMapFiles), bufferToMapFiles[:cutOffPosition], 0666); err != nil {
                log.Fatal(err)
                return numMapFiles, err
            } 

            // Keeps in buffer whatever couldn't be written to file (e.g. a piece of a broken word or a "lookahead byte")
            bufferToMapFiles = bufferToMapFiles[cutOffPosition:]
            bytesInBuffer = len(bufferToMapFiles)

            numMapFiles++
        }
    }

    file.Close()

    // If this point was reached, then no errors occurred when handling files
	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
