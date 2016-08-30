package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"unicode"
	"math"
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
	// 	When you are reading a file and the end-of-file is found, an error is returned.
	// 	To check for it use the following code:
	// 		if bytesRead, err = file.Read(buffer); err != nil {
	// 			if err == io.EOF {
	// 				// EOF error
	// 			} else {
	//				panic(err)
	//			}
	// 		}
	//
	// 	Use the mapFileName function generate the name of the files!

	var buffer []byte //stores the whole file
	buffer, err = ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatal(err)
	}

	fileSize := len(buffer)
	filenumb := 0 //index of the new file to be created
	initial_char_position:=0 //position of the first char to be written in a smaller file
	final_char_position:=0 //position of the last char to be written in a smaller file

	//runs along the buffer of the whole file and chunks it into smaller files wihout breakign words at the end
	for initial_char_position<fileSize {
		file, err := os.Create(mapFileName(filenumb))
		if err != nil {
			log.Fatal(err)
		}
		
		final_char_position = initial_char_position+chunkSize
		if final_char_position < fileSize{
			//if the proposed final char position is still inside the buffer, the loop below assures that no word will be broken
			for unicode.IsNumber(rune(buffer[final_char_position])) || unicode.IsLetter(rune(buffer[final_char_position])){
				final_char_position--
			}
			final_char_position = int(math.Min(float64(final_char_position), float64(initial_char_position+chunkSize-1)))
		} else {
			//if the remaining chars of the buffer arent enough to fill a file, the final char position must be the final position of the buffer
			final_char_position = fileSize-1
		}

		file.Write(buffer[initial_char_position:final_char_position])

		file.Close()
		initial_char_position=final_char_position+1
		filenumb++
	}

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////

	return filenumb, err
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
