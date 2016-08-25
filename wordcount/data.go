package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"unicode"

	"github.com/pauloaguiar/ces27-lab1/mapreduce"
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
	//				panic(err)
	//			}
	// 		}
	//
	// 	Use the mapFileName function to generate the name of the files!

	// Tentativa infeliz de particionar arquivos tomando cuidado para
	// não cortar as palavras ao meio
	var file, erro = os.Open(fileName)

	if erro != nil {
		file.Close()
		return 0, err
	}
	defer file.Close()

	numMapFiles = 0

	fileInfo, _ := file.Stat()
	var remainingSize = fileInfo.Size()

	for remainingSize > int64(chunkSize) {
		fmt.Printf("> %d (%d)\n", remainingSize, chunkSize)

		var decCarac = 0
		var carac = '0'
		for unicode.IsLetter(carac) || unicode.IsNumber(carac) {
			buf := make([]byte, 1)
			var Ncarac, _ = file.ReadAt(buf, remainingSize-int64(decCarac))
			carac = rune(Ncarac)
			fmt.Printf("[%c]", carac)
			decCarac++
		}

		partSize := chunkSize - decCarac
		remainingSize -= int64(partSize)

		fmt.Printf("> Part %d of size %d\n", numMapFiles, partSize)
		partBuffer := make([]byte, partSize)

		partFileName := mapFileName(numMapFiles)
		fmt.Print(partFileName)
		partFile, _ := os.Create(partFileName)
		ioutil.WriteFile(partFileName, partBuffer, os.ModeAppend)
		partFile.Close()
		numMapFiles++
	}

	fmt.Printf("> Part %d of size %d\n", numMapFiles, remainingSize)

	partBuffer := make([]byte, remainingSize)
	partFileName := mapFileName(numMapFiles)
	partFile, _ := os.Create(partFileName)
	ioutil.WriteFile(partFileName, partBuffer, os.ModeAppend)
	partFile.Close()

	fmt.Println("\n*************************")
	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
