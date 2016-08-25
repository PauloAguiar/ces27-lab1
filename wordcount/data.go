package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"

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
	//  não cortar as palavras ao meio
	// var file *os.File

	// if file, err := os.Open(fileName); err != nil {
	// 	file.Close()
	// 	return 0, nil
	// }
	// defer file.Close()

	// numMapFiles = 0

	// fileInfo, _ := file.Stat()
	// var remainingSize = fileInfo.Size()

	// for remainingSize > int64(chunkSize) {
	// 	numMapFiles++

	// 	var decCarac = 0
	// 	var carac = '0'
	// 	for unicode.IsLetter(carac) || unicode.IsNumber(carac) {
	// 		var carac, _ = file.Seek(remainingSize-int64(decCarac), 0)
	// 		b2 := make([]byte, 2)
	// 		fmt.Printf("%d: %s\n", carac, string(b2))
	// 		decCarac++
	// 	}

	// 	partSize := chunkSize - decCarac
	// 	remainingSize -= int64(partSize)

	// 	partBuffer := make([]byte, partSize)

	// 	partFileName := mapFileName(numMapFiles)
	// 	os.Create(partFileName)
	// 	ioutil.WriteFile(partFileName, partBuffer, os.ModeAppend)
	// }

	// numMapFiles++

	// partBuffer := make([]byte, remainingSize)
	// partFileName := mapFileName(numMapFiles)
	// os.Create(partFileName)
	// ioutil.WriteFile(partFileName, partBuffer, os.ModeAppend)

	// Safely openning/closing the input text file
	file, err := os.Open(fileName)
	if err != nil {
		file.Close()
		return 0, nil
	}
	defer file.Close()

	// Estimating the number of divisions in mapFiles
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	numMapFiles = int(math.Ceil(float64(fileSize) / float64(chunkSize)))

	// For each generated MapFile
	for i := 0; i < numMapFiles; i++ {

		// Calculates the mapSize and then generates mapBuffer
		mapSize := int(math.Min(float64(chunkSize), float64(int(fileSize)-int(i*chunkSize))))
		mapBuffer := make([]byte, mapSize)

		// Move file ahead
		file.Read(mapBuffer)

		// Generates new MapFile's name and content
		mapName := mapFileName(i)
		mapFile, _ := os.Create(mapName)
		ioutil.WriteFile(mapName, mapBuffer, os.ModeAppend)
		mapFile.Close()
	}

	// Returning numMapFiles
	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
