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

	// The idea is to read the text movind "chunck" bytes forward and then
	//  go back character in character until finding a separator.
	// Then, at this separator the file will be broken

	// So, each MapFile would be like this:

	// ##############(Separator)###(Chunk)######################
	// ##############(mapFile 0)#################################
	// ##############(mapFile 0)#################(Separator)##(Chunk)
	// ##############(mapFile 0)#################(mapFile 1)##########
	// ##############(mapFile 0)#################(mapFile 1)#####(mapFile 2)

	var file, erro = os.Open(fileName)
	defer file.Close()
	if erro != nil {
		return 0, erro
	}

	fileInfo, _ := file.Stat()
	// Remaining Size of the file to be split
	var remainingSize = fileInfo.Size()
	// Size of the split map in bytes
	var mapSize int

	for numMapFiles = 0; remainingSize > 0; numMapFiles++ {
		if remainingSize > int64(chunkSize) {
			// When we still have at least two remaining mapFiles,
			//  we have to break the first file
			var recoil int
			var carac = '0'
			// Go back from chunckSize position untill the first separator
			for unicode.IsLetter(carac) || unicode.IsNumber(carac) {
				Ncarac, _ := file.ReadAt(make([]byte, 1), remainingSize-int64(recoil))
				carac = rune(Ncarac)
				recoil++
			}
			// Recalculate mapSize and updates remainingSize
			mapSize = chunkSize - recoil
			remainingSize -= int64(mapSize)

		} else {
			// This is the last generated mapFile, with mapSize
			//  equal the remainingSize of the file
			mapSize = int(remainingSize)
			remainingSize = 0
		}
		// Generating new mapFile with calculated mapSize
		mapFileName := mapFileName(numMapFiles)
		mapFile, _ := os.Create(mapFileName)
		ioutil.WriteFile(mapFileName, make([]byte, mapSize), os.ModeAppend)
		mapFile.Close()
	}

	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
