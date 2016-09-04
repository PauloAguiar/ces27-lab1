package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"unicode/utf8"
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
	var(
		fileBuffer []byte //Buffer that stores everything from the file
		file *os.File //File that we create to store the pieces
		fileSize int //We keep track of the file size in bytes
	)

	//Attempts to open the input, if not successful, "throws" the erro
	if fileBuffer, err = ioutil.ReadFile(fileName); err != nil {
		return 0, err
	}


	numMapFiles = 0
	word := make([]byte, 0)

	//Creates the first split file, "throws" an error if any
	if file, err = os.Create(mapFileName(numMapFiles)); err != nil {
		return 0, err
	}
	numMapFiles++
	fileSize = 0

	//Iterates through all the runes in the input
	for len(fileBuffer) > 0 {
		r, size := utf8.DecodeRune(fileBuffer)

		//If the character is somehow larger than the chunk size, return an error
		if size > chunkSize {
			return 0, errors.New("Character size larger than chunk size")
		}

		/*We attempt to assemble an entire word before adding it to the file as not to
		cut a word in half during the process. If another character is found, adds it 
		directly to the file */

		if(unicode.IsLetter(r) || unicode.IsNumber(r)) {
			word = append(word, fileBuffer[:size]...)
		} else {
			//If another character is encountered, first attempts do add the finished
			//word to the file
			if len(word) > 0 {
				//If a word that is bigger than the chunk size is found, we cant handle it
				if len(word) > chunkSize {
					return 0, errors.New("Word larger than chunk size")
				} 
				//If adding the word exceed the chunk size, starts a new file
				if fileSize + len(word) > chunkSize {
					file.Close()
					if file, err = os.Create(mapFileName(numMapFiles)); err != nil {
						return numMapFiles, err
					}
					numMapFiles++
					fileSize = 0
				}
				if _, err = file.Write(word); err != nil {
					return numMapFiles, err
				}
				fileSize += len(word)
				//Cleans the word buffer
				word = word[:0]
			}
			//Now attempts to add the next character to the file
			//If it passes the cuhunk size, starts a new file
			if fileSize + size > chunkSize {
				file.Close()
				if file, err = os.Create(mapFileName(numMapFiles)); err != nil {
					return numMapFiles, err
				}
				numMapFiles++
				fileSize = 0
			}
			if _, err = file.Write(fileBuffer[:size]); err != nil {
				return numMapFiles, err
			}
			fileSize += size
		}
		//Continue to the next rune
		fileBuffer = fileBuffer[size:]
	}

	//If there's still a word in the buffer, add the word to the last file
	//Or creates a new file if there's no space
	if len(word) > 0 {
		if len(word) > chunkSize {
			return 0, errors.New("Word larger than chunk size")
		}
		if fileSize + len(word) > chunkSize {
			file.Close()
			if file, err = os.Create(mapFileName(numMapFiles)); err != nil {
				return numMapFiles, err
			}
			numMapFiles++
		}
		if _, err = file.Write(word); err != nil {
			return numMapFiles, err
		}
	}

	file.Close()

	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
