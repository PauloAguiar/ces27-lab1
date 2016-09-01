package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	//"math"
	//"strconv"
	//"io"
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
func splitData(fileName string, chunkSize int) (numMapFiles int, err error) {
	
	file, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	//var fileSize int = len(file)
	var buffer []byte
	var bufferPalavra []byte
	counter := 0
	countIn := 0
	for _, num := range file {
		if len(buffer)<=0 && len(bufferPalavra) > 0 {
			for _, num2 := range bufferPalavra{
				buffer = append(buffer, num2)
			}
		}
		if num != 32 && countIn != 0{
			bufferPalavra = append(bufferPalavra, num)
		} else {
			bufferPalavra = append(bufferPalavra, num)
			bufferTotal := len(buffer) + len(bufferPalavra)
			if bufferTotal >= chunkSize {
				name := mapFileName(counter)
				smallFile, err := os.Create(name)
				if err != nil {
					fmt.Println(err)
					return 0, err
				}
				ioutil.WriteFile(name, buffer, os.ModeAppend)
				smallFile.Close()
				counter++
				buffer = buffer[:0]
			} else {
				for _, num2 := range bufferPalavra{
					buffer = append(buffer, num2)
				}
				bufferPalavra = bufferPalavra[:0]
			}
		}
		countIn = 1
	}
	if len(buffer) >0 {
		for _, num2 := range bufferPalavra{
			buffer = append(buffer, num2)
		}
		name := mapFileName(counter)
		smallFile, err := os.Create(name)
		if err != nil {
			fmt.Println(err)
			return 0, err
		}
		ioutil.WriteFile(name, buffer, os.ModeAppend)
		smallFile.Close()
		buffer = buffer[:0]
	}
	return counter, err
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}