package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"bufio"
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

	/*
	 * Loading file
	 */
	f, _ := os.Open(fileName)

	s := bufio.NewScanner(f)
	
    s.Split(bufio.ScanRunes)

	/*
	 * Setting variables:
	 * 	chk: counts the chunksize for a file
	 * 	data: stores all string of a file
	 * 	word: stores a fragment of a file
	 * 	dataset: is an array of string per file
	 */
	chk := 0
	
	data := ""
	word := ""
	dataset := make([]string, 0)

	/*
	 * - for each rune do
	 *   - count rune size
	 *   - if a fragment does not reach the chunksize then 
	 *     - word should grows
	 *     - if current rune is a white space then
	 *       - data should grows
	 *       - word should reborn from nothing
	 *   - else
	 *     -  append dataset and reseat vars
	 */
	for s.Scan() {
		
		r := s.Text()

		chk += len(r)
		
		if chk < chunkSize {
			word += r
			
			if r == " " {
				data += word
				word = ""
			}
		} else {
			dataset = append(dataset, data)
			data = word + r
			chk =  len(data)
			
			if r == " " {
				word = ""
			}
		}
	}
	
	/*
	 * Appending last fragment
	 */
	dataset = append(dataset, data)
	
	/*
	 * creating a file for each fragment
	 */
	for index,content := range dataset {
		ioutil.WriteFile(mapFileName(index), []byte(content), 0644)
		numMapFiles++
	}

	
	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
