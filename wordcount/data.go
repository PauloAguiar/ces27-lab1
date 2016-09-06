package main

import (
	"unicode"
	"bufio"
	"io"
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	//"strconv"
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
	var (
		//nome string
		//inicio int
		//space int
		//position int
		tamanho int
		texto string
		palavra string
		//contador int
		nPalavra int 
	)
	//f, err := os.Open(fileName)

	//texto, err = ReadFile(fileName)
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	//inicio = 0
	tamanho = 0
	//position = 0
	//space = 0
	numMapFiles = -1
	nPalavra = 0
	arquivo := bufio.NewReader(file)


	for {
		if rune,_ ,err := arquivo.ReadRune(); err != nil {
			if err == io.EOF {
				numMapFiles = numMapFiles + 1
				saida,errw := os.Create(mapFileName(numMapFiles))
				if errw != nil {
						panic(errw)
					}
				w := bufio.NewWriter(saida)
				w.WriteString(texto)
				w.Flush()
				saida.Close()
				break
			} else {
				return 0, err
			}
		} else {
			palavra = palavra + fmt.Sprintf("%c", rune)
			nPalavra = nPalavra + 1
			if unicode.IsSpace(rune) {
				if tamanho + nPalavra < chunkSize {
					texto = texto + palavra
					palavra = ""
					tamanho = tamanho + nPalavra
					nPalavra = 0
				} else {
					numMapFiles = numMapFiles + 1
					//nome := "split" + strconv.Itoa(contador) + ".txt"
					saida,errw := os.Create(mapFileName(numMapFiles))
					if errw != nil {
						panic(errw)
					}
					w := bufio.NewWriter(saida)
					w.WriteString(texto)
					w.Flush()
					saida.Close()
					texto = palavra
					palavra = ""
					tamanho = nPalavra
					nPalavra = 0
				}
			}
		}
	}

	// if bytesRead, err = file.Read(buffer); err != nil {
	//  			if err == io.EOF {
	//  				// EOF error
	//  			} else {
	// 				return 0, err
	// 			}
	//  		}
	// s = string(texto)

	

	/////////////////////////
	//numMapFiles = 0
	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
