package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"io"
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
//funcao auxiliar tirada do site https://gobyexample.com/writing-files
func check(e error) {
    if e != nil {
        panic(e)
    }
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
	//https://gobyexample.com/reading-files
	//https://gobyexample.com/writing-files
	//ajudou muito esse site!
	//temos que criar um buffer de tamanho "chunkSize"
	//depois vamos ler o arquivo de nome "fileName"
	//aí vem o loop:
	//vamos ler o arquivo de entrada, colocando no buffer até enche-lo
	//depois criamos um novo arquivo com o nome dado pela funçao
	//depois escrevemos o que ha no buffer no arquivo
	numMapFiles = 0
	//o buffer vai ser um slice de tamanho chunkSize
	buffer := make([]byte, chunkSize)
	//vamos abrir o arquivo de entrada
	entrada, err := os.Open(fileName)
	check(err)
	//agora vamos ler os bytes do arquivo e criar os arquivos de saida
	//o for deve começar lendo, verificar se o buffer esta vazio e depois tratar o EOF
	for bytesRead, err := entrada.Read(buffer); bytesRead != 0 ; bytesRead, err = entrada.Read(buffer){
	//nao vejo o erro na sentença do for, pois se o arquivo for menor que o buffer,
	//daria erro e nao entraria no for. Quero que, mesmo lançando o erro, ele entre no loop para criar a saida
	//ele só sai do loop quando nao consegue ler mais nada!
		if err != nil {
			if err != io.EOF {
				// nao é EOF error
				return 0, err				
			} else {
				nomeSaida := mapFileName(numMapFiles)
				saida, err1 := os.Create(nomeSaida)
				check(err1)
				//agora escrevemos o buffer na saida
				_, err2 := saida.Write(buffer)
				check(err2)
				numMapFiles++
				saida.Close()
			}
		} else {
			nomeSaida := mapFileName(numMapFiles)
			saida, err1 := os.Create(nomeSaida)
			check(err1)
			//agora escrevemos o buffer na saida
			_, err2 := saida.Write(buffer)
			check(err2)
			numMapFiles++
			saida.Close()
		}
	}
	//agora fechamos todos os arquivos
	entrada.Close()

	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
