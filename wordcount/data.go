package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
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

	//Abrir o arquivo de entrada descrito como fileName
	inputFile, err := os.Open(fileName)
	if err != nil {
		log.Println("Couldn't open file '", fileName, "'. Error: ", err)
	}

	//Slice de bytes utilizado para guardar os valores do arquivo
	//com a capacidade dada pelo chunkSize
	content := make([]byte, chunkSize)

	//Pegando as informacoes do arquivo de entrada
	infoInputFile, err := inputFile.Stat()
	if err != nil {
		log.Println("Don't have stats for the file '", fileName, "'. Error: ", err)
	}

	//Quantidade de arquivos a serem gerados
	numMapFiles = int(math.Ceil(float64(infoInputFile.Size())/float64(chunkSize)))

	//Resto de bytes remanescentes para o ultimo arquivo
	//caso mod != 0
	mod := infoInputFile.Size() % int64(chunkSize)

	//Loop para a geracao de arquivos
	for i := 0; i < numMapFiles; i+=1 {

		//Caso seja o ultimo arquivo a ser gerado e
		//o ultimo arquivo nao contem exatamente chunkSize bytes
		if i == numMapFiles - 1 && mod != 0 {

			//Atribuir novo slice de bytes com o tamanho remanescente
			content = make([]byte, mod)

			//Ler o conteudo do arquivo e gravar em content
			inputFile.Read(content)

			//Gerar o nome do arquivo e o proprio arquivo
			nameOutputfile := mapFileName(int(i))
			outputFile, err := os.Create(nameOutputfile)
			if err != nil {
				log.Println("Can't create file '", nameOutputfile)
			}

			//Escrevendo conteudo de content no novo arquivo, 
			//sincronizando-o e fechando-o
			outputFile.Write(content)
			outputFile.Sync()
			outputFile.Close()

		} else { //Caso contrario

			//Ler o conteudo do arquivo e gravar em content
			inputFile.Read(content)

			//Gerar o nome do arquivo e o proprio arquivo
			nameOutputfile := mapFileName(int(i))
			outputFile, err := os.Create(nameOutputfile)
			if err != nil {
				log.Println("Can't create file '", nameOutputfile)
			}

			//Escrevendo conteudo de content no novo arquivo, 
			//sincronizando-o e fechando-o
			outputFile.Write(content)
			outputFile.Sync()
			outputFile.Close()
		}
	}
	
	//Finalizando a leitura do arquivo de entrada
	inputFile.Close()

	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
