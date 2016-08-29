package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"io"
	"log"
	"os"
	"path/filepath"
	"unicode"
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

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////
	// Codigo Pedro Nunes Baptista aluno Especial CE-288
	var (
		arquivo_saida *os.File
		erro error
		bytes_lidos int
		data_temp []byte
	)

	numMapFiles = 0 //contador para os nomes dos arquivos de saida

	//abre arquivo de entrada
 	arquivo_ent, err := os.Open (fileName)
	if err != nil {
		log.Fatal(err)
	}

	//cria o slice para leitura com o tamanho desejado
	data := make([]byte, chunkSize)

	for erro != io.EOF  {//percorre todo o arquivo de entrada
			bytes_lidos, erro = arquivo_ent.Read(data)

			if  erro != nil {	//verifica se tem algum erro de leitura
				if erro == io.EOF { // se for EOF error
					//codigo de fim de arquivo
					if bytes_lidos != 0 { //caso em que o arquivo e menor que o chunk
								//escreve no arquivo_saida
								data_temp = data [:bytes_lidos]
								arquivo_saida, _ = os.Create(mapFileName(numMapFiles))
								numMapFiles ++
								_,_ = arquivo_saida.Write(data_temp)
								arquivo_saida.Close()
					} //else -- arquivo vazio, nao faz nada
				} else { //erro de leitura diferente de EOF
						panic(erro)
				}
			} else { //sem erros de leitura
				//tratamento do chunk para novo arquivo
						for i := 1; ; i++ { //percorre o chunk lido de trás para frente testando o ponto de corte
							letra := rune (data[bytes_lidos-i])
							if !unicode.IsLetter(letra) && !unicode.IsNumber(letra) {
								//ponto de corte ok
								data_temp = data [:bytes_lidos-i+1]
								//escreve no arquivo_saida
								arquivo_saida, _ = os.Create(mapFileName(numMapFiles))
								numMapFiles ++
								_,_ = arquivo_saida.Write(data_temp)
								arquivo_saida.Close()
								//reposiciona o arquivo de leitura para o ponto de corte
								_,_ = arquivo_ent.Seek(int64(-i+1), 1)
 								break // quebra o loop pois ja achou o ponto de corte
							}
						}
					}// fim tratamento do chunk para novo arquivo
	}

	//fim da funcao, encerramento do arquivo de entrada
	err = arquivo_ent.Close()
	if err != nil {
		log.Fatal(err)
	}
	return numMapFiles, nil
}




func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
