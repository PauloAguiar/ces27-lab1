package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"strings"
	"unicode"
	"strconv"
)

// mapFunc is called for each array of bytes read from the splitted files. For wordcount
// it should convert it into an array and parses it into an array of KeyValue that have
// all the words in the input.
func mapFunc(input []byte) (result []mapreduce.KeyValue) {
	// 	Pay attention! We are getting an array of bytes. Cast it to string.
	//
	// 	To decide if a character is a delimiter of a word, use the following check:
	//		!unicode.IsLetter(c) && !unicode.IsNumber(c)
	//
	//	Map should also make words lower cased:
	//		strings.ToLower(string)
	//
	// IMPORTANT! The cast 'string(5)' won't return the character '5'.
	// 		If you want to convert to and from string types, use the package 'strconv':
	// 			strconv.Itoa(5) // = "5"
	//			strconv.Atoi("5") // = 5

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////
	// Codigo Pedro Nunes Baptista aluno Especial CE-288
	var (
		entrada = string(input)
		palavra string
 		inicio, fim int
		chave mapreduce.KeyValue
	)

	result = make([]mapreduce.KeyValue, 0)

	for i,v := range(entrada){
			if !unicode.IsLetter(v) && !unicode.IsNumber(v){ //se nao for letra ou numero
			 	if (inicio <= fim) { //significa que uma palavra inteira foi identificada
					palavra = entrada [inicio:fim+1]
					chave.Key = strings.ToLower(palavra)
					chave.Value = "1"
					result = append (result, chave)
				}
				inicio = i+1 //adianta a posicao de inicio da proxima palavra
			} else { // se for uma letra ou numero incrementa o fim para a posicao atual
			 fim = i
		 }
	 }
	 if fim != 0 {// testa se a entrada não era vazia
		if (inicio <= fim) {//significa que o ultimo conjunto da entrada representa uma palavra
		 	palavra = entrada [inicio:fim+1]
			chave.Key = strings.ToLower(palavra)
			chave.Value = "1"
			result = append (result, chave)
	 	}
	 }

	return result
}

// reduceFunc is called for each merged array of KeyValue resulted from all map jobs.
// It should return a similar array that summarizes all similar keys in the input.
func reduceFunc(input []mapreduce.KeyValue) (result []mapreduce.KeyValue) {
	// 	Maybe it's easier if we have an auxiliary structure? Which one?
	//
	// 	You can check if a map have a key as following:
	// 		if _, ok := myMap[myKey]; !ok {
	//			// Don't have the key
	//		}
	//
	// 	Reduce will receive KeyValue pairs that have string values, you may need
	// 	convert those values to int before being able to use it in operations.
	//  	strconv.Atoi(string_number)

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////
	// Codigo Pedro Nunes Baptista aluno Especial CE-288
	var (
		ja_tem bool
		value, key string
		qty1, qty2 int
		err error
		)
  mapa_resultado := make(map[string]string)

	if len(input) != 0 { //entrada valida
		for _, chave := range(input) { //percorre toda a entrada

				value, ja_tem = mapa_resultado[chave.Key] //testa a existencia da chave

				if ja_tem { //chave repetida, soma o valor da chave com o do mapa

          qty1, err = strconv.Atoi(chave.Value)
					if err != nil { //caso nao seja um numero considera 1
						qty1 = 1
					}
          qty2, err = strconv.Atoi(value)
					if err != nil { //caso nao seja um numero considera 1
						qty2 = 1
					}

          mapa_resultado[chave.Key] = strconv.Itoa(qty1+qty2)

        } else { //nova chave

        	mapa_resultado[chave.Key] = chave.Value

        }
      }

		// converte o mapa para o slice de saida
		result = make([]mapreduce.KeyValue, len(mapa_resultado))
		i := 0
		for key, value = range(mapa_resultado)	{
			result[i].Key = key
			result[i].Value = value
			i++
		}

  } else { //entrada vazia retornara uma saida vazia
    	result = make([]mapreduce.KeyValue, 0)
  }

	return result
}

// shuffleFunc will shuffle map job results into different job tasks. It should assert that
// the related keys will be sent to the same job, thus it will hash the key (a word) and assert
// that the same hash always goes to the same reduce job.
// http://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
func shuffleFunc(task *mapreduce.Task, key string) (reduceJob int) {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(task.NumReduceJobs))
}
