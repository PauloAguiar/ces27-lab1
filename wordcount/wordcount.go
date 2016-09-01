package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"strings"
	"strconv"
	"unicode"
)

// mapFunc is called for each array of bytes read from the splitted files. For wordcount
// it should convert it into an array and parses it into an array of KeyValue that have
// all the words in the input.
func mapFunc(input []byte) (result []mapreduce.KeyValue) {
	
	//Variavel que sera retornada pela funcao
	result = make([]mapreduce.KeyValue, 0)

	//Se o input nao for vazio
	if len(input) != 0 {
		
		//Verificar se ha algum caracter que nao e numero
		//nem letra e substituir por espaco
		for carac := range input {
			runeAux := rune(input[carac])
			if !unicode.IsLetter(runeAux) && !unicode.IsNumber(runeAux) {
				input[carac] = ' '
			}
		}

		//Transformando o slice de bytes em string
		s := string(input[:])
		s = strings.ToLower(s)

		//Splitando a string anterior em slice de strings
		ss := strings.Fields(s)	

		//Para cada elemento do slice de strings, aumentar o tamanho
		//e adicionar o novo elemento no map
		for value := range ss {

			//Novo tamanho do map
			newLength := len(result) + 1
			
			//Inicializando novo map com o novo tamanho
			newResult := make([]mapreduce.KeyValue, newLength)

			//Copiando o conteudo do map velho ao map novo
			copy(newResult, result)
			result = newResult

			//Adicionando os valores de chave e valor ao ultimo
			//elemento do map novo
			result[newLength - 1].Key = ss[value]
			result[newLength - 1].Value = "1"
		}
	}

	return result
}

// reduceFunc is called for each merged array of KeyValue resulted from all map jobs.
// It should return a similar array that summarizes all similar keys in the input.
func reduceFunc(input []mapreduce.KeyValue) (result []mapreduce.KeyValue) {
	
	//Variavel que sera retornada pela funcao
	result = make([]mapreduce.KeyValue, 0)

	//Se o input nao for vazio
	if len(input) != 0 {
		
		//Runa necessaria que pega o caracter utilizado como counter
		runeAux := rune(input[0].Value[0])

		//Para cada elemento do map de entrada
		for elementInput := range input {
			
			elementIndex := -1

			//Verificar se a chave ja existe no map resultante (final)
			for index := range result {
				if result[index].Key == input[elementInput].Key {
					elementIndex = index
					break
				}
			}

			//Se a chave nao existir no map final ainda, deve-se adiciona-la
			if elementIndex == -1 {

				//Aumentando o tamanho do map
				newLength := len(result) + 1

				newResult := make([]mapreduce.KeyValue, newLength)
				copy(newResult, result)
				result = newResult

				result[newLength - 1].Key = input[elementInput].Key

				//Verificar se o couter e numero; caso nao seja, colocar
				//valor 1 ao valor para a chave correspondente
				if unicode.IsNumber(runeAux) {
					result[newLength - 1].Value = input[elementInput].Value
				} else {
					result[newLength - 1].Value = strconv.Itoa(1)
				}

			} else { //Se a chave ja existir no map final, somar os Value

				//Caso seja numero, somar como int;
				//Caso nao seja, apenas incrementar o valor
				if unicode.IsNumber(runeAux) {
					
					elementValue, _ := strconv.Atoi(result[elementIndex].Value)
					oldValue, _ := strconv.Atoi(input[elementInput].Value)
					elementValue += oldValue
					result[elementIndex].Value = strconv.Itoa(elementValue)
				} else {
					elementValue, _ := strconv.Atoi(result[elementIndex].Value)
					elementValue += 1
					result[elementIndex].Value = strconv.Itoa(elementValue)
				}

			}
		}
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
