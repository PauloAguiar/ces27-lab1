package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"strconv"
	"strings"
	"unicode"
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

	//Doente, me perdoa por esse codigo tao feio :(
	result = make([]mapreduce.KeyValue, 0)
	var k mapreduce.KeyValue
	var i int = 0
	var ini int = 0
	var c rune
	var str string

	for ; i < len(input);  i++ {
		c = rune(input[i])
		if str = string(input[ini:i]) ; !unicode.IsLetter(c) && !unicode.IsNumber(c){
			if i != ini{
				str = strings.ToLower(str)
				k = mapreduce.KeyValue{Key: str , Value: string(1)}
				result = append(result, k)
			}
			ini = i + 1
		}
	}

	if i != ini{
		str = string(input[ini:i]) 
		str = strings.ToLower(str)
		k = mapreduce.KeyValue{Key: str , Value: string(1)}
		result = append(result, k)
	}
	//Enquanto o array de bytes nao tiver acabado
		//acha o proximo ponto de separacao
		//converte para string
		//guarda em um vetor mapreduce
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
	//  	package strconv: func Atoi(s string) (int, error)
	//
	// 	It's also possible to receive a non-numeric value (i.e. "+"). You can check the
	// 	error returned by Atoi and if it's not 'nil', use 1 as the value.

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////

	//Codigo reutilizado de wordcount_test.go
	var (
		combined  map[string]int
		k mapreduce.KeyValue
	)


		combined = make(map[string]int, 0)

		for _, kv := range input {
			if _, ok := combined[kv.Key]; !ok {
				value, err := strconv.Atoi(kv.Value)
				if err != nil {
					combined[kv.Key] = 1
				} else {
					combined[kv.Key] = value
				}

			} else {
				value, err := strconv.Atoi(kv.Value)
				if err != nil {
					combined[kv.Key] += 1
				} else {
					combined[kv.Key] += value
				}
			}
		}

		//result = make([]mapreduce.KeyValue, 0)
		for str, value := range combined{
			s := strconv.Itoa(value)
			k = mapreduce.KeyValue{Key: str , Value: s}
			result = append(result, k)
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
