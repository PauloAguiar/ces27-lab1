package main

import (
	//"fmt"
	"strings"
	"unicode"
	"strconv"
	//"bytes"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
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
	var words []string
	var str string

	myFunc := func(r rune) (bool) {
		if (!unicode.IsLetter(r) && !unicode.IsNumber(r)) {
			return true
		}
		return false
	}

	str = string(input)
	words = strings.FieldsFunc(str, myFunc)
	result = make([]mapreduce.KeyValue, 0)

	for i := 0; i < len(words); i++ {
		words[i] = strings.ToLower(words[i])
		aux := mapreduce.KeyValue {words[i], "1"}
		
		result = append(result, aux)

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
	//  	package strconv: func Atoi(s string) (int, error)
	//
	// 	It's also possible to receive a non-numeric value (i.e. "+"). You can check the
	// 	error returned by Atoi and if it's not 'nil', use 1 as the value.

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////
	auxmap := make(map[string]int)
	result = make([]mapreduce.KeyValue, 0)

	for i := 0; i < len(input) ; i++ {
		if _, ok := auxmap[input[i].Key]; !ok {
			// Don't have the key
			auxval, err := strconv.Atoi(input[i].Value)
			if err != nil {
					auxmap[input[i].Key] = 1
				} else {
					auxmap[input[i].Key] = auxval
			}
		} else {
			addition, err := strconv.Atoi(input[i].Value)
			if err != nil {
				auxmap[input[i].Key] = auxmap[input[i].Key] + 1
			} else {
				auxmap[input[i].Key] = auxmap[input[i].Key] + addition
			}
		}
	}

	for key, value := range auxmap {
		auxvalue := strconv.Itoa(value)
		auxKeyValue := mapreduce.KeyValue {key, auxvalue}
		result = append(result, auxKeyValue)
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
