package main

import (
	//"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"unicode"
	"unicode/utf8"
	"strings"
	"strconv"
)

// mapFunc is called for each array of bytes read from the splitted files. For wordcount
// it should convert it into an array and parses it into an array of KeyValue that have
// all the words in the input.
func mapFunc(input []byte) (result []mapreduce.KeyValue) {

	//Create an empty array of KeyValues
	result = make([]mapreduce.KeyValue, 0)

	//This stores each individual word as it is being constructed from the input
	word := make([]byte, 0)

	//The following code is based on the one found on: https://golang.org/pkg/unicode/utf8/#DecodeRune

	//We work with a slice to facilitade manipulations
	b := input[:]


	for len(b) > 0 {
		//Gets the first rune in the input and the size
		r, size := utf8.DecodeRune(b)

		//If it's a letter or number, add it to the word currently being assembled
		if(unicode.IsLetter(r) || unicode.IsNumber(r)) {
			word = append(word, b[:size]...)
		} else { //If the character is not a letter or number, the word is finished
			//If the word exists, add it to the result and clean the word buffer
			if len(word) > 0 {
				result = append(result, mapreduce.KeyValue{strings.ToLower(string(word)), "1"})
				word = word[:0]
			}
		}
		//Move to the next rune
		b = b[size:]
	}

	//There may still be a word in the buffer when we reach the end. If there is, add it as well
	if len(word) > 0 {
		result = append(result, mapreduce.KeyValue{strings.ToLower(string(word)), "1"})
	}

	return result
}

// reduceFunc is called for each merged array of KeyValue resulted from all map jobs.
// It should return a similar array that summarizes all similar keys in the input.
func reduceFunc(input []mapreduce.KeyValue) (result []mapreduce.KeyValue) {

	//We store the strings as keys and the count as values so they can be easily accessed
	aux := make(map[string]int)

	for _, v := range input {
		//If the value is non-numeric, consider it 1
		if i, e := strconv.Atoi(v.Value); e != nil {
			//If the count for that word does not exist, it is created and set to 0
			aux[v.Key]++
		} else {
			//If the result is numeric, add it to the current count
			aux[v.Key] += i
		}
	}

	result = make([]mapreduce.KeyValue, 0)

	//Converts all the map entries to KeyValue entries in result
	for k, v := range aux {
		result = append(result, mapreduce.KeyValue{k, strconv.Itoa(v)})
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
