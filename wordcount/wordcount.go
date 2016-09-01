package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"strings"
	"unicode"
	"strconv"
)

func appendToKVSplit(word string, input []mapreduce.KeyValue)(result []mapreduce.KeyValue){
	var item mapreduce.KeyValue
	item.Key = strings.ToLower(word)
	item.Value = "1"
	result = append(input, item)

	return result
}

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
	
	// Get the size of the input chunk
	input_size := len(input)
	// Cast it to string
	s_input := string(input[:input_size])
	// Create slice with result
	result = make([]mapreduce.KeyValue, 0)
	// Detect and list the words
	word := ""

	for _, c := range s_input {
		if !unicode.IsLetter(c) && !unicode.IsNumber(c) {
			if word != "" {
				result = appendToKVSplit(word, result)
				word = ""
			}
		} else {
			word = word + string(c)
		}
	}

	// Get the last word
	if word != "" {
		result = appendToKVSplit(word, result)
	}

	return result
}

// reduceFunc is called for each merged array of KeyValue resulted from all map jobs.
// It should return a similar array that summarizes all similar keys in the input.
func reduceFunc(input []mapreduce.KeyValue) (result []mapreduce.KeyValue) {
	// 	Maybe it's easier if we have an auxiliary structure? Which one?
	//
	// 	You can check if a map has a key as following:
	// 		if _, ok := myMap[myKey]; !ok {
	//			// Don't have the key
	//		}
	//
	// 	Reduce will receive KeyValue pairs that have string values, you may need to
	// 	convert those values to int before being able to use it in operations.
	//  	strconv.Atoi(string_number)

	result = make([]mapreduce.KeyValue, 0)

	wordsMap := map[string]int{}

	for _, kvPair := range input {
		if _, ok := wordsMap[kvPair.Key]; !ok {
			if value, err := strconv.Atoi(kvPair.Value); err != nil {
				wordsMap[kvPair.Key] = 1
			} else {
				wordsMap[kvPair.Key] = value
			}
		} else {
			if value, err := strconv.Atoi(kvPair.Value); err != nil {
				wordsMap[kvPair.Key] += 1
			} else {
				wordsMap[kvPair.Key] += value
			}
		}
	}

	for k, v := range wordsMap {
		var item mapreduce.KeyValue
		item.Key = k
		item.Value = strconv.Itoa(v)
		result = append(result, item)
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
