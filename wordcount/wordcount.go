package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"strings"
	"unicode"
	"unicode/utf8"
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

	result = make([]mapreduce.KeyValue, 0)

	// Bytes that don't correspond to delimeters are stored in a buffer until a delimiter (or end of input) is found
	buffer := make([]byte, 0)

	// The input content is accessed using a slice, so that the input itself remains unchanged
	inputSlice := input[:]

	// The following code is strongly based on the example from https://golang.org/pkg/unicode/utf8/#DecodeRune
	for len(inputSlice) > 0 {

		// Fetch next Unicode character (as a rune) from input
		r, size := utf8.DecodeRune(inputSlice)

		// If it's not a delimiter, add character to buffer
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			buffer = append(buffer, inputSlice[:size]...)

		// If it is a delimeter, then the current buffer content (if not empty) is a valid word
		// Convert the buffer to a lowercase string, add a KeyValue pair to the result and empty the buffer
		} else {
			if len(buffer) > 0 {
				result = append(result, mapreduce.KeyValue{strings.ToLower(string(buffer)), "1"})
				buffer = buffer[:0]
			}
		}

		// Moves to next character in input
		inputSlice = inputSlice[size:]

		// If end of input is found and there are still characters in the buffer, then it contains a valid word
		if len(inputSlice) == 0 && len(buffer) > 0 {
			result = append(result, mapreduce.KeyValue{strings.ToLower(string(buffer)), "1"})
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
	//  	package strconv: func Atoi(s string) (int, error)
	//
	// 	It's also possible to receive a non-numeric value (i.e. "+"). You can check the
	// 	error returned by Atoi and if it's not 'nil', use 1 as the value.

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////

	result = make([]mapreduce.KeyValue, 0)

	// This auxiliary map is used to count the number of occurrences of a certain key
	auxiliaryMap := make(map[string]int)

	for _, keyValuePair := range input {

		// If the value in the input key-value pair is numeric, then parse the value and update
		// the corresponding value in the auxiliary map.
		// If a key is not in the auxiliary map, 0 is returned as the corresponding value.
		// Using that fact, it's not necessary to check for the key.
		if value, err := strconv.Atoi(keyValuePair.Value); err == nil {
			auxiliaryMap[keyValuePair.Key] += value

		// If it's a non-numeric value, count as 1 occurrence
		// This considers that all possible non-numeric values are equivalent 
		// (e.g. "-" or "+" have the same meaning as values)
		} else {
			auxiliaryMap[keyValuePair.Key] += 1
		}
	}

	// Convert the key-value pairs in auxiliary map to the output format (array of mapreduce.KeyValue)
	for key, value := range auxiliaryMap {
		result = append(result, mapreduce.KeyValue{key, strconv.Itoa(value)})
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
