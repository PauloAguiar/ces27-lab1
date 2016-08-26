package main

import (
	"hash/fnv"
	"strconv"
	"strings"
	"unicode"

	"github.com/pauloaguiar/ces27-lab1/mapreduce"
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

	// Creating empty output Map for this problem
	var happyMap = make([]mapreduce.KeyValue, 0)
	// Creating auxiliary hashMap to help the solution
	var auxMap = make(map[string]int)
	// Adding a final separator to the last word of text
	//  so that it's detected inside the for loop
	text := strings.ToLower(string(input) + " ")
	var word string
	for _, char := range text {
		// Append letters and numbers to the word
		if unicode.IsLetter(char) || unicode.IsNumber(char) {
			word += string(char)
		} else if word != "" { // We have formed a new word. Check if it's not empty.
			// Check if that word already exists on our happyMap
			//   in a fast O(1) way using the auxialiary hashMap
			if _, ok := auxMap[word]; ok {
				auxMap[word]++
			} else {
				// The key doesn't exists. Starts counter
				auxMap[word] = 1
			}
			word = ""
		}
	}
	// Just go through the auxialiary hashmap, adding results Maps to happyMap
	for key, value := range auxMap {
		var resultMap mapreduce.KeyValue
		resultMap.Key = key
		resultMap.Value = strconv.Itoa(value)
		happyMap = append(happyMap, resultMap)
	}
	// HappyMap built in O(n) solution
	return happyMap
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

	// Creating empty output Map for this problem
	var happyMap = make([]mapreduce.KeyValue, 0)
	// Creating auxiliary hashMap to help the solution
	var auxMap = make(map[string]int)
	for _, eachMap := range input {
		// Tries to get eachMap value detected on previous steps
		var value, err = strconv.Atoi(eachMap.Value)
		// If it's not numeric, gets its length as value
		if err != nil {
			value = len(eachMap.Value)
		}
		// Checks if the eachMap's Key is on auxiliary hashMap
		//  (that's a O(1) operation. Super fast)
		if _, ok := auxMap[eachMap.Key]; ok {
			auxMap[eachMap.Key] += value
		} else {
			// The key doesn't exists. Starts counter
			auxMap[eachMap.Key] = value
		}
	}
	// Just go through the hashmap, adding results Maps to happyMap
	for key, value := range auxMap {
		var resultMap mapreduce.KeyValue
		resultMap.Key = key
		resultMap.Value = strconv.Itoa(value)
		happyMap = append(happyMap, resultMap)
	}
	// HappyMap built in O(n) solution
	return happyMap
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
