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
	// 	Pay attention! We are getting an array of bytes.
	//
	// 	To decide if a character is a delimiter of a word, use the following check:
	//		!unicode.IsLetter(c) && !unicode.IsNumber(c)
	//
	//	Map should also make words lower cased:
	//		strings.ToLower(string)

	// Creating empty output Map for this problem
	var happyMap = make([]mapreduce.KeyValue, 0)
	// Adding a final separator to the last word of text
	text := strings.ToLower(string(input) + " ")
	// Initializing empty string "word"
	var word string
	// For each character of text:
	for _, char := range text {
		// Append letters and numbers to the word
		if unicode.IsLetter(char) || unicode.IsNumber(char) {
			word += string(char)
			// If it's a separator or unknown symbol,
			//  we have formed a new word. Check if it's not empty
		} else if word != "" {
			// Check if that word already exists on our happyMap
			for i, eachMap := range happyMap {
				if eachMap.Key == word {
					// Increase occurrency value on happyMap
					value, _ := strconv.Atoi(eachMap.Value)
					happyMap[i].Value = strconv.Itoa(value + 1)
					// Starts detecting new word
					word = ""
				}
			}
			// If the detected word wasn't found on happyMap
			if word != "" {
				// Includes it on the happyMap with value 1
				happyMap = append(happyMap, mapreduce.KeyValue{Key: word, Value: "1"})
				// Starts detecting new word
				word = ""
			}
		}
	}
	// Returns happyMap
	return happyMap
}

// reduceFunct is called for each merged array of KeyValue resulted from all map jobs.
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

	var happyMap = make([]mapreduce.KeyValue, 0)
	var found = false
	for _, Hmap := range input {
		for j, myHmap := range happyMap {
			if myHmap.Key == Hmap.Key {
				inc1, err1 := strconv.Atoi(Hmap.Value)
				inc2, err2 := strconv.Atoi(myHmap.Value)
				if err1 != nil {
					inc1 = 1
				}
				if err2 != nil {
					inc2 = 1
				}
				happyMap[j].Value = strconv.Itoa(inc1 + inc2)
				found = true
			}
		}
		if !found {
			happyMap = append(happyMap, Hmap)
		}
		found = false
	}
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
