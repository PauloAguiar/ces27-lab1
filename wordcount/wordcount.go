package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"unicode"
	"strings"
	"sort"
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
	var n = len(input)
	var s = make([]string, n)
	var k = 0
	var i = 0

	for i < n {
		for i < n && !unicode.IsLetter(rune(input[i])) && !unicode.IsNumber(rune(input[i])) {
			i++
		}
		if i >= n {
			break
		}
		var j = i
		for j < n && !(!unicode.IsLetter(rune(input[j])) && !unicode.IsNumber(rune(input[j]))) {
			j++
		}
		s[k] = strings.ToLower(string(input[i:j]))
		i = j
		k++
	}
	result = make([]mapreduce.KeyValue, k)
	for i := 0; i < k; i++ {
		result[i].Key = s[i]
		result[i].Value = "+"
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
	var myMap = make(map[string]int)
	
	for _, kv := range input {
		x, err := strconv.Atoi(kv.Value)
		if _, ok := myMap[kv.Key]; !ok {
			if err != nil{
				myMap[kv.Key] = 1
			} else{
				myMap[kv.Key] = x
			}
		} else{
			if err != nil{
				myMap[kv.Key]++
			} else{
				myMap[kv.Key] += x
			}
		}
	}
	i := 0
	var n = len(myMap)
	aux := make([]string, n)
	for k, _ := range myMap {
		aux[i] = k
		i++
	}
	sort.Strings(aux)
	result = make([]mapreduce.KeyValue, n)
	for i = 0; i < n; i++ {
		result[i].Key = aux[i]
		result[i].Value = strconv.Itoa(myMap[aux[i]])
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
