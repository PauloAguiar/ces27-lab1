package main

import (
	"bytes"
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
	var (
		readWord bytes.Buffer
		kvPairs  map[string]int
	)

	kvPairs = make(map[string]int)

	// Append a whitespace to the end so that the last word is also evaluated
	for _, c := range string(append(input, byte(' '))) {
		if !unicode.IsLetter(c) && !unicode.IsNumber(c) {
			if readWord.Len() > 0 {

				word := readWord.String()
				word = strings.ToLower(word)

				if _, present := kvPairs[word]; present {
					kvPairs[word]++
				} else {
					kvPairs[word] = 1
				}
			}

			readWord.Reset()

		} else {
			readWord.WriteString(string(c))
		}
	}

	result = make([]mapreduce.KeyValue, 0, len(kvPairs))
	for index, value := range kvPairs {
		result = append(result, mapreduce.KeyValue{index, strconv.Itoa(value)})
	}

	return result
}

// reduceFunct is called for each merged array of KeyValue resulted from all map jobs.
// It should return a similar array that summarizes all similar keys in the input.
func reduceFunc(input []mapreduce.KeyValue) (result []mapreduce.KeyValue) {
	var (
		kvInputMap map[string]int
		value      int
		err        error
	)

	kvInputMap = make(map[string]int)

	for _, kvInputPair := range input {
		if value, err = strconv.Atoi(kvInputPair.Value); err != nil {
			value = 1
		}
		if _, keyPresent := kvInputMap[kvInputPair.Key]; keyPresent {
			kvInputMap[kvInputPair.Key] += value
		} else {
			kvInputMap[kvInputPair.Key] = value
		}
	}

	result = make([]mapreduce.KeyValue, 0, len(kvInputMap))
	for index, value := range kvInputMap {
		result = append(result, mapreduce.KeyValue{index, strconv.Itoa(value)})
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
