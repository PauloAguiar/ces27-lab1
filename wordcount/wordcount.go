package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"strconv"
	"strings"
	"unicode"
)

/**
 * mapFunc is called for each array of bytes read from the splitted files. For wordcount
 * it should convert it into an array and parses it into an array of KeyValue that have
 * all the words in the input.
 */
func mapFunc(input []byte) (result []mapreduce.KeyValue) {
	/*
	 * Pay attention! We are getting an array of bytes. Cast it to string.
	 *
	 * To decide if a character is a delimiter of a word, use the following check:
	 * !unicode.IsLetter(c) && !unicode.IsNumber(c)
	 *
	 * Map should also make words lower cased:
	 * 	strings.ToLower(string)
	 *
	 * IMPORTANT! The cast 'string(5)' won't return the character '5'.
	 *		If you want to convert to and from string types, use the package 'strconv':
	 *			strconv.Itoa(5) // = "5"
	 *			strconv.Atoi("5") // = 5
	 */

	/*
	 * Casting byte array to string
	 */
	s := string(input)

	/*
	 * Split text and generating output
	 */
	result = make([]mapreduce.KeyValue, 0)

	word := ""

	for _, runeValue := range s {

		if !unicode.IsLetter(runeValue) && !unicode.IsNumber(runeValue) {

			if len(word) > 0 {
				result = append(result, mapreduce.KeyValue{strings.ToLower(word), "1"})
				word = ""
			}
		} else {
			word += string(runeValue)
		}
	}

	/*
	 * Adding last word
	 */
	if len(word) > 0 {
		result = append(result, mapreduce.KeyValue{strings.ToLower(word), "1"})
	}

	return result
}

/*
 * reduceFunc is called for each merged array of KeyValue resulted from all map jobs.
 * It should return a similar array that summarizes all similar keys in the input.
 */
func reduceFunc(input []mapreduce.KeyValue) (result []mapreduce.KeyValue) {
	/*
	 *	Maybe it's easier if we have an auxiliary structure? Which one?
	 *
	 * 	You can check if a map have a key as following:
	 * 		if _, ok := myMap[myKey]; !ok {
	 *			// Don't have the key
	 *		}
	 *
	 * 	Reduce will receive KeyValue pairs that have string values, you may need
	 * 	convert those values to int before being able to use it in operations.
	 *  	package strconv: func Atoi(s string) (int, error)
	 *
	 * 	It's also possible to receive a non-numeric value (i.e. "+"). You can check the
	 * 	error returned by Atoi and if it's not 'nil', use 1 as the value.
	 */

	m := make(map[string]int)

	for _, str := range input {

		if _, ok := m[str.Key]; !ok {
			m[str.Key] = 0
		}

		i, err := strconv.Atoi(str.Value)

		if err != nil {
			m[str.Key] = m[str.Key] + 1
		} else {
			m[str.Key] = m[str.Key] + i
		}
	}

	result = make([]mapreduce.KeyValue, 0)

	for k := range m {
		result = append(result, mapreduce.KeyValue{k, strconv.Itoa(m[k])})
	}

	return result
}

/*
 * shuffleFunc will shuffle map job results into different job tasks. It should assert that
 * the related keys will be sent to the same job, thus it will hash the key (a word) and assert
 * that the same hash always goes to the same reduce job.
 * http://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
 */
func shuffleFunc(task *mapreduce.Task, key string) (reduceJob int) {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(task.NumReduceJobs))
}
