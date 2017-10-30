package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"unicode"
	"strings"
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

	last := 0;
	result = make([]mapreduce.KeyValue, len(input));
	cnt := 0;

	for i := 0; i <= len(input); i++ {

		var c rune;
		if i < len(input) {
			c = rune(input[i]);
		} else {
			c = ' ';
		}

		if !unicode.IsLetter(c) && !unicode.IsNumber(c) {

			if i > last {
				var kv mapreduce.KeyValue;
				kv.Key = strings.ToLower(string(input[last:i]));
				kv.Value = "1";
				result[cnt] = kv;
				cnt++;
			}

			last = i+1;
		}
	}

	return result[0:cnt];
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

	var app map[string]int;
	app = make(map[string]int);
	
	for i := 0; i < len(input); i++ {
		var kv mapreduce.KeyValue;
		kv = input[i];

		var cnt int;
		if !unicode.IsNumber(rune(kv.Value[0])) {
			cnt = 1;
		} else {
			cnt, _ = strconv.Atoi(kv.Value);
		}

		app[kv.Key] = app[kv.Key] + cnt;
	}

	result = make([]mapreduce.KeyValue, len(app));

	i := 0;
	for key, cnt := range app {
		result[i].Key = key;
		result[i].Value = strconv.Itoa(cnt);
		i++;
	}
	
	return result;
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
