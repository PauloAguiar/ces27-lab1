package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"unicode"
	"strconv"
	"strings"
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

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////
	mapaux := make(map[string]int)
	var mapfinal []mapreduce.KeyValue
	first :=0
	texto := string(input)
	ehpalavra := false
	i := 0
	for ;i<len(input);i++{
		c:=rune(texto[i])
		if !unicode.IsLetter(c) && !unicode.IsNumber(c) && ehpalavra{
			palavra := strings.ToLower(string(texto[first:i]))
			aValue, exists := mapaux[palavra]
			if exists {
				mapaux[palavra] = aValue + 1	
			} else{
				mapaux[palavra] = 1
			}
			first=i+1
			ehpalavra=false
		} else if (!ehpalavra && (unicode.IsLetter(c) || unicode.IsNumber(c) ) ) {
			first = i
			ehpalavra = true
		}
	}

	if ehpalavra {
		palavra := string(texto[first:i])
		aValue, exists := mapaux[palavra]
		if exists {
			mapaux[palavra] = aValue + 1	
		} else{
			mapaux[palavra] = 1
		}
	}

	for k := range mapaux{
		mapfinal = append(mapfinal, mapreduce.KeyValue{k, strconv.Itoa(mapaux[k]) }) //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
	}
	return mapfinal
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

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////
	var arrayfinal []mapreduce.KeyValue
	mapaux := make(map[string]int)
	intaux := 0
	for ninput := range input{
		if input[ninput].Value == "+" {
			intaux = 1
		} else {
			intaux, _ = strconv.Atoi( input[ninput].Value )
		}
		if value, exists := mapaux[ input[ninput].Key ]; exists{
			mapaux[ input[ninput].Key ] = value + intaux
		} else {
			mapaux[ input[ninput].Key ] = intaux
		}
	}
	for k := range mapaux {
		arrayfinal = append(arrayfinal, mapreduce.KeyValue{k, strconv.Itoa(mapaux[k]) })
	}
	return arrayfinal
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
