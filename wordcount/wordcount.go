package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"strings"
	"unicode"
	"strconv"
	"bytes"
	//"utf8"	
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
	result = make([]mapreduce.KeyValue, 0) 
	
	n := bytes.Index(input, []byte{0}) //input size
	s := string(input[0:n]) //converts array of bytes into an array
	strings.ToLower(s) //make words lower cased

	sepAux := 0
	for sep := 1 ; sep < len(s)  ; sep++ {
		//case a separator is found, new word must be added to the result
		if( !unicode.IsLetter(rune(s[sep])) && !unicode.IsNumber(rune(s[sep]))){
			result = append(result , mapreduce.KeyValue{Key:s[(sepAux + 1):(sep - sepAux - 1)] , Value:strconv.Itoa(1)})
			sepAux = sep 
		}
	}
	
	return result
	/*for c := 0 ; c < utf8.RuneCountInString(s) ;  c++ {
		if !unicode.IsLetter(s[c]) && !unicode.IsNumber(s[c]) {
			//array with positions of separators
			n := len(slice)
    		if n == cap(slice) {
        		// Slice is full; must grow.
        		// We double its size and add 1, so if the size is zero we still grow.
        		newSlice := make([]int, len(slice), 2*len(slice)+1)
        		copy(newSlice, slice)
        		slice = newSlice
    		}
   		 	slice = slice[0 : n+1]
    		slice[n] = c
		} 	
	}
	for i = 0 ; i < len(slice) - 1 ; i ++ {
		result[mapreduce.KeyValue{s[(slice[i] + 1):(slice[i+1] - slice[i])], 1}]
	}*/
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
	//iterar sobre o input usando range
	//for	input range 
		//contar o número de ocorrências 
	//	strconv.Atoi(element.value) 

	result = make([]mapreduce.KeyValue, 0)
	m :=  make(map[string]string)
	for _ , element := range input {
		if _, ok := m[element.Key]; !ok {
			// Don't have the key
			//m[element.Key] = strconv.Atoi(element.Value)
			m[element.Key] = element.Value 
		} else {
			 val1 , err1 := strconv.Atoi(m[element.Key])
			 val2 , err2 := strconv.Atoi(element.Value)
			 if err1 == nil && err2 == nil {
				m[element.Key] = strconv.Itoa(val1 + val2)
			} else {
				m[element.Key] = "1"
			}
		}
	}

	for key, value  := range m {
		//aux := mapreduce.KeyValue{Key:k , Value: strconv.Itoa(m[k])}
		aux := mapreduce.KeyValue{Key:key , Value: value}
		result = append ( result , aux )
	}
	return result
	/*
	for index , element := range input {
		if _, ok := result[element]; !ok {
			// Don't have the key
			result = append ( result , element ) 
		} else {
			//adiciona 1 ao value do elemento já está em result
			for indexR , elementR := range result {
				if(Compare(element.Key , elementR.Key)){
					result[indexR].Value = strconv.Itoa(strconv.Atoi(elementR.Value) + 1)  
				}
			}
		}	
	}*/
	
	
	//for _, value := range array {
    //	sum += value
	//}
	
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
