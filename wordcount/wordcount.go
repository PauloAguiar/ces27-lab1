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

	string_casted := string(input);	
	string_casted = strings.ToLower(string_casted)

	length := len(string_casted);	
	result = make([]mapreduce.KeyValue, 0, length)
	mapStrings:=make(map[string]int)
	currentString:=make([]byte,0,length)

	for m := 0; m <length; m++ {
		if  isDelimiter(string_casted[m]) {        
			if len(currentString)>0{
				mapStrings[string(currentString)]++ 
				currentString=currentString[:0]
			}
		} else {
			currentString=AppendByte(currentString,string_casted[m])
		}
	}
	if length>0 && isDelimiter(string_casted[length-1])==false{
		if len(currentString)>0{			
			mapStrings[string(currentString)]++ 
		}
	}
	for key, value := range mapStrings {
		data := mapreduce.KeyValue{Key: key, Value: strconv.Itoa(value)}
		result = AppendKeyValue(result, data)		
	}
	return result
}
func isDelimiter(b byte)(answ bool){
	charac:=rune(b);
	if(!unicode.IsLetter(charac) && !unicode.IsNumber(charac)){
		return true
	}
	return false
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
	mapStrings:=make(map[string]int)
	length:=len(input)	
	result = make([]mapreduce.KeyValue, 0, length)

	for m:=0;m<length;m++{
		if v, err := strconv.Atoi(input[m].Value); err == nil {
		    mapStrings[input[m].Key]+=v;
		} else{
			mapStrings[input[m].Key]++
		}
	}
	for key, value := range mapStrings {
		data := mapreduce.KeyValue{Key: key, Value: strconv.Itoa(value)}
		result = AppendKeyValue(result, data)		
	}

	return result
}
func AppendByte(slice []byte, data byte) []byte {
    m := len(slice)
    n := m + 1
    slice = slice[0:n]
    slice[m]=data
    return slice
}
func AppendKeyValue(slice []mapreduce.KeyValue, data mapreduce.KeyValue) []mapreduce.KeyValue{
	m := len(slice)
    n := m + 1
    slice = slice[0:n]
    slice[m]=data
    return slice
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
