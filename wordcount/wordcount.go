package main

import (
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"unicode" //coloquei
	"fmt"
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
	//Go'eh unicode entao nao posso ler byte a byte
	var text = string(input)
	text = text + " " //bizu pra entrar a ultima palavra
	text = strings.ToLower(text)
	var word string = ""
	var mapAux map[string]int = make(map[string]int)//make aloca
	
	for _, c := range text {  //i is 0,1, etc  c is the character
		if unicode.IsLetter(c) || unicode.IsNumber(c) {
			word = word + string(c)
		} else {
			if word != "" {
				_, ok := mapAux[word]
				if !ok {  //If the map does not exist,  create it
					mapAux[word] = 1
				} else { //If the map exists, add 1 (counting more one equal word)
					mapAux[word]++
				} 
				//println(word)
				word = "" //reset the word to compute the next one
			}
		}	
	}
	
	for k, v := range mapAux {
		result = append(result, mapreduce.KeyValue{k,strconv.Itoa(v)})
	}
	//fmt.Printf("%v", mapAux)
	return result
	//return make([]mapreduce.KeyValue, 0)//saida padrao pro teste rodar
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
	var mapAux map[string]int = make(map[string]int)
	
	for _,item := range input { 
		_, ok := mapAux[item.Key]
		v,err := strconv.Atoi(item.Value)	//v is the generated number, err indicate error in the conversion	
		if !ok { //If the key (word) is not in the map, create it
			if err == nil { 
				mapAux[item.Key]=v //when the Value string represents a number
			} else {
				mapAux[item.Key]=1 //when the Value string is any symbol (so we cannot convert to number)
			}
		} else{ //Increment the quantity of the word
			if err == nil { 
				mapAux[item.Key]=mapAux[item.Key] + v //increment by the other value
			} else {
				mapAux[item.Key]=mapAux[item.Key] + 1 //only increment by 1
			}			
		}
	}
	for k, v := range mapAux {
		result = append(result, mapreduce.KeyValue{k,strconv.Itoa(v)})
	}
	fmt.Printf("%v", mapAux)
	
	return result
	//return make([]mapreduce.KeyValue, 0)
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
