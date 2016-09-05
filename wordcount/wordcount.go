package main

import (
	//"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"hash/fnv"
	"strings"
	"unicode"
	"strconv"
)

// mapFunc is called for each array of bytes read from the splitted files. For wordcount
// it should convert it into an array and parses it into an array of KeyValue that have
// all the words in the input.
func mapFunc(input []byte) (result []mapreduce.KeyValue) {
	s := string(input)	
	rot := func(r rune) rune {
		switch {
		case !unicode.IsLetter(r)&&!unicode.IsNumber(r):
			return ' '
		}
		return r
	}
	f := strings.Map(rot, s)
	g := strings.ToLower(f)
	h := strings.Split(g," ")
	var x []string
	//fmt.Printf("Input Length: %v\n", len(input))
	for _, num := range h{
		if num != "" {
			x = append(x,num)
		}
	}
	result = make([]mapreduce.KeyValue, 0)
	for _, num := range x {
		result = append(result, mapreduce.KeyValue{num, "1"})
	}
	return result
}

// reduceFunc is called for each merged array of KeyValue resulted from all map jobs.
// It should return a similar array that summarizes all similar keys in the input.
func reduceFunc(input []mapreduce.KeyValue) (result []mapreduce.KeyValue) {
	store := make([]mapreduce.KeyValue, 0)	
	for k, num := range input {
		_, errx := strconv.Atoi(num.Value)
		if errx != nil {
			input[k].Value = "1"
		}	
	}
	for _, num := range input {
		if stringInArray(num.Key, store) {
		for j, num2 := range store {
			if (num.Key == num2.Key) {
				aux, err1 := strconv.Atoi(num.Value)
				if err1 != nil {
					aux = 1
				}
				aux2, err2 := strconv.Atoi(num2.Value)
				if err2 != nil {
					aux2 = 1
				}
				aux3 := aux + aux2
				store[j].Value = strconv.Itoa(aux3)
			}
		}
		} else {
			_, err := strconv.Atoi(num.Value)
			if err == nil {
				store = append(store, num)
			} else {
				store = append(store, mapreduce.KeyValue{num.Key, "1"})
			}			
		}
	}
	return store
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

func stringInArray(key string, list []mapreduce.KeyValue) bool {
    for _, each := range list {
        if each.Key == key {
            return true
        }
    }
    return false
}
