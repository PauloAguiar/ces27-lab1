package main

import (
	"github.com/miltoneiji/mapreduce"
	"hash/fnv"
    "unicode"
    "bytes"
    "strings"
    "strconv"
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
    var buffer bytes.Buffer
    input = append(input, '.')
    var words []string
    var m map[string]int
    m = make(map[string]int)
    for idx := range input {
        c := input[idx]
        if !unicode.IsLetter(rune(c)) && !unicode.IsNumber(rune(c)) {
            if len(buffer.String()) > 0 {
                word := strings.ToLower(buffer.String())
                if m[word] == 0 {
                    words = append(words, word)
                }
                m[word] = m[word] + 1
            }
            buffer.Reset()
        } else {
            buffer.WriteString(string(c))
        }
    }
  
    for idx := range words {
        w := words[idx]
        result = append(result, mapreduce.KeyValue{w, strconv.Itoa(m[w])})
    }
	return result
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

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////

    var m map[string]int
    m = make(map[string]int)
    var wordList []string
    for idx := range input {
        key := input[idx].Key
        value := input[idx].Value
        
        if _, ok := m[key]; !ok {
            wordList = append(wordList, key)
        }
        
        tmp_value, err := strconv.Atoi(value)
        // In the case of num-numeric counter, the default value is 1
        if err != nil {
            tmp_value = 1
        }
        m[key] = m[key] + tmp_value
    }

    for idx := range wordList {
        w := wordList[idx]
        result = append(result, mapreduce.KeyValue{w, strconv.Itoa(m[w])})
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
