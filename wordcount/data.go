package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"log"
	"os"
    "io"
	"path/filepath"
    "unicode"
    "unicode/utf8"
)

const (
	MAP_PATH           = "map/"
	RESULT_PATH        = "result/"
	MAP_BUFFER_SIZE    = 10
	REDUCE_BUFFER_SIZE = 10
)

// fanInData will run a goroutine that reads files crated by splitData and share them with
// the mapreduce framework through the one-way channel. It'll buffer data up to
// MAP_BUFFER_SIZE (files smaller than chunkSize) and resume loading them
// after they are read on the other side of the channle (in the mapreduce package)
func fanInData(numFiles int) chan []byte {
	var (
		err    error
		input  chan []byte
		buffer []byte
	)

	input = make(chan []byte, MAP_BUFFER_SIZE)

	go func() {
		for i := 0; i < numFiles; i++ {
			if buffer, err = ioutil.ReadFile(mapFileName(i)); err != nil {
				close(input)
				log.Fatal(err)
			}

			log.Println("Fanning in file", mapFileName(i))
			input <- buffer
		}
		close(input)
	}()
	return input
}

// fanOutData will run a goroutine that receive data on the one-way channel and will
// proceed to store it in their final destination. The data will come out after the
// reduce phase of the mapreduce model.
func fanOutData() (output chan []mapreduce.KeyValue, done chan bool) {
	var (
		err           error
		file          *os.File
		fileEncoder   *json.Encoder
		reduceCounter int
	)

	output = make(chan []mapreduce.KeyValue, REDUCE_BUFFER_SIZE)
	done = make(chan bool)

	go func() {
		for v := range output {
			log.Println("Fanning out file", resultFileName(reduceCounter))
			if file, err = os.Create(resultFileName(reduceCounter)); err != nil {
				log.Fatal(err)
			}

			fileEncoder = json.NewEncoder(file)

			for _, value := range v {
				fileEncoder.Encode(value)
			}

			file.Close()
			reduceCounter++
		}

		done <- true
	}()

	return output, done
}

// Reads input file and split it into files smaller than chunkSize.
// CUTCUTCUTCUTCUT!
func splitData(fileName string, chunkSize int) (numMapFiles int, err error) {
	// 	When you are reading a file and the end-of-file is found, an error is returned.
	// 	To check for it use the following code:
	// 		if bytesRead, err = file.Read(buffer); err != nil {
	// 			if err == io.EOF {
	// 				// EOF error
	// 			} else {
	//				return 0, err
	//			}
	// 		}
	//
	// 	Use the mapFileName function to generate the name of the files!
	//
	//	Go strings are encoded using UTF-8.
	// 	This means that a character can't be handled as a byte. There's no char type.
	// 	The type that hold's a character is called a 'rune' and it can have 1-4 bytes (UTF-8).
	//	Because of that, it's not possible to index access characters in a string the way it's done in C.
	//		str[3] will return the 3rd byte, not the 3rd rune in str, and this byte may not even be a valid
	//		rune by itself.
	//		Rune handling should be done using the package 'strings' (https://golang.org/pkg/strings/)
	//
	//  For more information visit: https://blog.golang.org/strings
	//
	//	It's also important to notice that errors can be handled here or they can be passed down
	// 	to be handled by the caller as the second parameter of the return.

	/////////////////////////
	// YOUR CODE GOES HERE //
	/////////////////////////
	
    var (
        file *os.File
        fileInfo os.FileInfo
    )
    
    if file, err = os.Open(fileName); err != nil {
        log.Fatal(err)
        return 0, err
    }
    
    defer closeFile(file)

    if fileInfo, err = file.Stat(); err != nil {
        log.Fatal(err)
        return 0, err
    }

    fileSize := fileInfo.Size()
    
    buffer := make([]byte, fileSize)
    if _, err = file.Read(buffer); err!=nil && err!=io.EOF {
        log.Fatal(err)
        return 0, err
    }
    
    numMapFiles = 0
    minOffset := 0
    maxOffset := 0
    lastNotWordCharOffset := 0
    
    // IMPORTANT: This code already supports non-ascii texts;
    for currOffset, char:=range string(buffer[:]) {
        isWordChar := unicode.IsLetter(char) || unicode.IsNumber(char) // Word chars must be letters or numeric digits;
        charSize := utf8.RuneLen(char) // In bytes;
        if currOffset-minOffset+charSize>chunkSize { // Current char cannot be included;
            maxOffset = currOffset
            if isWordChar {
                maxOffset = lastNotWordCharOffset
            }
            if err = createFile(mapFileName(numMapFiles), buffer[minOffset:maxOffset]); err!=nil {
                log.Fatal(err)
                return numMapFiles, err
            }
            numMapFiles++
            minOffset = maxOffset
        } else if !isWordChar {
            lastNotWordCharOffset = currOffset+charSize
        }
    }
    // Check reamining bytes;
    if maxOffset<len(buffer) { // Last remaining bytes;
        if err = createFile(mapFileName(numMapFiles), buffer[maxOffset:]); err!=nil {
            log.Fatal(err)
            return numMapFiles, err
        }
        numMapFiles++
    }
    
	return numMapFiles, nil
}

func closeFile(file *os.File) {
    file.Close()
}

func createFile(fileName string, data []byte) (err error) {
    var file *os.File
    if file, err = os.Create(fileName); err != nil {
		return err
	}
    defer closeFile(file)
    if _, err = file.Write(data); err != nil {
		return err
	}
    return nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}
