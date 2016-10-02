package main

import (
	"encoding/json"
	"fmt"
	"github.com/pauloaguiar/ces27-lab1/mapreduce"
	"io/ioutil"
	"io"
	"unicode"
	"log"
	"os"
	"path/filepath"
	"bufio"	
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
	var size      int
	var rune_r    rune
	var rune_buff []rune = make([]rune,0)
	var file_in   *os.File
	var file_out  *os.File
	var reader    *bufio.Reader		
	numMapFiles = 0	

	// Open input file
	if file_in, err = os.Open(fileName); err != nil{
		fmt.Println("Problem to open file ", fileName)
		fmt.Println(err)
		return numMapFiles, err
	}

	// Open output file
	if file_out, err = os.Create(mapFileName(numMapFiles)); err != nil{
		fmt.Println("Problem to create file ", mapFileName(numMapFiles))
		fmt.Println(err)
		return numMapFiles, err	
	}

	reader = bufio.NewReader(file_in)
	for{
		
		// Reading all runes, one by one	
		if rune_r, size, err = reader.ReadRune(); err != nil{

			// Checking for error or EOF
			if err == io.EOF{
				break
			} else {
				fmt.Println("Problem reading rune")
				fmt.Println(err)
				return numMapFiles, err
			}
		}

		// If rune is part of a word, append it to buffer. This
		//must be the same check inside map function
		if(unicode.IsLetter(rune_r) || unicode.IsNumber(rune_r)){						
			rune_buff = append(rune_buff, rune_r)

		// If rune is not a member of a word, flush the buffer
		//and then write the new rune, they can be writed on 
		//different files
		} else {
			if 0 < len(rune_buff){
				file_out, numMapFiles, err = writeToFile(file_out, string(rune_buff), numMapFiles, chunkSize)
				if err != nil{
					fmt.Println("Problem to write to file")
					fmt.Println(err)
					return numMapFiles, err
				}
				rune_buff = make([]rune,0)
			}

			file_out, numMapFiles, err = writeToFile(file_out, string(rune_r), numMapFiles, chunkSize)
			if err != nil{
				fmt.Println("Problem to write to file")
				fmt.Println(err)
				return numMapFiles, err
			}
		}
	}
	
	// If there is information in the buffer, flush it
	if 0 < len(rune_buff){
		file_out, numMapFiles, err = writeToFile(file_out, string(rune_buff), numMapFiles, chunkSize)
		if err != nil{
			fmt.Println("Problem to write to file")
			fmt.Println(err)
			return numMapFiles, err
		}
		rune_buff = make([]rune,0)
	}

	// Close output file
	if err = file_out.Close(); err != nil{
		fmt.Println("Error to close file ", mapFileName(numMapFiles))
		fmt.Println(err)
		return numMapFiles, err
	}

	return numMapFiles, nil
}

// Append the string to the file if it has enough space, otherwise it close
//the file and open a new one and append the string. Is a requiremente
//that chunckSize is allways bigger than or equal to len(str)
func writeToFile(file_out *os.File, str string, numMapFiles int, chunkSize int)(*os.File, int, error){
	var n   int
	var err error
	var file_sts os.FileInfo

	if file_sts, err = file_out.Stat(); err != nil{
		fmt.Println("Error to read file stats, file ", mapFileName(numMapFiles))
		fmt.Println(err)
		return nil,0,err
	}

	if chunkSize < int(file_sts.Size()) + len(str){
		if err = file_out.Close(); err != nil{
			fmt.Println("Error to close file ", mapFileName(numMapFiles))
			fmt.Println(err)
			return nil, 0, err
		}

		if file_out, err = os.Create(mapFileName(numMapFiles+1)); err != nil{
			fmt.Println("Problem to create file ", mapFileName(numMapFiles+1))
			fmt.Println(err)
			return nil, 0, err	
		}

		numMapFiles++
	}

	if n, err = file_out.WriteString(str); err != nil{
		fmt.Println("Problem to write to file ", mapFileName(numMapFiles))
		fmt.Println(err)
		return nil, 0, err	
	}
	
	return file_out, numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}



	// n, err = file_in.Read(buff)
	// for  n > 0 && err == nil{

	// 	if file_out, err = os.Create(mapFileName(numMapFiles)); err != nil{
	// 		fmt.Println("Create - ",mapFileName(numMapFiles))
	// 		return numMapFiles, err
	// 	}
		
	// 	n, err = file_out.Write(buff[:n])						
	// 	if err = file_out.Close(); err != nil{
	// 		fmt.Println("Write - ",mapFileName(numMapFiles))
	// 		return numMapFiles, err
	// 	}

	// 	numMapFiles++
	// 	n, err = file_in.Read(buff)		
	// }	

	// if err == nil{
	// 	err = file_in.Close()
	// }else{
	// 	file_in.Close()
	// }	