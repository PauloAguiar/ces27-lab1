package main

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"bytes"
	// "unsafe"
)

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

	// References:
	// https://stackoverflow.com/questions/36111777/golang-how-to-read-a-text-file

	numMapFiles = 0
	
	var buffer bytes.Buffer
	
	b, err := ioutil.ReadFile(fileName) // just pass the file name
	if err != nil {
		fmt.Print(err)
	}

	//fmt.Println(b) // print the content as 'bytes'
	buffer.Reset()
	str := string(b) // convert content to a 'string'
	words := strings.Fields(str)
	var chuncks []string
	for i := 0; i < len(words); i++ {
		 if (len(buffer.String()) + len(words[i]) + len(" ")) < chunkSize {
			buffer.WriteString(" ")
			buffer.WriteString(words[i])
			fmt.Println(len(buffer.String()))
		 } else {
		 	chuncks = append(chuncks,buffer.String())
		 	buffer.Reset()
		 	buffer.WriteString(words[i])
		 }
	}
	fmt.Println(len(chuncks),len(chuncks[0]),chuncks[0])

	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join("map_test/", fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join("result_path/", fmt.Sprintf("result-%v", id))
}

func main() {
	splitData("files/blah.txt", 1024)
}