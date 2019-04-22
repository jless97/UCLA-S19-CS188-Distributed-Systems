package main

import (
	"bufio"
	"fmt"
	//"log"
	"os"
	"strings"
)

func main() {
	//  Define a map
	countsChan := make(chan map[string]int)

	for _, filename := range os.Args[1:] {
		go scan(filename, countsChan)
	}

	aggCounts := make(map[string]int)
	for i := 1; i < len(os.Args[1:]); i++ {
		// var partial_counts map[string]int = <- countsChan
		// for word, count := range partial_counts {
		// 	aggCounts[word] += count
		// }

		//var partial_counts map[string]int = <- countsChan
		for word, count := range <-countsChan {
			aggCounts[word] += count
		}
	}

	for w, c := range aggCounts {
		fmt.Println(w, c)
	}

	fmt.Println("Done!")
}

func scan(filename string, countsChan chan map[string]int) error {
	counts := make(map[string]int)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	//  Push to stack, execute surrounding function, then call Close()
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Fields(line)

		for _, word := range words {
			counts[word]++
		}
	}

	countsChan <- counts
	return scanner.Err()
}
