package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
)

type Task struct {
	Row  map[string]interface{}
	Line int
}

func readAndParseCSV(filePath string, tasks chan<- Task, estimatedTotalLines int, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		close(tasks)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)

	headers, err := reader.Read()
	if err != nil {
		fmt.Println("Error reading CSV headers:", err)
		close(tasks)
		return
	}

	bar := progressbar.Default(int64(estimatedTotalLines))

	lineNumber := 0
	for {
		record, err := reader.Read()
		lineNumber++
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Println("Error reading CSV record:", err)
			break
		}

		row := make(map[string]interface{})
		for i, value := range record {
			key := strings.ToLower(headers[i])
			row[key] = value
		}

		// Send the parsed row to the tasks channel
		tasks <- Task{Row: row, Line: lineNumber}

		bar.Add(1)
	}

	close(tasks)
	bar.Finish()
}

func worker(_ int, tasks <-chan Task, _ string, wg *sync.WaitGroup, result *sync.Mutex, encoder *json.Encoder) {
	defer wg.Done()

	for task := range tasks {
		// Acquire the result mutex before writing to the JSON file
		result.Lock()

		if err := encoder.Encode(task.Row); err != nil {
			fmt.Printf("Error writing JSON on line %d: %v\n", task.Line, err)
			result.Unlock()
			return
		}

		// Release the mutex
		result.Unlock()
	}
}

func main() {
	args := os.Args
	fileIndex := -1
	outputIndex := -1

	for i, arg := range args {
		if arg == "--file" && i+1 < len(args) {
			fileIndex = i + 1
		} else if arg == "--output" && i+1 < len(args) {
			outputIndex = i + 1
		}
	}

	if fileIndex != -1 {
		filePath := args[fileIndex]
		outputPath := ""
		if outputIndex != -1 {
			outputPath = args[outputIndex]
		}

		startTime := time.Now()

		fmt.Println("Reading file...")
		fmt.Println("=================")

		estimatedTotalLines, err := evaluateTotalLines(filePath)
		if err != nil {
			fmt.Println("Error evaluating total lines:", err)
			return
		}

		fmt.Printf("Estimated total lines: %d\n", estimatedTotalLines)

		tasks := make(chan Task)

		var wg sync.WaitGroup

		// Create a JSON file and an encoder
		outputFile, err := os.Create(outputPath)
		if err != nil {
			fmt.Println("Error creating JSON file:", err)
			return
		}
		defer outputFile.Close()

		encoder := json.NewEncoder(outputFile)
		encoder.SetIndent("", "  ")

		// Start multiple workers (e.g., 6 workers)
		workerCount := 6
		resultMutex := sync.Mutex{} // Mutex to protect the JSON file writing

		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go worker(i, tasks, outputPath, &wg, &resultMutex, encoder)
		}

		// Start a goroutine to read and parse the CSV file
		wg.Add(1)
		go readAndParseCSV(filePath, tasks, estimatedTotalLines, &wg)

		// Wait for all goroutines to finish
		wg.Wait()

		fmt.Println("Conversion complete!")
		endTime := time.Now()
		processTime := endTime.Sub(startTime).Seconds()
		fmt.Printf("File name: %s\n", filePath)
		fmt.Printf("Processing time: %.2f seconds\n", processTime)
	} else {
		fmt.Println("Please provide a file path using the --file argument.")
	}
}

func evaluateTotalLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	// Return lineCount - 1 to account for the header row
	return lineCount - 1, nil
}
