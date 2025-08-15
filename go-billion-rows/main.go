package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type StationStats struct {
	Min   int64
	Max   int64
	Sum   int64
	Count int64
}

var numWorkers = runtime.NumCPU()

func main() {
	filename := "data.txt"

	fmt.Println("Billion row challenge go version")
	fmt.Printf("Using %d CPU cores as workers\n", numWorkers)

	if len(os.Args) > 1 && os.Args[1] == "-generate" {

		generator := NewBillionRowGenerator()

		if err := generator.LoadStations("weather_stations.csv"); err != nil {
			log.Fatalf("Error loading stations: %v", err)
		}

		if _, err := os.Stat(filename); err == nil {
			fmt.Printf("File %s already exists. Overwrite? (y/N): ", filename)
			var response string
			fmt.Scanln(&response)
			if response != "y" && response != "Y" {
				fmt.Println("Generation cancelled")
				return
			}
		}

		totalStart := time.Now()
		if err := generator.Generate(filename); err != nil {
			log.Fatalf("Error generating data: %v", err)
		}
		totalDuration := time.Since(totalStart)

		fmt.Printf("\nGENERATION COMPLETE\n")
		fmt.Printf("Total time: %v\n", totalDuration)
		return
	}

	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		fmt.Printf("File %s does not exist. Generate data first with -generate\n", filename)
		log.Fatalf("file %s does not exist", filename)
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	start := time.Now()

	data, cleanup, err := mmapFile(file)
	if err != nil {
		log.Fatalf("Error memory-mapping file: %v", err)
	}
	defer cleanup()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Error getting file info: %v", err)
	}
	fileSize := fileInfo.Size()

	chunks := calculateChunks(data, fileSize)

	resultsChan := make(chan map[string]*StationStats, numWorkers)
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go processChunk(data, chunks[i], &wg, resultsChan)
	}

	wg.Wait()
	close(resultsChan)

	finalStats := make(map[string]*StationStats, 10000)
	for workerResult := range resultsChan {
		for station, stats := range workerResult {
			if existing, ok := finalStats[station]; ok {
				existing.Min = min(existing.Min, stats.Min)
				existing.Max = max(existing.Max, stats.Max)
				existing.Sum += stats.Sum
				existing.Count += stats.Count
			} else {
				finalStats[station] = stats
			}
		}
	}

	duration := time.Since(start)

	printResults(finalStats, duration, fileSize)
}

func calculateChunks(data []byte, fileSize int64) [][2]int64 {
	chunks := make([][2]int64, numWorkers)
	chunkSize := fileSize / int64(numWorkers)

	var currentPos int64 = 0
	for i := 0; i < numWorkers; i++ {
		start := currentPos
		end := start + chunkSize
		if end >= fileSize {
			end = fileSize
		} else {
			newlineIndex := bytes.IndexByte(data[end:], '\n')
			if newlineIndex != -1 {
				end += int64(newlineIndex) + 1
			} else {
				end = fileSize
			}
		}

		chunks[i] = [2]int64{start, end}
		currentPos = end
	}

	chunks[numWorkers-1][1] = fileSize

	return chunks
}

func processChunk(data []byte, chunk [2]int64, wg *sync.WaitGroup, resultsChan chan<- map[string]*StationStats) {
	defer wg.Done()

	stats := make(map[string]*StationStats, 10000)
	chunkData := data[chunk[0]:chunk[1]]

	var line []byte
	for len(chunkData) > 0 {
		idx := bytes.IndexByte(chunkData, '\n')
		if idx == -1 {
			line = chunkData
			chunkData = nil
		} else {
			line = chunkData[:idx]
			chunkData = chunkData[idx+1:]
		}

		if len(line) == 0 {
			continue
		}

		sepIdx := bytes.LastIndexByte(line, ';')
		if sepIdx == -1 {
			continue
		}

		stationName := string(line[:sepIdx])
		tempStr := line[sepIdx+1:]
		temp := parseTemp(tempStr)

		if s, ok := stats[stationName]; ok {
			s.Min = min(s.Min, temp)
			s.Max = max(s.Max, temp)
			s.Sum += temp
			s.Count++
		} else {
			stats[stationName] = &StationStats{
				Min:   temp,
				Max:   temp,
				Sum:   temp,
				Count: 1,
			}
		}
	}

	resultsChan <- stats
}

func parseTemp(s []byte) int64 {
	var val int64
	var negative bool

	if s[0] == '-' {
		negative = true
		s = s[1:]
	}

	val = int64(s[len(s)-1] - '0')
	s = s[:len(s)-2]

	place := int64(10)
	for i := len(s) - 1; i >= 0; i-- {
		val += int64(s[i]-'0') * place
		place *= 10
	}

	if negative {
		return -val
	}
	return val
}

func printResults(stats map[string]*StationStats, duration time.Duration, fileSize int64) {
	stationNames := make([]string, 0, len(stats))
	for name := range stats {
		stationNames = append(stationNames, name)
	}
	sort.Strings(stationNames)

	var buffer bytes.Buffer
	buffer.WriteString("{")
	for i, name := range stationNames {
		s := stats[name]
		avg := float64(s.Sum) / float64(s.Count) / 10.0
		buffer.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, float64(s.Min)/10.0, avg, float64(s.Max)/10.0))
		if i < len(stationNames)-1 {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString("}\n")

	fmt.Print(buffer.String())

	fmt.Printf("\nRESULTS\n")
	fmt.Printf("Total Time: %v\n", duration)
	rowsPerSecond := float64(1_000_000_000) / duration.Seconds()
	gbPerSecond := float64(fileSize) / (1024 * 1024 * 1024) / duration.Seconds()
	fmt.Printf("Speed: %.2f million rows/second\n", rowsPerSecond/1_000_000)
	fmt.Printf("I/O Rate: %.2f GB/second\n", gbPerSecond)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
