package formats

import (
	"bufio"
	"fmt"
	"os"
)

func WriteCSVChunks(header string, chunk <-chan []byte, targetFilename string) error {
    f, err := os.Create(targetFilename)
    if err != nil { return fmt.Errorf("create %s: %w", targetFilename, err) }
    defer f.Close()

    bw := bufio.NewWriterSize(f, 16*1024*1024) // 16MB
    defer bw.Flush()

    if _, err := bw.WriteString(header); err != nil { return err }
    if err := bw.WriteByte('\n'); err != nil { return err }

    var totalBytes int64
    var recordCount int64
    for buf := range chunk {
        if len(buf) == 0 { continue }
        if _, err := bw.Write(buf); err != nil { return err }
        totalBytes += int64(len(buf))
        // Count records by counting newlines
        for _, b := range buf {
            if b == '\n' {
                recordCount++
            }
        }
    }
    fmt.Printf("Successfully wrote %d records to %s\n", recordCount, targetFilename)
    return nil
}