package main

import (
	"flag"
	"fmt"
	"github.com/anchor/bletchley/dataframe"
	"log"
	"os"
)

const (
	Version           = "0.1.0"
	DefaultFrameCount = 100
)

// Given the value of the split-files argument and the number of bursts
// we've written so far, return a file pointer to the current correct
// output stream.
func getCurrentOutputStream(splitFiles string, burstIndex int) (*os.File, error) {
	var err error
	fo := os.Stdout
	if splitFiles != "" {
		fName := fmt.Sprintf("%v.%02d", splitFiles, burstIndex)
		fo, err = os.Create(fName)
		if err != nil {
			return nil, err
		}
	}
	return fo, nil
}

func main() {
	frameCount := flag.Int("count", 100, "Number of frames to generate (if -burst is false, this is forced to 1).")
	burstPack := flag.Bool("burst", true, "Generate DataBursts rather than plain DataFrames.")
	burstLen := flag.Int("burst-len", 10, "Number of DataFrames per DataBurst (only used with -burst).")
	splitFiles := flag.String("split-files", "", "Write generated DataBursts to (<count>/<burst-len>) files, named numerically using the value of this argument as the prefix.")

	flag.Usage = func() {
		helpMessage := "framegen will generate random DataFrames for testing purposes. By default, it will write them to stdout.\n\n" +
			fmt.Sprintf("Usage: %s [options]\n\n", os.Args[0]) +
			"Options:\n\n"
		fmt.Fprintf(os.Stderr, helpMessage)
		flag.PrintDefaults()
	}
	flag.Parse()

	frameBatch := make([]*dataframe.DataFrame, *burstLen)
	burstCount := 0

	nFrames := *frameCount
	if !*burstPack {
		nFrames = 1
	}

	fileCount := 0
	for i := 0; i < nFrames; i++ {
		frame := dataframe.GenTestDataFrame()
		if !*burstPack {
			bytes, err := dataframe.MarshalDataFrame(frame)
			if err != nil {
				log.Printf("Error marshalling frame %v: %v\n", frame, err)
			} else {
				os.Stdout.Write(bytes)
			}
		} else {
			frameBatch[burstCount] = frame
			burstCount += 1
			if burstCount == *burstLen {
				fo, err := getCurrentOutputStream(*splitFiles, fileCount)
				if err != nil {
					log.Fatal(err)
				}
				burst := dataframe.BuildDataBurst(frameBatch)
				bytes, err := dataframe.MarshalDataBurst(burst)
				if err != nil {
					log.Printf("Error marshalling burst: %v\n", err)
				} else {
					fo.Write(bytes)
				}
				frameBatch = make([]*dataframe.DataFrame, *burstLen)
				burstCount = 0
				fileCount += 1
			}
		}
	}
}
