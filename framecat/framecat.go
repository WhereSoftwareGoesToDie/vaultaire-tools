package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/anchor/bletchley/dataframe"
	"github.com/anchor/bletchley/framestore"
	"io"
	"io/ioutil"
	"os"
)

func catFrame(frame *dataframe.DataFrame, json bool) {
	if !json {
		fmt.Println(*frame)
	} else {
		writer := io.Writer(os.Stdout)
		jsonWriter, err := framestore.NewJSONWriter(writer)
		if err != nil {
			fmt.Printf("Error JSON-encoding frame: %v\n\n%v\n", err, *frame)
		}
		jsonWriter.WriteFrame(frame)
	}
}

func main() {
	var err error
	var buf bytes.Buffer

	burstPacked := flag.Bool("burst", true, "Parse DataBurst-encapsulated frames rather than plain DataFrames")
	json := flag.Bool("json", false, "Output frames in JSON")

	flag.Usage = func() {
		helpMessage := "framefelid will accept a stream of DataFrames on stdin and print a textual representation to stdout." +
			fmt.Sprintf("Usage: %s [options]\n\n", os.Args[0]) +
			"Options:\n\n"
		fmt.Fprintf(os.Stderr, helpMessage)
		flag.PrintDefaults()
	}
	flag.Parse()
	if *burstPacked {
		_, err = buf.ReadFrom(os.Stdin)
		if err != nil {
			fmt.Println(err)
		}
		burst, err := dataframe.UnmarshalDataBurst(buf.Bytes())
		if err != nil {
			fmt.Println(err)
		}
		for _, frame := range burst.Frames {
			catFrame(frame, *json)
		}
	} else {
		reader := io.Reader(os.Stdin)
		packet, err := ioutil.ReadAll(reader)
		if err != nil {
			fmt.Println(err)
		}
		frame, err := dataframe.UnmarshalFrame(packet)
		if err != nil {
			fmt.Println(err)
		}
		catFrame(frame, *json)
		if err != io.EOF {
			fmt.Println(err)
		}
	}
}
