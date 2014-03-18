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

func catBurst(frames []*dataframe.DataFrame) {
	burst := dataframe.BuildDataBurst(frames)
	b, err := dataframe.MarshalDataBurst(burst)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshalling burst: %v\n", err)
	}
	os.Stdout.Write(b)
}

func catFrame(frame *dataframe.DataFrame, json bool) {
	if !json {
		fmt.Println(*frame)
	} else {
		writer := io.Writer(os.Stdout)
		jsonWriter, err := framestore.NewJSONWriter(writer)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error JSON-encoding frame: %v\n\n%v\n", err, *frame)
		}
		jsonWriter.WriteFrame(frame)
	}
}

func catFromStdin(json, burstPacked bool) {
	var err error
	var buf bytes.Buffer
	if burstPacked {
		_, err = buf.ReadFrom(os.Stdin)
		if err != nil {
			fmt.Println(err)
		}
		burst, err := dataframe.UnmarshalDataBurst(buf.Bytes())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshalling databurst: %v\n", err)
		}
		for _, frame := range burst.Frames {
			catFrame(frame, json)
		}
	} else {
		reader := io.Reader(os.Stdin)
		packet, err := ioutil.ReadAll(reader)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		frame, err := dataframe.UnmarshalFrame(packet)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		catFrame(frame, json)
		if err != io.EOF {
			fmt.Fprintln(os.Stderr, err)
		}
	}
}

func main() {
	burstPacked := flag.Bool("burst", true, "Parse DataBurst-encapsulated frames rather than plain DataFrames")
	json := flag.Bool("json", false, "Output frames in JSON")
	pb := flag.Bool("pb", false, "Output frames as a DataBurst")

	flag.Usage = func() {
		helpMessage := "framecat will accept a stream of DataFrames on stdin and print a textual representation to stdout." +
			fmt.Sprintf("Usage: %s [options]\n\n", os.Args[0]) +
			"Options:\n\n"
		fmt.Fprintf(os.Stderr, helpMessage)
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		catFromStdin(*json, *burstPacked)
	} else {
		var buf bytes.Buffer
		frames := make([]*dataframe.DataFrame, 0)
		for _, fname := range flag.Args() {
			fi, err := os.Open(fname)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
				os.Exit(1)
			}
			_, err = buf.ReadFrom(fi)
			burst, err := dataframe.UnmarshalDataBurst(buf.Bytes())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error unmarshalling burst: %v\n", err)
				continue
			}
			for _, f := range burst.Frames {
				frames = append(frames, f)
			}
		}
		if *pb {
			catBurst(frames)
		} else {
			for _, f := range frames {
				catFrame(f, *json)
			}
		}
	}
}
