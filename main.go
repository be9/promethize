package main

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/pkg/timestamp"
	tsdbLabels "github.com/prometheus/tsdb/labels"

	"github.com/prometheus/tsdb"
	"gopkg.in/alecthomas/kingpin.v2"
	"io"
	"io/ioutil"
	"os"
	"time"
	"unsafe"
)

var (
	initialize    = kingpin.Command("init", "Initialize the database.")
	initPath      = initialize.Arg("path", "Path to the database").Required().String()
	initRetention = initialize.Flag("retention", "Days of retention").Default("15").Uint64()

	load             = kingpin.Command("load", "Load data to the database.")
	loadPath         = load.Arg("path", "Path to the database").Required().String()
	loadDataFilename = load.Arg("datafile", "Data filename").Required().String()
)

func main() {
	switch kingpin.Parse() {
	case "init":
		initDb(*initPath, *initRetention)
	case "load":
		content, err := ioutil.ReadFile(*loadDataFilename)
		if err != nil {
			exitWithError(err)
		}

		loadFileIntoDb(*loadPath, content)
	}
}

func initDb(path string, retention uint64) {
	if err := os.RemoveAll(path); err != nil {
		exitWithError(err)
	}
	if err := os.MkdirAll(path, 0777); err != nil {
		exitWithError(err)
	}

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l = log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	_, err := tsdb.Open(path, l, nil, &tsdb.Options{
		WALFlushInterval:  1 * time.Second,
		RetentionDuration: retention * 24 * 60 * 60 * 1000, // retention days in milliseconds
		BlockRanges:       tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
	})
	if err != nil {
		exitWithError(err)
	}
}

func loadFileIntoDb(dbPath string, content []byte) {
	var (
		p = textparse.New(content, "application/octet-stream")

		defTime = timestamp.FromTime(time.Now())
	)

	db, err := tsdb.Open(dbPath, nil, nil, nil)
	if err != nil {
		exitWithError(err)
	}

	app := db.Appender()
	cache := make(map[string]uint64)

	for {
		var et textparse.Entry
		var err error

		if et, err = p.Next(); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if et != textparse.EntrySeries {
			continue
		}

		t := defTime
		met, tp, v := p.Series()
		if tp != nil {
			t = *tp
		}

		if ref, ok := cache[yoloString(met)]; ok {
			err = app.AddFast(ref, t, v)
			if err != nil {
				println(err)
			}
		} else {
			var lset labels.Labels
			_ = p.Metric(&lset)

			ref, err = app.Add(toTSDBLabels(lset), t, v)
			if err != nil {
				println(err)
			} else {
				cache[yoloString(met)] = ref
			}
		}
	}

	if err := app.Commit(); err != nil {
		exitWithError(err)
	}
}

func toTSDBLabels(l labels.Labels) tsdbLabels.Labels {
	return *(*tsdbLabels.Labels)(unsafe.Pointer(&l))
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
