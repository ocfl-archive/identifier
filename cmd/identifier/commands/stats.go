package commands

import (
	"fmt"
	"os"
	"regexp"

	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/ocfl-archive/identifier/identifier"
)

// StatRecord represents aggregate metrics (count and total bytes) for a grouping key.
type StatRecord struct {
	Key   string
	Count int64
	Size  int64
}

// LogFilterSummary prints standardized console logs summarizing active file filtering options.
func LogFilterSummary(empty, duplicates bool, regexPattern string, prefix string) {
	if regexPattern != "" {
		fmt.Printf("#including regexp \"%s\"\n", regexPattern)
	}
	if empty {
		fmt.Println("#including empty files")
	}
	if duplicates {
		fmt.Println("#including duplicate files")
	}
	if prefix != "" {
		fmt.Printf("#including prefix \"%s\"\n", prefix)
	}
	if !empty && !duplicates && regexPattern == "" {
		fmt.Println("#including all files")
	}
}

// CompileRegexOrExit compiles a regular expression string or logs an error and exits if invalid.
func CompileRegexOrExit(pattern string) *regexp.Regexp {
	if pattern == "" {
		return nil
	}
	r, err := regexp.Compile(pattern)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot compile regular expression '%s'", pattern)
		os.Exit(1)
	}
	return r
}

// AggregateFileData scans the Badger database index using a filter and aggregates count and total byte size by key.
func AggregateFileData(
	badgerIterator *identifier.BadgerIterator,
	prefix string,
	filter identifier.Predicate[identifier.FileData],
	keySelector func(fData *identifier.FileData) string,
) (map[string]*StatRecord, error) {
	stats := make(map[string]*StatRecord)
	err := badgerIterator.IterateIndex(prefix, func(fData *identifier.FileData) (remove bool, err error) {
		if !filter(fData) {
			return false, nil
		}
		key := keySelector(fData)
		entry, exists := stats[key]
		if !exists {
			entry = &StatRecord{Key: key}
			stats[key] = entry
		}
		entry.Size += fData.Size
		entry.Count++
		return false, nil
	})
	return stats, err
}

// OutputStats formats and outputs aggregated statistics across all configured targets (table, CSV, JSONL, XLSX, Console).
func OutputStats[T any](
	stats map[string]*StatRecord,
	title string,
	fields []string,
	output *identifier.Output,
	console bool,
	jsonObjBuilder func(rec *StatRecord) T,
) {
	tw := table.NewWriter()
	header := table.Row{}
	for _, field := range fields {
		header = append(header, field)
	}
	tw.AppendHeader(header)

	for _, rec := range stats {
		row := []any{
			rec.Key,
			rec.Count,
			rec.Size,
			humanize.Bytes(uint64(rec.Size)),
		}
		if err := output.Write(row, jsonObjBuilder(rec)); err != nil {
			logger.Error().Err(err).Msg("cannot write output")
		}
		tw.AppendRow(table.Row{rec.Key, rec.Count, rec.Size, humanize.Bytes(uint64(rec.Size))})
	}
	tw.SetTitle(title)
	if console {
		fmt.Println(tw.Render())
	}
}
