package identifier

import (
	"emperror.dev/errors"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/tealeg/xlsx/v3"
	"os"
	"time"
)

func NewOutput(console bool, csvPath string, jsonlPath string, xlsxPath string, name string, fields []string, logger zLogger.ZLogger) (*Output, error) {
	var err error
	output := &Output{console: console, fields: fields}
	if csvPath != "" {
		if output.csvFile, err = os.Create(csvPath); err != nil {
			output.Close()
			return nil, errors.Wrapf(err, "cannot create csv file '%s'", csvPath)
		}
		output.csvWriter = csv.NewWriter(output.csvFile)
		output.csvWriter.Write(fields)
	}
	if jsonlPath != "" {
		if output.jsonlFile, err = os.Create(jsonlPath); err != nil {
			output.Close()
			return nil, errors.Wrapf(err, "cannot create jsonl file '%s'", jsonlPath)
		}
	}
	if xlsxPath != "" {
		// check, whether a file can be created
		xlsxFile, err := os.Create(xlsxPath)
		if err != nil {
			output.Close()
			return nil, errors.Wrapf(err, "cannot create xlsx file '%s'", xlsxPath)
		}
		if err := xlsxFile.Close(); err != nil {
			output.Close()
			return nil, errors.Wrapf(err, "cannot close xlsx file '%s'", xlsxPath)
		}
		if err := os.Remove(xlsxPath); err != nil {
			output.Close()
			return nil, errors.Wrapf(err, "cannot remove xlsx file '%s'", xlsxPath)
		}
		output.xlsxFilename = xlsxPath
		output.xlsxWriter = xlsx.NewFile()
		output.sheet, err = output.xlsxWriter.AddSheet(name)
		row, err := output.sheet.AddRowAtIndex(0)
		if err != nil {
			output.Close()
			return nil, errors.Wrapf(err, "cannot add header row to xlsx file '%s'", xlsxPath)
		}
		for _, field := range fields {
			cell := row.AddCell()
			cell.SetString(field)
			style := xlsx.NewStyle()
			style.Alignment.Horizontal = "center"
			style.Fill.BgColor = "0A0A0A00"
			style.Border = *xlsx.NewBorder("thin", "thin", "thin", "thick")
			cell.SetStyle(style)
		}
	}
	return output, nil
}

type Output struct {
	csvFile      *os.File
	csvWriter    *csv.Writer
	jsonlFile    *os.File
	xlsxFilename string
	xlsxWriter   *xlsx.File
	sheet        *xlsx.Sheet
	console      bool
	fields       []string
}

func (o *Output) Close() error {
	var errs = []error{}
	if o.csvWriter != nil {
		o.csvWriter.Flush()
	}
	if o.csvFile != nil {
		if err := o.csvFile.Close(); err != nil {
			errs = append(errs, errors.Wrap(err, "cannot close csv file"))
		}
	}
	if o.jsonlFile != nil {
		if err := o.jsonlFile.Close(); err != nil {
			errs = append(errs, errors.Wrap(err, "cannot close jsonl file"))
		}
	}
	if o.xlsxWriter != nil {
		if err := o.xlsxWriter.Save(o.xlsxFilename); err != nil {
			errs = append(errs, errors.Wrap(err, "cannot save xlsx file"))
		}
	}
	return errors.Combine(errs...)
}

func (o *Output) WriteCSV(record []any) error {
	if o.csvWriter != nil {
		strs := make([]string, len(record))
		for key, val := range record {
			strs[key] = fmt.Sprintf("%v", val)
		}
		return errors.WithStack(o.csvWriter.Write(strs))
	}
	return nil
}

func (o *Output) WriteJSONL(data any) error {
	if o.jsonlFile != nil {
		return errors.WithStack(json.NewEncoder(o.jsonlFile).Encode(data))
	}
	return nil
}

func (o *Output) WriteXLSX(record []any) error {
	if o.sheet != nil {
		row := o.sheet.AddRow()
		for _, field := range record {
			cell := row.AddCell()
			switch field.(type) {
			case string:
				cell.SetString(field.(string))
			case int:
				cell.SetInt(field.(int))
			case int64:
				cell.SetInt64(field.(int64))
			case float64:
				cell.SetFloat(field.(float64))
			case time.Time:
				cell.SetDateTime(field.(time.Time))
			default:
				cell.SetString(fmt.Sprintf("%v", field))
			}
		}
	}
	return nil
}

func (o *Output) WriteConsole(record []any) error {
	if o.console {
		if len(o.fields) != len(record) {
			return errors.Errorf("fields and record length do not match: %v != %v", o.fields, record)
		}
		for key, field := range record {
			fmt.Printf("%s: %v // ", o.fields[key], field)
		}
		fmt.Println()
	}
	return nil
}

func (o *Output) Write(record []any, data any) error {
	var errs = []error{}
	if err := o.WriteCSV(record); err != nil {
		errs = append(errs, errors.Wrap(err, "cannot write csv"))
	}
	if err := o.WriteJSONL(data); err != nil {
		errs = append(errs, errors.Wrap(err, "cannot write jsonl"))
	}
	if err := o.WriteXLSX(record); err != nil {
		errs = append(errs, errors.Wrap(err, "cannot write xlsx"))
	}
	if err := o.WriteConsole(record); err != nil {
		errs = append(errs, errors.Wrap(err, "cannot write console"))
	}
	return errors.Combine(errs...)
}
