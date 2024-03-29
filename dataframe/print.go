package dataframe

import (
	"fmt"
	"math"
	"strings"
)

// Formatted print method for dataframes
func (df *Dataframe) Print(n ...int) {
	// Variadic was used in the `n` argument in order to make the argument optional.

	if df.IsEmpty() {
		fmt.Println()
		return
	}

	numberOfRows := 5
	if len(n) > 0 {
		numberOfRows = n[0] // When input provided, always evaluated in zero index.
	}
	if numberOfRows > len(df.Rows) {
		numberOfRows = len(df.Rows)
	}

	maxCharsPerColumn := make(map[int]int)

	// Create data to built the Column and Column Types Lines
	var columnNames []interface{}
	var columnTypes []interface{}

	for idx, c := range df.Schema.Columns {
		columnNames = append(columnNames, c.Name)
		columnTypes = append(columnTypes, fmt.Sprintf("(%s)", c.Type))
		n, _ := columnNames[idx].(string)
		t, _ := columnTypes[idx].(string)
		maxCharsPerColumn[idx] = int(math.Max(float64(len(n)), float64(len(t))))
	}

	// Create data to built the Row lines
	var rowLines [][]interface{}
	for _, r := range df.Rows[:numberOfRows] {
		rowStringValues := make([]interface{}, 0)
		for idx, v := range r.Values {
			stringValue := fmt.Sprintf("%v", v)
			rowStringValues = append(rowStringValues, stringValue)
			maxCharsPerColumn[idx] = int(math.Max(float64(maxCharsPerColumn[idx]), float64(len(stringValue))))
		}
		rowLines = append(rowLines, rowStringValues)
	}

	var dashes []interface{}
	var printLines []string

	for idx := range df.Schema.Columns {
		n, _ := columnNames[idx].(string)
		t, _ := columnTypes[idx].(string)
		columnNames[idx] = centerAlighn(n, maxCharsPerColumn[idx])
		columnTypes[idx] = centerAlighn(t, maxCharsPerColumn[idx])
		dashes = append(dashes, strings.Repeat("-", maxCharsPerColumn[idx]))
	}

	columnNamesLineStr := fmt.Sprintf(strings.Repeat(" %v |", len(columnNames)), columnNames...)
	columnTypesLineStr := fmt.Sprintf(strings.Repeat(" %v |", len(columnTypes)), columnTypes...)
	printLines = append(printLines, columnNamesLineStr[:len(columnNamesLineStr)-1])
	printLines = append(printLines, columnTypesLineStr[:len(columnTypesLineStr)-1])

	for rIdx, row := range rowLines {
		for vIdx, value := range row {
			vStr, _ := value.(string)
			rowLines[rIdx][vIdx] = leftAlighn(vStr, maxCharsPerColumn[vIdx])
		}

		dashesLineStr := fmt.Sprintf(strings.Repeat("-%v-+", len(dashes)), dashes...)
		rowLineStr := fmt.Sprintf(strings.Repeat(" %v |", len(rowLines[rIdx])), rowLines[rIdx]...)
		printLines = append(printLines, dashesLineStr[:len(dashesLineStr)-1])
		printLines = append(printLines, rowLineStr[:len(rowLineStr)-1])
	}

	fmt.Println()
	for _, l := range printLines {
		fmt.Println(l)
	}

}

func centerAlighn(text string, outputLength int) string {

	textLength := len(text)
	if textLength >= outputLength {
		return text
	}

	totalBufferChars := outputLength - textLength
	leftBufferLength := int(totalBufferChars / 2)
	rightBufferLength := outputLength - textLength - leftBufferLength

	return strings.Repeat(" ", leftBufferLength) + text + strings.Repeat(" ", rightBufferLength)

}

func leftAlighn(text string, outputLength int) string {

	lengthStringFormat := fmt.Sprintf("%%%d", outputLength)
	leftAlighnFormat := fmt.Sprintf("%ss", lengthStringFormat)
	return fmt.Sprintf(leftAlighnFormat, text)

}
