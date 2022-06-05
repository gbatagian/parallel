package dataframe

import (
	"fmt"
	"strings"
)

// Formatted print method for dataframes
func (df *Dataframe) Print(n ...int) {

	// Variadic was used in the `n` argument in order to make the argument optional.
	numberOfRows := 5
	if len(n) > 0 {
		numberOfRows = n[0] // When input provided, always evaluated in zero index.
	}
	if numberOfRows > len(df.Rows) {
		numberOfRows = len(df.Rows)
	}

	maxCharsPerColumn := make(map[int]int)

	// Create data to built for Column and Column Types Lines
	var columnNames []interface{}
	var columnTypes []interface{}

	for idx, c := range df.Schema.Columns {
		columnNames = append(columnNames, c.Name)
		columnTypes = append(columnTypes, fmt.Sprintf("(%s)", c.Type))
		n, _ := columnNames[idx].(string)
		t, _ := columnTypes[idx].(string)
		maxCharsPerColumn[idx] = maxInt(len(n), len(t))
	}

	// Create data to built the Row lines
	var rowLines [][]interface{}
	for _, r := range df.Rows[:numberOfRows] {
		rowStringValues := make([]interface{}, 0)
		for idx, v := range r.Values {
			stringValue := fmt.Sprintf("%v", v)
			rowStringValues = append(rowStringValues, stringValue)
			maxCharsPerColumn[idx] = maxInt(maxCharsPerColumn[idx], len(stringValue))
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

	for r_idx, row := range rowLines {
		for v_idx, value := range row {
			v_s, _ := value.(string)
			rowLines[r_idx][v_idx] = leftAlighn(v_s, maxCharsPerColumn[v_idx])
		}

		dashesLineStr := fmt.Sprintf(strings.Repeat("-%v-+", len(dashes)), dashes...)
		rowLineStr := fmt.Sprintf(strings.Repeat(" %v |", len(rowLines[r_idx])), rowLines[r_idx]...)
		printLines = append(printLines, dashesLineStr[:len(dashesLineStr)-1])
		printLines = append(printLines, rowLineStr[:len(rowLineStr)-1])
	}

	fmt.Println()
	for _, l := range printLines {
		fmt.Println(l)
	}

}

func maxInt(int1, int2 int) int {
	if int1 > int2 {
		return int1
	}
	return int2
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
