package dataframe

type Dataframe struct {
	Rows   []Row
	Schema Schema
}

func createDataframeWithNoSchemaInfo(rows [][]interface{}) Dataframe {

	df := Dataframe{}
	for _, r := range rows {
		df.Rows = append(df.Rows, CreateRow(r))
	}
	df.Schema = df.Rows[0].Schema

	return df

}

func createDataframeWithSchemaInfo(rows [][]interface{}, i interface{}) Dataframe {

	df := Dataframe{}

	// Case 1: The provided info is the column names.
	if _, ok := i.([]string); ok {
		for _, r := range rows {
			df.Rows = append(df.Rows, CreateRow(r))
		}
	}

	return df

}

func CreateDatafeme(rows [][]interface{}, i ...interface{}) Dataframe {

	// Case1: No schema related information was provided in dataframe definition
	if len(i) == 0 {
		createDataframeWithNoSchemaInfo(rows)
	}

	return createDataframeWithSchemaInfo(rows, i)
}
