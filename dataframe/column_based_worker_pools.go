package dataframe

import (
	"parallel/core"
)

type operation func(df *Dataframe, columnNames ...string) interface{}

type OperationJob struct {
	df        *Dataframe
	columns   []string
	operation operation
}

type WorkerJob struct {
	df        Dataframe
	columns   []string
	operation operation
}

func operationWorker(jobs <-chan WorkerJob, resluts chan<- interface{}) {

	for opJ := range jobs {
		resluts <- opJ.operation(&opJ.df, opJ.columns...)
	}

}

func Pool(opJ OperationJob) []interface{} {

	// Split the dataframe into available number of workers parts
	nWorkers := core.NumWorkers
	dfPackets := opJ.df.Split(nWorkers)

	// Initialise workers' channels
	jobs := make(chan WorkerJob, len(dfPackets))
	results := make(chan interface{}, len(dfPackets))

	// Setup workers
	for i := 1; i <= nWorkers; i++ {
		go operationWorker(jobs, results)
	}

	// Send jobs
	for _, packetDf := range dfPackets {
		job := WorkerJob{
			df:        packetDf,
			columns:   opJ.columns,
			operation: opJ.operation,
		}
		jobs <- job
	}
	close(jobs)

	// Collect results
	var resultPackets []interface{}
	for i := 1; i <= len(dfPackets); i++ {
		resultPackets = append(resultPackets, <-results)
	}

	return resultPackets

}
