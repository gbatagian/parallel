package dataframe

import (
	"parallel/core"
)

type operation func(df *Dataframe, columnNames ...string) interface{}

type ConcurrentOperationCore struct {
	df        *Dataframe
	columns   []string
	operation operation
}

type WorkerJob struct {
	df  Dataframe
	cnc *ConcurrentOperationCore
}

func operationWorker(jobs <-chan WorkerJob, resluts chan<- interface{}) {

	for job := range jobs {
		resluts <- job.cnc.operation(&job.df, job.cnc.columns...)
	}

}

func Pool(cnc ConcurrentOperationCore) []interface{} {

	// Split the dataframe into available number of workers parts
	nWorkers := core.NumWorkers
	dfPackets := cnc.df.Split(nWorkers)

	// Initialise workers' channels
	jobs := make(chan WorkerJob, len(dfPackets))
	results := make(chan interface{}, len(dfPackets))

	// Setup workers
	for i := 1; i <= nWorkers; i++ {
		go operationWorker(jobs, results)
	}

	// Send jobs
	for _, packetDf := range dfPackets {
		job := WorkerJob{df: packetDf, cnc: &cnc}
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
