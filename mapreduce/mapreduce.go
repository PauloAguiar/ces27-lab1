package mapreduce

import (
	"log"
	"os"
)

// RunSequential will ensure that map and reduce function runs in
// a single-core linearly. The Task is passed from the calling package
// and should contains the definitions for all the required functions
// and parameters.
// Notice that this implementation will store data locally. In the distributed
// version of mapreduce it's common to store the data in the same worker that computed
// it and just pass a reference to reduce jobs so they can go grab it.
func RunSequential(task *Task) {
	var (
		mapCounter int = 0
		mapResult  []KeyValue
	)

	log.Print("Running RunSequential...")

	_ = os.Mkdir(REDUCE_PATH, os.ModeDir)

	for v := range task.InputChan {
		mapResult = task.Map(v)
		storeLocal(task, mapCounter, mapResult)
		mapCounter++
	}

	mergeLocal(task, mapCounter)

	for r := 0; r < task.NumReduceJobs; r++ {
		data := loadLocal(r)
		task.OutputChan <- task.Reduce(data)
	}

	close(task.OutputChan)
	return
}
