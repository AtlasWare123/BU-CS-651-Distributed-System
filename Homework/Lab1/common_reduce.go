package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	/*
		mr.run函数来执行具体的运行任务，其中重点是schedule(mapPhase)和schedule(reducePhase)，这里schedule函数是调用时的1个匿名函数，主要功能是区分任务类型，对于map任务执行doMap函数，对于reduce任务执行doReduce函数，紧接着是调用mr.merge函数来整合nReduce个结果文件成1个最终文件，最后向doneChannel信道中写入true标识这次任务的结束。
	*/
	output_map := make(map[string][]string)
	// deal with input files first, iterate through each map task
	for i := 0; i < nMap; i++ {
		inputName := reduceName(jobName, i, reduceTaskNumber)
		inputFile, _ := os.Open(inputName)
		inputFile_decode := json.NewDecoder(inputFile) // creating a
		//decoder for input files
		var kvs []KeyValue // variable consists of multiple KeyValues
		inputFile_decode.Decode(&kvs)
		// decoding results are stored in kvs array of KeyValue
		inputFile.Close()
		for _, key := range kvs {
			output_map[key.Key] = append(output_map[key.Key], key.Value)
		}
		//inputFile.Close()
	}

	// Then deal with the ouputFile
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	outputName := mergeName(jobName, reduceTaskNumber)
	outputFile, err := os.Open(outputName)
	if os.IsNotExist(err) {
		outputFile, _ = os.Create(outputName)
	}
	outputFile_encode := json.NewEncoder(outputFile)
	// creating an encoder for outputs
	for key, values := range output_map {
		err = outputFile_encode.Encode(KeyValue{key, reduceF(key, values)})
		if err != nil {
			fmt.Println("Reducing Encounting error:", err)
		}
	}
	outputFile.Close()
}
