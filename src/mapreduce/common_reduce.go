package mapreduce

import (
	"io/ioutil"
	"encoding/json"
	"sort"
	"log"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	log.Println("Reducer: start reducing")

	var sortedKeyValues = map[string][]string{}
	for i:=0; i<nMap ; i++  {
		readBytes, err := ioutil.ReadFile(reduceName(jobName, i, reduceTask))
		if err != nil {
			log.Fatal("Read internal file error: ", err)
		}
		var kvs []KeyValue
		json.Unmarshal(readBytes, &kvs)
		for _, kv := range kvs {
			if values, ok := sortedKeyValues[kv.Key]; ok {
				sortedKeyValues[kv.Key] = append(values, kv.Value)
			} else {
				sortedKeyValues[kv.Key] = []string{kv.Value}
			}
		}
	}

	fp, err := os.OpenFile(outFile, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0777)
	defer fp.Close()

	if err != nil {
		log.Fatalln("Reducer: open output file err, ", err)
	}
	out := json.NewEncoder(fp)

	result := map[string]string{}
	for k, vs := range sortedKeyValues {
		sort.Strings(vs)
		result[k] = reduceF(k, vs)
		err := out.Encode(KeyValue{k, reduceF(k, vs)})
		if err != nil {
			log.Fatalln("Reducer: encode err, ", err)
		}
	}

	log.Println("Reducer: finish reduce")

}
