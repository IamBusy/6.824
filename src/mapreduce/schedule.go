package mapreduce

import (
	"fmt"
	"sync"
	"log"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var waiter sync.WaitGroup
	doneChan := make(chan int, 1)
	successChan := make(chan string, 5)
	failChan := make(chan string, 5)
	taskManager := NewTaskManager()

	// 预生成所有task
	for i:=0; i<ntasks; i++ {
		task := DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}
		taskManager.Push(task)
	}

	avaiWks := availableWorkers(doneChan, registerChan, successChan, failChan)
	for {
		if !taskManager.Empty() {
			select {
			case worker := <-avaiWks:
				waiter.Add(1)
				go submitTask(worker, taskManager.Pop().(DoTaskArgs), &successChan, &failChan, taskManager, &waiter)
				continue
			default:
				continue
			}
		}
		waiter.Wait()
		if taskManager.Empty() {
			log.Println("Schedule: finish all tasks successfully")
			doneChan<-1
			break
		}
	}
	fmt.Printf("Schedule: %v done\n", phase)
}

func availableWorkers(done chan int,
	register chan string,
	success chan string,
	fail chan string) chan string {
	wks := make(chan string, 5)
	go func() {
		for {
			select {
			case <-done:
				break
			case worker := <-register:
				wks<-worker
			case worker := <-success:
				wks<-worker
			//case worker := <-fail:
			//	wks<-worker
			}
		}
	}()
	return wks
}

func submitTask(worker string, task DoTaskArgs,
	doneWorkers *chan string,
	failWorkers *chan string,
	taskManager *TaskManager,
	waiter *sync.WaitGroup)  {

	log.Println("Schedule: submit task to worker ", worker)
	//waiter.Add(1)
	defer waiter.Done()
	isSuccess := call(worker, "Worker.DoTask", task, nil)
	log.Printf("Schedule: result of task submitted worker[%s] with task[%d] is %t ",
		worker, task.TaskNumber, isSuccess)
	if isSuccess {
		go func() {
			log.Println("Schedule: before send successful signal")
			*doneWorkers<-worker
			log.Println("Schedule: after send successful signal")
			//atomic.AddInt32(counter, -1)
			//waiter.Done()
		}()
	} else {
		go func() {
			taskManager.Push(task)
			*failWorkers<-worker
			//atomic.AddInt32(counter, -1)
			//waiter.Done()
		}()
	}

	//waiter.Done()
	log.Println("Schedule: worker finish task ", worker)
}
