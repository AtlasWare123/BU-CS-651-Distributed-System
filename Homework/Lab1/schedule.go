package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	/*
		这里就要实现分布式的Map/Reduce。我们先前的实现是串行的，即一个个地执行map和reduce任务，比较简单但是性
		能不高。这部分我们要利用多核，将任务分发给多个worker线程。虽然这不是真实环境中的多机分布式，但
		是我们将使用RPC和channel来模拟分布式计算。     　　为了协同任务的并行执行，我们将使用1个特殊的master线程，来分发任务到worker线程
		并等待它们完成。实验中提供了worker的实现代码和启动代码(mapreduce/worker.go)以及RPC消息处理的代码(mapreduce/common_rpc.go)。
		我们的任务实现mapreduce包中的schedule.go文件，尤其是其中的schedule函数来分发map和reduce任务到worker，并当它们完成后才返回。
		先前我们分析过mr.run函数[master.go]，里面会         调用schedule函数来运行map和reduce任务，然后调用merge函数
		来将每个reduce任务的结果文件整合成1个最终文件。schedule函数只需要告诉worker输入文件的文件名(mr.files[task])和任务号。m
		ast         er节点通过RPC调用Worker.DoTask，传递1个DoTaskArgs对象作为RPC的参数来告诉worker新的任务。
		当1个worker启动时，它会发送1个注册RPC给master，传递新worker的信息到mr.registerChannel。我们的schedule函数通
		过读取mr.registerChannel来获得可用的worker。
		这里可以读一下hint中go的RPC文档RPC和并发文档concurrency，来了解一下RPC的实现和如何保护并发处理。

		主要过程是先区分一下这是map任务还是reduce任务，对于map任务，任务数ntask为输入文件的个数，io数nios为reduce
		worker的数目nReduce，对于reduce任务，任务数ntask为reduce
		worker的数目nReduce，io数nios为map worker的数目即输入文件的个数。然后创建1个同步包sync中的等待组WaitG
		roup，对于每个任务，将其加入到等待组中，并运行1个goroutine来运行进行分发任务。首先从mr.registerChannel中获得
		1个可用的worker，构建DoTaskArgs对象，作为参数调用worker的Worker.DoTask来执行任务，当其完成任务后将其重新
		加入到mr.registerChannel表示可用。最后使用WaitGroup的wait函数等待所有任务完成。因为只有当map任务都完成后才
		能执行reduce任务。
	*/
	// call() sends an RPC to the rpcname handler on server srv
	// with arguments args, waits for the reply, and leaves the
	// reply in reply. the reply argument should be the address
	// of a reply structure.
	//
	// call() returns true if the server responded, and false
	// if call() was not able to contact the server. in particular,
	// reply's contents are valid if and only if call() returned true.
	//
	// you should assume that call() will time out and return an
	// error after a while if it doesn't get a reply from the server.
	//
	// please use call() to send all RPCs, in master.go, mapreduce.go,
	// and worker.go.  please don't change this function.
	//
	// DoTaskArgs holds the arguments that are passed to a worker when a job is
	// scheduled on it.
	/*
							type DoTaskArgs struct {
							JobName    string
							File       string   // the file to process
							Phase      jobPhase // are we in mapPhase or reducePhase?
							TaskNumber int      // this task's index in the current phase

							// NumOtherPhase is the total number of tasks in other phase; mappers
							// need this to compute the number of output bins, and reducers needs
							// this to know how many input files to collect.
							NumOtherPhase int
						}

						type Master struct {
					sync.Mutex

					address         string
					registerChannel chan string
					doneChannel     chan bool
					workers         []string // protected by the mutex

					// Per-task information
					jobName string   // Name of currently executing job
					files   []string // Input files
					nReduce int      // Number of reduce partitions

					shutdown chan struct{}
					l        net.Listener
					stats    []int
				}

				RPC的失败并不是表示worker的宕机，worker可能只是网络不可达，仍然在工作计算。所以如果重新分配任务可能造成2个worker接受相同的任务并计算。但是这没关系，
				因为相同的任务生成相同的结果。我们只要实现重新分配任务即可。
		　　这1点体现在上面schedule函数的第2个无限for循环中，当RPC的call失败时，仅仅就是重新选取1个worker，只有当成功时，才会break。
	*/
	var wg sync.WaitGroup // set up a waiting group
	for i := 0; i < ntasks; i++ {
		wg.Add(1)                                                    // add the task into the waitingGroup
		go func(taskNumber int, numOtherPhase int, Phase jobPhase) { // execute a goroutine to distribute task
			defer wg.Done()
			for { // for {} means while (true)
				worker := <-mr.registerChannel
				var args DoTaskArgs
				args.NumOtherPhase = numOtherPhase
				args.Phase = Phase
				args.TaskNumber = taskNumber
				args.File = mr.files[taskNumber]
				args.JobName = mr.jobName

				// use call function
				state := call(worker, "Worker.DoTask", &args, new(struct{})) //func call(srv string, rpcname string,args interface{}, reply interface{}) bool
				if state {
					go func() {
						mr.registerChannel <- worker
					}()
					break
				}

			}
		}(i, nios, phase)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
