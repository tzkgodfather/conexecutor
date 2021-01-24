package conexecutor

import (
	"context"
	"errors"
	"sync"
)

var (
	//ErrFlowNotFound = errors.New("Flow not found\n")
	ErrNotRunning   = errors.New("ConExecutor not running\n")
	ErrJobNoHandle    = errors.New("Job with no handle function\n")
)

type JobOptions struct{
	Name string
	Params  map[string]string
	Ctx context.Context
}

func newJobOptions(jobOption ...JobOption) *JobOptions{
	no := new(JobOptions)
	no.Params = make(map[string]string)

	for _,o := range jobOption{
		o(no)
	}

	return no
}

type JobOption func(*JobOptions)
type JobFunc func(in interface{}, out interface{}, o *JobOptions) error

func WithJobParam(key string, value string) JobOption{
	return func(o * JobOptions){
		if nil == o.Params{
			o.Params = make(map[string] string)
		}
		o.Params[key] = value
	}
}

func WithJobName(name string) JobOption{
	return func(o * JobOptions){
		o.Name = name
	}
}

func WithJobContext(ctx context.Context) JobOption {
	return func(o *JobOptions) {
		o.Ctx = ctx
	}
}

type JobInfo struct {
	Options  *JobOptions // Job参数
	//callOpts []JobOption
	in interface{} //输入对象，业务的实际信息
	out interface{} //输出对象
	fJob JobFunc	//实际调用的函数
}

func newJobInfo(fJob JobFunc, in interface{}, out interface{}, jobOption ...JobOption) *JobInfo{
	j := new(JobInfo)
	j.Options = newJobOptions(jobOption...)
	j.fJob = fJob
	j.in = in
	j.out = out

	return j
}

func (job *JobInfo) Execute() error{
	if nil != job.fJob{
		return job.fJob(job.in, job.out, job.Options)
	}
	return ErrJobNoHandle
}

type ConExecutorStatus int32

const (
	ECeStatusInit      ConExecutorStatus = 0
	ECeStatusWaitStart ConExecutorStatus = 1
	ECeStatusRunning   ConExecutorStatus = 2
	ECeStatusWaitStop  ConExecutorStatus = 3
	ECeStatusStop      ConExecutorStatus = 4
)

//并行执行器的Options
type CeOptions struct{

	concurrency int
}
type CeOption func(* CeOptions)

func newCeOptions(optList ...CeOption) *CeOptions{
	co := new(CeOptions)
	co.concurrency = 10 //默认10
	for _,o := range optList{
		o(co)
	}

	return co
}

func WithCeConcurrency(con int) CeOption{
	return func(o *CeOptions){
		if 0 < con {
			o.concurrency = con
		}
	}
}

//并行执行器
type ConExecutor struct {
	eRunStatus ConExecutorStatus

	chanClose chan int
	chanJob   chan *JobInfo
	wgJob     *sync.WaitGroup //group for job

	chanCon   chan struct{} //用于限制并发数量
	opts *CeOptions
}

func NewConExecutor(optList ...CeOption) *ConExecutor{
	ce := new(ConExecutor)
	ce.opts = newCeOptions(optList...)
	//log.Printf("NewConExecutor opt[%v]", ce.opts)
	return ce
}

func (ce *ConExecutor) Init() {
	ce.eRunStatus = ECeStatusInit
	//ce.wgJob = new(sync.WaitGroup)
	ce.wgJob = new(sync.WaitGroup)

	//fmt.Printf("Init TaskCenter config  taskConCurrent[%d]", ce.taskConCurrent)
	//log.Info("Init TaskCenter config", zap.Int("taskConcurrency", ce.taskConcurrency))
}

func (ce *ConExecutor) IsRunning() bool{
	return ECeStatusRunning == ce.eRunStatus
}

func (ce *ConExecutor) Start() {
	//启动各个 worker
	ce.eRunStatus = ECeStatusWaitStart

	//开启Task处理线程
	//ce.eRunStatus = ECeStatusRunning
	ce.chanCon = make(chan struct{}, ce.opts.concurrency )
	ce.chanJob = make(chan *JobInfo)
	ce.chanClose = make(chan int)
	//for i:=0; i < ce.taskConCurrent{

	go ce.run()
	//}

}

func (ce *ConExecutor) Stop() {

	ce.eRunStatus = ECeStatusWaitStop

	//等待Task接收线程停止
	ce.chanClose <- 1
	//log.Debug("Wait WgTask")
	ce.wgJob.Wait()
	//log.Debug("Close chanCon", zap.Int("ringLen", len(ce.chanCon)))
	close(ce.chanCon)
	//log.Debug("Wait WgTask done")

	//暂不支持向各个 task 发送停止指令， 只能等待结束

	//log.Debug("Wait wgWorker done")
	ce.eRunStatus = ECeStatusStop
}
func (ce *ConExecutor) run() {
	ce.eRunStatus = ECeStatusRunning
	quit := false
	for {
		if quit {
			break
		}
		select {
		case JobInfo := <-ce.chanJob:

			if nil != JobInfo {
				ce.chanCon <- struct{}{}
				go func() {
					ce.wgJob.Add(1)

					//err := ce.doJob(JobInfo)
					ce.doJob(JobInfo)
					//协程退出处理
					<-ce.chanCon
					ce.wgJob.Done()
				}()

			} else {
				//log.Error("Get JobInfo is nil")
			}

		case <-ce.chanClose:
			ce.eRunStatus = ECeStatusWaitStop
			quit = true
			break
		}
	}

	//等待Job接收线程停止
	//log.Info("TaskCenter Stop")
}

//
//sync 为true 时， 表示 同步方式执行任务，但任务优先级与异步方式的job一致，也需要等到 并发控制允许时才能执行
func (ce *ConExecutor) DoJob(fJob JobFunc, in interface{}, out interface{}, sync bool,jo ...JobOption) error {
	if ECeStatusRunning != ce.eRunStatus { //暂不加锁处理
		//log.Error("recv task on unexpected", zap.Int("status", int(ce.eRunStatus)))
		return ErrNotRunning
	}
	//log.Info("AddTask on ", zap.Int("status", int(ce.eRunStatus)))

	newJob := newJobInfo(fJob, in, out, jo...)
	var err error
	if sync {
		//同步执行
		ce.chanCon <- struct{}{}

		//同步等待任务结束
		ce.wgJob.Add(1)

		err = ce.doJob(newJob)
		//协程退出处理
		<-ce.chanCon
		ce.wgJob.Done()

	}else{
		//异步执行
		ce.chanJob <- newJob
		err = nil
	}

	return err
}

//Task的主协程由TaskCenter控制
func (ce *ConExecutor) doJob(job *JobInfo)  error{
	return job.Execute()
}
