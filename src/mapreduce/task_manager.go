package mapreduce

import (
	"container/list"
	"sync"
)

type TaskManager struct {
	sync.Mutex
	holder *list.List
}

func NewTaskManager() (tm *TaskManager)  {
	return &TaskManager{
		holder:list.New(),
	}
}

func (tm *TaskManager) Push(v interface{})  {
	tm.Lock()
	defer tm.Unlock()
	tm.holder.PushBack(v)
}

func (tm *TaskManager) Pop() (v interface{})  {
	tm.Lock()
	defer tm.Unlock()
	element := tm.holder.Front()
	tm.holder.Remove(element)
	v = element.Value
	return
}

func (tm *TaskManager) Empty() (bool)  {
	return tm.holder.Len() == 0
}

