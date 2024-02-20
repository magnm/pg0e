package util

import (
	"sync"
)

type SyncedList[T any] struct {
	sync.RWMutex
	list []T
}

type SyncedListItem[T any] struct {
	Index int
	Value T
}

func NewSyncedList[T any]() *SyncedList[T] {
	return new(SyncedList[T])
}

func (l *SyncedList[T]) Add(item T) {
	l.Lock()
	defer l.Unlock()
	l.list = append(l.list, item)
}

func (l *SyncedList[T]) Remove(f func(T) bool) {
	l.Lock()
	defer l.Unlock()
	for i, v := range l.list {
		if f(v) {
			l.list = append(l.list[:i], l.list[i+1:]...)
		}
	}
}

func (l *SyncedList[T]) RemoveFirst(f func(T) bool) {
	l.Lock()
	defer l.Unlock()
	for i, v := range l.list {
		if f(v) {
			l.list = append(l.list[:i], l.list[i+1:]...)
			return
		}
	}
}

func (l *SyncedList[T]) Each() <-chan SyncedListItem[T] {
	c := make(chan SyncedListItem[T])

	f := func() {
		l.Lock()
		defer l.Unlock()
		for i, value := range l.list {
			c <- SyncedListItem[T]{i, value}
		}
		close(c)
	}
	go f()

	return c
}

func (l *SyncedList[T]) EachA(after func([]T) []T) <-chan SyncedListItem[T] {
	c := make(chan SyncedListItem[T])

	go func() {
		l.Lock()
		defer l.Unlock()
		for i, value := range l.list {
			c <- SyncedListItem[T]{i, value}
		}
		close(c)
		l.list = after(l.list)
	}()

	return c
}

func (l *SyncedList[T]) Len() int {
	l.RLock()
	defer l.RUnlock()
	return len(l.list)
}

func (l *SyncedList[T]) Clear() {
	l.RLock()
	defer l.RUnlock()
	l.list = []T{}
}
