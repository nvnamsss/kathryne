package internal

import "sync"

const ()

type KComponentLSMTree struct {
	c0        Component   // first component
	ck        []Component // other components
	k         int         // component amount
	threshold int
	rw        sync.RWMutex
}

type Component interface {
	Find(key string) (string, error)
	Put(key string, value string) error
	Delete(key string) error
	Compact(to Component) error
	Len() int
}

func NewLSMTree() *KComponentLSMTree {
	return &KComponentLSMTree{}
}

func (t *KComponentLSMTree) Put(key string, value string) error {
	var (
		err error
	)
	t.rw.Lock()
	defer t.rw.Unlock()

	if t.c0.Len() >= t.threshold {
		if err = t.c0.Compact(t.ck[0]); err != nil {
			return err
		}
		for index, v := range t.ck {
			if v.Len() >= t.threshold {
				if err = v.Compact(t.ck[index+1]); err != nil {
					return err
				}
			} else {
				break
			}
		}
	}

	if err = t.c0.Put(key, value); err != nil {
		return err
	}

	return nil
}

func (t *KComponentLSMTree) Get(key string) (string, error) {
	var (
		value string
		err   error
	)
	t.rw.RLock()
	defer t.rw.RLock()

	if value, err = t.c0.Find(key); err == nil {
		return value, nil
	}

	return value, nil
}

func (t *KComponentLSMTree) Delete(key string) error {
	var (
		err error
	)
	t.rw.Lock()
	defer t.rw.Unlock()

	if _, err = t.c0.Find(key); err == nil {
		return t.c0.Delete(key)
	}
	for _, v := range t.ck {
		if _, err = v.Find(key); err == nil {
			return v.Delete(key)
		}
	}

	return nil
}
