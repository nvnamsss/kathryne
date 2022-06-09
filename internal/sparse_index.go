package internal

import "strings"

type SparseIndex struct {
	elems []Elem
}

func (s *SparseIndex) searchLower(key string) int64 {
	var (
		l      = 0
		r      = len(s.elems) - 1
		length = r + 1
	)
	for l <= r {
		mid := l + (r-l)/2
		cmp := strings.Compare(s.elems[mid].Key, key)
		if cmp >= 0 {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}

	if l == length {
		return s.elems[l-1].Offset + 1
	}
	return s.elems[l].Offset
}

func (s *SparseIndex) searchUpper(key string) int64 {
	var (
		l      = 0
		r      = len(s.elems) - 1
		length = r + 1
	)
	for l <= r {
		mid := l + (r-l)/2
		cmp := strings.Compare(s.elems[mid].Key, key)
		if cmp > 0 {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	if l == length {
		l = l - 1
	}
	return s.elems[l].Offset
}

func (s *SparseIndex) Search(key string) (int64, int64) {
	var (
		lower int64 = s.searchLower(key)
		upper int64 = s.searchUpper(key)
	)

	return lower, upper
}

func (s *SparseIndex) Append(elem Elem) {
	s.elems = append(s.elems, elem)
}

func (s *SparseIndex) Clear() {
	s.elems = nil
}

type Elem struct {
	Key    string
	Offset int64
}
