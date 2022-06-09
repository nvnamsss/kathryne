package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/nvnamsss/kathryne/logger"
)

/*
- Concurrency: 0
- File Management: 1
- Caching: 1
- Searching: 1
*/
const (
	PutOperation    int = 1
	DeleteOperation int = 2
)

var (
	KeyNotFound error = errors.New("Key not found")
)

type Record struct {
	Key       string
	Value     string
	Next      int64
	Operation int
}

type RecordFormat interface {
	Read(file *os.File, from int64) *Record
	Write(file *os.File, r Record) error
	GetSize(r Record) int64
}

type RFTransaction struct {
}

func newRecordFormatTransaction() RecordFormat {
	return &RFTransaction{}
}

func (rf *RFTransaction) Read(file *os.File, from int64) *Record {
	if file == nil {
		return nil
	}

	var (
		header      []byte = make([]byte, 9)
		n           int
		key         []byte
		value       []byte
		keyLength   int
		valueLength int
		err         error
		record      = &Record{}
	)

	n, err = file.ReadAt(header, from)
	if err != nil || n == 0 {
		log.Printf("Read header got error: %v", err)
		return nil
	}
	from = from + int64(n)

	record.Operation, keyLength, valueLength = rf.decodeHeader(header)
	key = make([]byte, keyLength)
	value = make([]byte, valueLength)

	n, err = file.ReadAt(key, from)
	if err != nil || n == 0 {
		log.Printf("Read key got error: %v", err)
		return nil
	}

	from = from + int64(n)
	n, err = file.ReadAt(value, from)
	if err != nil {
		log.Printf("Read value got error: %v", err)
	}
	record.Next = from + int64(n)
	record.Key = string(key)
	record.Value = string(value)

	return record
}

func (rf *RFTransaction) Write(file *os.File, record Record) error {
	var (
		data = rf.encode(record)
	)
	_, err := file.Write(data)
	return err
}

func (rf *RFTransaction) GetSize(r Record) int64 {
	var size int64 = 9
	size = size + int64(len(r.Key))
	size = size + int64(len(r.Value))
	return size
}

func (lm *RFTransaction) decodeHeader(header []byte) (operation int, keyLength int, valueLength int) {
	var (
		op    = header[0:1]
		key   = header[1:5]
		value = header[5:9]
	)

	operation = int(op[0])
	keyLength = int(binary.BigEndian.Uint32(key))
	valueLength = int(binary.BigEndian.Uint32(value))
	return
}

func (rf *RFTransaction) encode(r Record) []byte {
	var (
		header []byte = rf.encodeHeader(r.Operation, len(r.Key), len(r.Value))
		key    []byte = []byte(r.Key)
		value  []byte = []byte(r.Value)
		data   []byte = make([]byte, 0, len(header)+len(key)+len(value))
	)

	data = append(data, header...)
	data = append(data, key...)
	data = append(data, value...)
	return data
}

func (rf *RFTransaction) decode(data []byte) *Record {
	var (
		r *Record = &Record{}
	)

	return r
}

func (rf *RFTransaction) encodeHeader(operation, key, value int) []byte {
	var (
		header    []byte = make([]byte, 0, 9)
		keyData   []byte = make([]byte, 4)
		valueData []byte = make([]byte, 4)
	)
	binary.BigEndian.PutUint32(keyData, uint32(key))
	binary.BigEndian.PutUint32(valueData, uint32(value))
	header = append(header, byte(operation))
	header = append(header, keyData...)
	header = append(header, valueData...)

	return header
}

type RFRecord struct {
}

func (rf *RFRecord) Read(file *os.File, from int64) *Record {
	var (
		header      []byte = make([]byte, 8)
		key         []byte
		value       []byte
		keyLength   int
		valueLength int
		err         error
		record      = &Record{}
		n           int
	)
	n, err = file.ReadAt(header, from)
	if err != nil || n == 0 {
		logger.Errorf("Read header got error: %v", err)
		return nil
	}
	from = from + int64(n)

	keyLength, valueLength = rf.decodeHeader(header)
	key = make([]byte, keyLength)
	value = make([]byte, valueLength)

	n, err = file.ReadAt(key, from)
	if err != nil || n == 0 {
		logger.Errorf("Read key got error: %v", err)
		return nil
	}

	from = from + int64(n)
	n, err = file.ReadAt(value, from)
	if err != nil {
		logger.Errorf("Read value got error: %v", err)
	}
	record.Next = from + int64(n)
	record.Key = string(key)
	record.Value = string(value)

	return record
}

func (rf *RFRecord) Write(file *os.File, record Record) error {
	var (
		data = rf.encode(record)
	)
	_, err := file.Write(data)
	return err
}

func (rf *RFRecord) GetSize(r Record) int64 {
	var size int64 = 8
	size = size + int64(len(r.Key))
	size = size + int64(len(r.Value))
	return size
}

func (lm *RFRecord) decodeHeader(header []byte) (keyLength int, valueLength int) {
	var (
		key   = header[0:4]
		value = header[4:8]
	)

	keyLength = int(binary.BigEndian.Uint32(key))
	valueLength = int(binary.BigEndian.Uint32(value))
	return
}

func (rf *RFRecord) encode(r Record) []byte {
	var (
		header []byte = rf.encodeHeader(len(r.Key), len(r.Value))
		key    []byte = []byte(r.Key)
		value  []byte = []byte(r.Value)
		data   []byte = make([]byte, 0, len(header)+len(key)+len(value))
	)

	data = append(data, header...)
	data = append(data, key...)
	data = append(data, value...)
	return data
}

func (rf *RFRecord) encodeHeader(key, value int) []byte {
	var (
		header    []byte = make([]byte, 0, 8)
		keyData   []byte = make([]byte, 4)
		valueData []byte = make([]byte, 4)
	)
	binary.BigEndian.PutUint32(keyData, uint32(key))
	binary.BigEndian.PutUint32(valueData, uint32(value))
	header = append(header, keyData...)
	header = append(header, valueData...)
	return header
}

func newRecordFormatRecord() RecordFormat {
	return &RFRecord{}
}

type SSTableOption struct {
	DirectoryPath string
	Name          string
	Threshold     int
	BlockSize     int
}

func (s *SSTableOption) init() {
	if s.BlockSize <= 0 {
		s.BlockSize = 10
	}
	if s.Threshold <= 0 {
		s.Threshold = 1000
	}
	if s.DirectoryPath == "" {
		s.DirectoryPath, _ = os.Getwd()
	}
}

type SSTable struct {
	m      map[string]string
	index  SparseIndex
	length int

	option SSTableOption

	fTransactions *os.File
	fRecords      *os.File
	rfRecords     RecordFormat
	rfTransaction RecordFormat
	filter        *bloom.BloomFilter
	rwMutex       sync.RWMutex
}

func (s *SSTable) Find(key string) (string, error) {
	s.rwMutex.RLock()
	if value, ok := s.m[key]; ok {
		s.rwMutex.RUnlock()
		return value, nil
	}
	s.rwMutex.RUnlock()

	if !s.filter.Test([]byte(key)) {
		return "", KeyNotFound
	}
	var (
		from, to = s.index.Search(key)
	)

	for from <= to {
		record := s.rfRecords.Read(s.fRecords, from)
		if record.Key == key {
			return record.Value, nil
		}
		from = record.Next
	}

	return "", KeyNotFound
}

func (s *SSTable) Put(key string, value string) error {
	if err := s.rfTransaction.Write(s.fTransactions, Record{
		Key:       key,
		Value:     value,
		Operation: PutOperation,
	}); err != nil {
		return err
	}

	s.rwMutex.Lock()
	s.m[key] = value
	if len(s.m) == s.option.Threshold {
		s.Compress()
		s.m = make(map[string]string)
	}
	s.rwMutex.Unlock()

	s.filter.AddString(key)
	return nil
}

func (s *SSTable) Delete(key string) error {
	if err := s.rfTransaction.Write(s.fTransactions, Record{
		Key:       key,
		Value:     "nothing",
		Operation: DeleteOperation,
	}); err != nil {
		return err
	}

	delete(s.m, key)

	return nil
}

func (s *SSTable) Compress() {
	/* says that we have 2 files
	- keys
	- transactions
	foreach key in keys:
		iterate over transactions:
			apply the transaction to the key
		based on the state on current key, put it to new file

	time complexity: O(nm) which n equal to the number of keys, m equal to the number of transaction

	for optimization, the algorithm could be:

	iterate over the transactions to build up a new records
	iterate over the keys, merge the record from transactions to the key
	put these new records to new file
	time complexity: O(n + m + mlogm)
	*/
	var (
		records        map[string]Record = make(map[string]Record)
		fTransaction   *os.File          = s.fTransactions
		fRecords       *os.File          = s.fRecords
		fMergedRecords *os.File          = s.openRecordFile()
		rfTransaction                    = newRecordFormatTransaction()
		rfRecord                         = newRecordFormatRecord()
		record         *Record           = &Record{}
		offset         int64
	)
	defer fRecords.Close()

	for record != nil {
		record = rfTransaction.Read(fTransaction, record.Next)
		if record != nil {
			records[record.Key] = *record
		}
	}
	keys := make([]string, 0, len(records))
	for k := range records {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// need to sort after read
	var (
		index  = 0
		length = len(keys)
	)
	record = &Record{}
	s.filter.ClearAll()
	s.index.Clear()
	count := 0
	for {
		if record != nil {
			record = rfRecord.Read(fRecords, record.Next)
		}
		var r Record
		if record == nil && index >= length {
			break
		}

		if record == nil {
			r = records[keys[index]]
			index++
			r.Next = offset + rfRecord.GetSize(r)
		} else if index >= length {
			r = *record
		} else {
			if strings.Compare(keys[index], record.Key) <= 0 {
				r = records[keys[index]]
				index++
				r.Next = offset + rfRecord.GetSize(r)
			} else {
				r = *record
			}
		}

		if r.Operation != DeleteOperation {
			if err := rfRecord.Write(fMergedRecords, r); err != nil {
				logger.Errorf("Merging record error: %v", err)
			}
			s.filter.Add([]byte(r.Key))
			if count%s.option.BlockSize == 0 {
				s.index.Append(Elem{Key: r.Key, Offset: offset})
			}
			offset = r.Next
			count++
		}
	}

	s.fRecords = fMergedRecords
	if err := s.fTransactions.Truncate(0); err != nil {
		logger.Errorf("Truncate transaction file error: %v", err)
	}
	if _, err := s.fTransactions.Seek(0, 0); err != nil {
		logger.Errorf("Seek transaction file error: %v", err)
	}
}

func (s *SSTable) openRecordFile() *os.File {
	var (
		name = path.Join(s.option.DirectoryPath, fmt.Sprintf("%v_record_%v", s.option.Name, time.Now().Unix()))
	)
	f, err := os.OpenFile(name, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf("open record file got error: %v", err)
	}
	return f
}

func (s *SSTable) openLatestRecordFile() *os.File {
	files, err := SearchFileName(s.option.DirectoryPath)
	if err != nil {
		return nil
	}

	length := len(files)
	prefix := fmt.Sprintf("%v_record", s.option.Name)
	for i := length - 1; i >= 0; i-- {
		if strings.HasPrefix(files[i], prefix) {
			name := path.Join(s.option.DirectoryPath, files[i])
			f, err := os.OpenFile(name, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				logger.Errorf("open latest record file got error: %v", err)
			}
			return f
		}
	}

	return s.openRecordFile()
}

func (s *SSTable) openTransactionFile() *os.File {
	var (
		name = path.Join(s.option.DirectoryPath, fmt.Sprintf("%v_transaction", s.option.Name))
	)
	f, err := os.OpenFile(name, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf("open transaction file got error: %v", err)
	}
	return f
}

func (s *SSTable) Init() {
	var (
		record *Record
		offset int64
		index  int
	)
	s.fRecords = s.openLatestRecordFile()
	s.fTransactions = s.openTransactionFile()

	record = s.rfRecords.Read(s.fRecords, offset)
	for record != nil {
		s.filter.Add([]byte(record.Key))
		if index%s.option.BlockSize == 0 {
			s.index.Append(Elem{Key: record.Key, Offset: offset})
		}
		offset = record.Next
		index++
		record = s.rfRecords.Read(s.fRecords, offset)
	}
}

func (s *SSTable) Close() {
	s.fRecords.Close()
	s.fTransactions.Close()
}

func (s *SSTable) Len() int {
	return s.length
}

func NewSSTable(opt SSTableOption) *SSTable {
	var (
		ss *SSTable = &SSTable{
			m: make(map[string]string),
		}
	)
	opt.init()
	ss.option = opt
	ss.filter = bloom.NewWithEstimates(uint(opt.Threshold*2), 0.01)
	ss.rfTransaction = newRecordFormatTransaction()
	ss.rfRecords = newRecordFormatRecord()

	return ss
}
