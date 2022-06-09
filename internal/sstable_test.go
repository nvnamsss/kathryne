package internal

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/nvnamsss/kathryne/logger"
	"github.com/stretchr/testify/assert"
)

func Test_RFTransaction(t *testing.T) {
	var (
		rf *RFTransaction = &RFTransaction{}
	)

	encoded := rf.encode(Record{
		Key:       "key",
		Value:     "value",
		Operation: PutOperation,
	})

	op, kl, vl := rf.decodeHeader(encoded[0:9])
	log.Printf("Op: %v, Kl: %v, Vl: %v", op, kl, vl)
	assert.Equal(t, 1, op)
	assert.Equal(t, 3, kl)
	assert.Equal(t, 5, vl)
}

func Test_Anything(t *testing.T) {
	var (
		sstable SSTable = *NewSSTable(SSTableOption{
			DirectoryPath: "test/anything",
			Name:          "anything",
			Threshold:     5,
			BlockSize:     1,
		})

		times = 20
	)

	sstable.Init()
	defer sstable.Close()

	for i := 0; i < times; i++ {
		k := fmt.Sprintf("k%v", i)
		v := fmt.Sprintf("v%v", i)
		if err := sstable.Put(k, v); err != nil {
			t.Error(err)
		}
	}

	// for i := 0; i < times; i++ {
	// 	k := fmt.Sprintf("k%v", i)
	// 	v := fmt.Sprintf("v%v", i)
	// 	value, err := sstable.Find(k)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	assert.Equal(t, v, value)
	// }
}

func Test_Compress(t *testing.T) {
	var (
		sstable SSTable = *NewSSTable(SSTableOption{
			Name:          "compress",
			Threshold:     5000,
			BlockSize:     10,
			DirectoryPath: "test/compress",
		})
		times = 1000
	)
	sstable.Init()
	defer sstable.Close()

	now := time.Now()
	for i := 0; i < times; i++ {
		k := fmt.Sprintf("k%v", i)
		v := fmt.Sprintf("v%v", i)

		if err := sstable.Put(k, v); err != nil {
			t.Errorf("put error: %v", err)
		}
	}

	logger.Infof("put time: %v", time.Since(now).Microseconds())
	now = time.Now()
	sstable.Compress()
	logger.Infof("compress time: %v", time.Since(now).Microseconds())
}

func Test_Concurrency(t *testing.T) {
	var (
		sstable SSTable = *NewSSTable(SSTableOption{
			Name:          "concurrency",
			Threshold:     1000,
			BlockSize:     10,
			DirectoryPath: "test/concurrency",
		})
		wg    *sync.WaitGroup = &sync.WaitGroup{}
		times                 = 1005
	)

	sstable.Init()
	defer sstable.Close()

	for i := 0; i < times; i++ {
		wg.Add(1)
		go func(index int) {
			if index%100 == 0 {
				fmt.Printf("Storing at %v", index)
			}
			k := fmt.Sprintf("k%v", i)

			v := fmt.Sprintf("v%v", index)
			if err := sstable.Put(k, v); err != nil {
				t.Error(err)
			}
			wg.Done()
		}(i)
		// t.Run(fmt.Sprintf("store %v", i), func(t *testing.T) {
		// 	if i%100 == 0 {
		// 		t.Logf("Storing at %v", i)
		// 	}
		// 	k := fmt.Sprintf("k%v", i)
		// 	v := fmt.Sprintf("v%v", i)
		// 	if err := sstable.Put(k, v); err != nil {
		// 		t.Error(err)
		// 	}
		// })

	}

	wg.Wait()
	// for i := 0; i < times; i++ {
	// 	t.Run(fmt.Sprintf("load %v", i), func(t *testing.T) {
	// 		k := fmt.Sprintf("k%v", i)
	// 		v := fmt.Sprintf("v%v", i)
	// 		value, err := sstable.Find(k)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		assert.Equal(t, v, value)
	// 	})

	// }
}
