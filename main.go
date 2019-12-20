package main

import (
	"fmt"
	"log"
	"os"
	"time"

	uuid "github.com/satori/go.uuid"

	badger "github.com/dgraph-io/badger"
)

var numMessagesPerBatch = 5000

func gcBadgerDB(db *badger.DB) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
	again:
		err := db.RunValueLogGC(0.5)
		if err == nil {
			goto again
		}
	}
}

func main() {

	homeDir, err := os.UserHomeDir()
	x := time.Now().Unix()
	db, err := badger.Open(badger.DefaultOptions(homeDir + "/badger20"))
	fmt.Printf("DB Opened in: %v secs\n", time.Now().Unix()-x)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	go gcBadgerDB(db)

	// Your code here�~@�
	for {
		time.Sleep(25 * time.Millisecond)
		var messageIDs [][]byte

		for i := 0; i < numMessagesPerBatch; i++ {
			messageID := uuid.NewV4().Bytes()
			messageIDs = append(messageIDs, messageID)
		}

		_ = db.Update(func(txn *badger.Txn) error {
			// Your code here�~@�
			for _, messageID := range messageIDs {
				e := badger.NewEntry([]byte(messageID), nil).WithTTL(86400 * time.Second)
				if err := txn.SetEntry(e); err == badger.ErrTxnTooBig {
					_ = txn.Commit()
					txn = db.NewTransaction(true)
					_ = txn.SetEntry(e)
				}
			}
			return nil
		})
	}
}
