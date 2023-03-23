package tstorage

import (
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var storageManagerImpl *storageManager

type storageManager struct {
	storages []*storage
	ticker   *time.Ticker
}

func init() {
	log.Infof("starting storage manager")

	storageManagerImpl = &storageManager{}
	storageManagerImpl.storages = make([]*storage, 0)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for {
			sig := <-c
			switch sig {
			case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
				log.Infof("recieved quit, term or intrupt signal attempting to close storage safely")
				for _, storage := range storageManagerImpl.storages {
					err := storage.Close()
					if err != nil {
						log.Error("failed to close storage safely")
						return
					}
				}
			default:
				return
			}
		}
	}()

	// periodically check and permanently remove expired partitions.
	go func() {
		ticker := time.NewTicker(checkExpiredInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, s := range storageManagerImpl.storages {
					err := s.removeExpiredPartitions()
					if err != nil {
						log.Error(err)
					}
				}
			}
		}
	}()
}

func register(storage *storage) {
	if storage.inMemoryMode() == false {
		storageManagerImpl.storages = append(storageManagerImpl.storages, storage)
	}
}
