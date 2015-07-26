package util

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/rdwilliamson/aws"
	"github.com/rdwilliamson/aws/glacier"
)

const (
	checkDelay = 30 * time.Second
)

func getRegion(name string) *aws.Region {
	for _, region := range aws.Regions {
		if name == region.Name {
			return region
		}
	}
	return aws.USWest1
}

// GetCon create a new glacier.Connection using environment variables.
func GetCon(region string) *glacier.Connection {
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	if accessKey == "" || secretKey == "" {
		panic("S3_ACCESS_KEY or S3_SECRET_KEY not set")
	}
	con := glacier.NewConnection(secretKey, accessKey, getRegion(region))
	return con
}

// RestoreArchive wait for job completion and restore the archive to the given path.
func RestoreArchive(con *glacier.Connection, vault, jobId, path string, wg *sync.WaitGroup) error {
	if wg != nil {
		defer wg.Done()
	}
	var job *glacier.Job
	var err error
	for {
		job, err = con.DescribeJob(vault, jobId)
		if err != nil {
			return err
		}
		if job.Completed {
			log.Printf("Archive %v: job %v completed", job.ArchiveId, jobId)
			break
		}
		time.Sleep(checkDelay)
	}
	r, _, err := con.GetRetrievalJob(vault, jobId, 0, job.ArchiveSizeInBytes-1)
	if err != nil {
		return err
	}
	defer r.Close()
	if path == "" {
		path = job.ArchiveId
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	io.Copy(f, r)
	log.Printf("Archive %v: restored to %v", job.ArchiveId, path)
	return nil
}

func handleClose(db *DB) {
	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-cs
	log.Printf("Stopping...")
	db.Close()
	os.Exit(1)
}

func Restore(con *glacier.Connection, db *DB, vault string) (err error) {
	var wg sync.WaitGroup
	go handleClose(db)
	jobs := []string{}
	kvs := make(chan *KeyValue)
	go db.Iter(kvs, "archive:", "archive:\xff", 0)
	errch := make(chan error)
	go func() {
		select {
		case cerr := <-errch:
			if cerr != nil {
				panic(cerr)
			}
		}
	}()
	for kv := range kvs {
		archive := &glacier.Archive{}
		if err := json.Unmarshal([]byte(kv.Value), archive); err != nil {
			return err
		}
		var jobId string
		statusKey := fmt.Sprintf("job:%v", kv.Key)
		jobId, err = db.Get(statusKey)
		if err != nil {
			return err
		}
		if jobId == "" {
			jobId, err = con.InitiateRetrievalJob(vault, archive.ArchiveId, "", "")
			if err != nil {
				return err
			}
			if err := db.Set(statusKey, jobId); err != nil {
				return err
			}
			log.Printf("Initiated a new job for archive retrieval %v", archive.ArchiveId)
		}
		wg.Add(1)
		go func(archive *glacier.Archive, jobId string) {
			if err := RestoreArchive(con, vault, jobId, filepath.Base(archive.ArchiveDescription), &wg); err != nil {
				errch <- err
			}
		}(archive, jobId)
		jobs = append(jobs, jobId)
	}
	if len(jobs) == 0 {
		log.Printf("Error: no archive in local DB, maybe sync is not done yet?")
		db.Close()
		os.Exit(1)
	}
	log.Printf("Waiting for %v jobs to complete, you can safely CTRL+C and resume this process", len(jobs))
	wg.Wait()
	defer close(errch)
	log.Printf("Restore done, %v archives restored", len(jobs))
	return nil
}

func Sync(con *glacier.Connection, db *DB, vault string) error {
	go handleClose(db)
	lastSync, err := db.Get("inventory-last-run")
	if err != nil {
		return err
	}
	if lastSync != "" {
		fmt.Printf("WARNING: A successful sync was performed on %v,\n", lastSync)
		fmt.Printf("if a new sync is started, any previous jobs running won't be tracked anymore.\n")
		fmt.Printf("Are you sure you want to continue (yes/no)? ")
		if !askForConfirmation() {
			log.Printf("Aborting...")
			db.Close()
			os.Exit(1)
		} else {
			db.Drop()
		}
	}
	var jobId string
	jobId, err = db.Get("inventory-job-id")
	if err != nil {
		return err
	}
	var jobExists bool
	if jobId != "" {
		jobExists = true
		_, err := con.DescribeJob(vault, jobId)
		if err != nil {
			jobExists = false
			log.Printf("Job %v isn't available anymore", jobId)
		}
	}
	if jobId == "" || !jobExists {
		jobId, err = con.InitiateInventoryJob(vault, "", "")
		if err != nil {
			return err
		}
		log.Printf("Initiated a new inventory job: %v", jobId)
		if err := db.Set("inventory-job-id", jobId); err != nil {
			return err
		}
	} else {
		log.Printf("Checking job %v", jobId)
	}
	log.Printf("Waiting for job to complete, you can safely CTRL+C and resume this process")
	for {
		job, err := con.DescribeJob(vault, jobId)
		if err != nil {
			return err
		}
		if job.Completed {
			log.Printf("Job %v completed", jobId)
			break
		}
		time.Sleep(checkDelay)
	}
	inventory, err := con.GetInventoryJob(vault, jobId)
	if err != nil {
		return err
	}
	for index, archive := range inventory.ArchiveList {
		archiveJson, err := json.Marshal(&archive)
		if err != nil {
			return err
		}
		log.Printf("Saving archive %v", archive.ArchiveId)
		if err := db.Set(fmt.Sprintf("archive:%v", index), string(archiveJson)); err != nil {
			return err
		}
	}
	if err := db.Set("inventory-last-run", time.Now().Format(time.RFC3339)); err != nil {
		return err
	}
	log.Printf("Inventory saved, sync done")
	return nil
}
