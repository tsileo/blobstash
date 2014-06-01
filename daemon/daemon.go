package daemon

import (
	"os"
	"log"
	"sync"
	"sort"
	"io/ioutil"
	"fmt"
	"time"
 	"encoding/json"
 	dclient "github.com/tsileo/datadatabase/client"
 	"github.com/cznic/kv"
 	"github.com/robfig/cron"
)

var (
  config *Config
  configLock = new(sync.RWMutex)
  configUpdated = make(chan struct{})
)

func loadConfig(fail bool){
  file, err := ioutil.ReadFile("config.json")
  if err != nil {
    log.Println("open config: ", err)
    if fail { os.Exit(1) }
  }

  temp := new(Config)
  if err = json.Unmarshal(file, temp); err != nil {
    log.Println("parse config: ", err)
    if fail { os.Exit(1) }
  }
  configLock.Lock()
  config = temp
  configLock.Unlock()
}

func GetConfig() *Config {
  configLock.RLock()
  defer configLock.RUnlock()
  return config
}

func watchFile(filePath string) error {
    initialStat, err := os.Stat(filePath)
    if err != nil {
        return err
    }

    for {
        stat, err := os.Stat(filePath)
        if err != nil {
            return err
        }

        if stat.Size() != initialStat.Size() || stat.ModTime() != initialStat.ModTime() {
            break
        }

        time.Sleep(1 * time.Second)
    }

    return nil
}

func init() {
	loadConfig(true)
    go func() {
    	for {
		    err := watchFile("config.json")
		    if err != nil {
		        fmt.Println(err)
		    }
		    loadConfig(false)
		    configUpdated <- struct{}{}
		}
	}()
}

type ConfigEntry struct {
	Path string `json:"path"`
	Spec string `json:"spec"`
}

type Config struct {
	Snapshots []ConfigEntry `json:"snapshots"`
}

type Job struct {
	config *ConfigEntry
	sched cron.Schedule
	Prev time.Time
	Next time.Time
}

func NewJob(conf *ConfigEntry, sched cron.Schedule) *Job {
	return &Job{config: conf, sched: sched}
}

func (j *Job) Key() string {
	return fmt.Sprintf("%v-%v", j.config.Path, j.config.Spec)
}

func opts() *kv.Options {
	return &kv.Options{
		VerifyDbBeforeOpen:  true,
		VerifyDbAfterOpen:   true,
		VerifyDbBeforeClose: true,
		VerifyDbAfterClose:  true,
	}
}

// New initialize a new DiskLRU.
func NewDB(path string) (*kv.DB, error) {
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	return createOpen(path, opts())
}

type Daemon struct {
	client *dclient.Client
	stop chan struct{}
	running bool
	jobsIndex map[string]*Job
	jobs []*Job
	sync.Mutex
}

type byTime []*Job

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

func New(client *dclient.Client) *Daemon {
	return &Daemon{client:client, stop:make(chan struct{}), jobsIndex: make(map[string]*Job), jobs: []*Job{}}
}

func (d *Daemon) Stop() {
	d.stop <- struct{}{}
}

func (d *Daemon) Run() {
	log.Println("Run")
	log.Printf("Initial conf: %+v", GetConfig())
	d.updateJobs()
	now := time.Now()
	d.running = true
	var checkTime time.Time
	for {
		log.Printf("for:")
		sort.Sort(byTime(d.jobs))
		if len(d.jobs) == 0 {
			// Sleep for 5 years until the config change
			checkTime = now.AddDate(5, 0, 0)
		} else {
			log.Printf("sleep until %v", d.jobs[0].Next)
			checkTime = d.jobs[0].Next
		}
		select {
		case now = <- time.After(checkTime.Sub(now)):
			log.Printf("CHECK %+v/now %v", d.jobs[0], now)
			for _, job := range d.jobs {
				if job.Next.Equal(now) {
					break
				}
				log.Printf("Running job %+v", job)
				time.Sleep(1 * time.Second)
				job.Prev = job.Next
				job.Next = job.sched.Next(now)
				continue
			}
		case <- d.stop:
			d.running = false
			return
		case <- configUpdated:
			log.Println("configUpdated")
			d.updateJobs()
		}
	}
}

func (d *Daemon) updateJobs() {
	d.Lock()
	defer d.Unlock()
	conf := GetConfig()
	for _, snap := range conf.Snapshots {
		spec, err := cron.Parse(snap.Spec)
		if err != nil {
			log.Printf("Bad spec %v: %v\naborting updateJobs", snap.Spec, err)
			return
		}
		job := NewJob(&snap, spec)
		_, exists := d.jobsIndex[job.Key()]
		if !exists {
			prev := time.Now()
			if !job.Prev.IsZero() {
				prev = job.Prev
			}
			job.Next = job.sched.Next(prev)
			d.jobsIndex[job.Key()] = job
			d.jobs = append(d.jobs, job)
		}
	}
}
