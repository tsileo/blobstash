/*

Package daemon implements a cron using robfig/cron parser [1]
designed to perform snapshots.

Configuration is "hot reloaded" so the daemon doesn't need to be restarted to load the new config.

It stores the last run time in a kv database [2].

The daemon support an "anacron mode" [3] (intended for laptop users)
where a job will be run if the delay has been exceeded (for simple recurring cycle
like "@every 24h") of if the next schedule is over (for con-like spec,
but the next schedule will follow the spec).

Links

	[1]: https://github.com/robfig/cron
	[2]: https://github.com/cznic/kv
	[3]: http://anacron.sourceforge.net/

*/
package daemon

import (
	"os"
	"os/signal"
	"log"
	"syscall"
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
	AnacronMode bool `json:"anacron_mode"`
	Snapshots []ConfigEntry `json:"snapshots"`
}

type Job struct {
	daemonConfig *Config
	config *ConfigEntry
	sched cron.Schedule
	Prev time.Time
	Next time.Time
}

func NewJob(conf *ConfigEntry, sched cron.Schedule) *Job {
	return &Job{config: conf, sched: sched}
}

func (job *Job) ComputeNext(now time.Time) {
	nowUTC := time.Now().UTC()
	elapsed := nowUTC.Sub(job.Next)
	if GetConfig().AnacronMode {
		csd, ok := job.sched.(cron.ConstantDelaySchedule)
		if !ok {
			// If a job.Next exists check that it isn't over,
			// if so, trigger the job now.
			if !job.Next.IsZero() {
				if elapsed > 0 {
					job.Next = nowUTC
				} else {

				}
				return
			}
		} else {
			elapsed := now.Sub(job.Prev)
			// If AnacronMode is enabled and the elapsed time
			// since the last run is greater than the scheduled delay,
			// the job next run is scheduled right now.
			if !job.Prev.IsZero() && elapsed > csd.Delay {
				job.Next = now
				return
			}
		}
	}
	if elapsed > 0 {
		job.Next = job.sched.Next(now).UTC()
	}
	return
}

func (j *Job) Key() string {
	return fmt.Sprintf("%v-%v", j.config.Path, j.config.Spec)
}

func (j *Job) Run() error {
	log.Printf("Running job %+v", j)
	return nil
}

func (j *Job) String() string {
	return fmt.Sprintf("Job %v:%v/prev:%v/next:%v)",
		j.config.Path, j.config.Spec, j.Prev, j.Next)
}

func (j *Job) Value() string {
	prev := "0"
	next := "0"
	if !j.Prev.IsZero() {
		prev = j.Prev.Format(time.RFC3339)
	}
	if !j.Next.IsZero() {
		next = j.Next.Format(time.RFC3339)
	}
	return fmt.Sprintf("%v %v", prev, next)
}

func ScanJob(job *Job, s string) (err error) {
	var prev, next string
	fmt.Sscan(s, &prev, &next)
	if prev != "0" {
		job.Prev, err = time.Parse(time.RFC3339, prev)
		if err != nil {
			return
		}
	}
	if next != "0" {
		job.Next, err = time.Parse(time.RFC3339, next)
		if err != nil {
			return
		}
	}
	return
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
	jobs []*Job
	db *kv.DB
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
	db, err := NewDB("db")
	if err != nil {
		panic(err)
	}
	return &Daemon{client:client, stop:make(chan struct{}),
		jobs: []*Job{}, db: db}
}

func (d *Daemon) Stop() {
	d.stop <- struct{}{}
}

func (d *Daemon) Run() {
	log.Println("Running...")
	defer d.db.Close()
	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt,
		syscall.SIGHUP,
	    syscall.SIGINT,
	    syscall.SIGTERM,
	    syscall.SIGQUIT)
	if err := d.updateJobs(); err != nil {
		panic(err)
	}
	now := time.Now().UTC()
	d.running = true
	var checkTime time.Time
	for {
		log.Printf("jobs:%q", d.jobs)
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
			for _, job := range d.jobs {
				if now.Sub(job.Next) < 0 {
					break
				}
				d.Lock()
				go job.Run()
				job.Prev = job.Next
				job.ComputeNext(now)
				if err := d.db.Set([]byte(job.Key()), []byte(job.Value())); err != nil {
					panic(err)
				}
				d.Unlock()
				continue
			}
		case <- d.stop:
			d.running = false
			return
		case <- configUpdated:
			log.Println("config updated")
			d.updateJobs()
		case sig := <- cs:
			log.Printf("captured %v\n", sig)
			d.db.Close()
			os.Exit(1)
		}
	}
}

func (d *Daemon) updateJobs() error {
	d.Lock()
	defer d.Unlock()
	conf := GetConfig()
	d.jobs = []*Job{}
	for _, snap := range conf.Snapshots {
		spec, err := cron.Parse(snap.Spec)
		if err != nil {
			log.Printf("Bad spec %v: %v\naborting updateJobs", snap.Spec, err)
			return err
		}
		job := NewJob(&snap, spec)
		res, err := d.db.Get(nil, []byte(job.Key()))
		if res == nil {
			prev := time.Now().UTC()
			if !job.Prev.IsZero() {
				prev = job.Prev
			}
			job.ComputeNext(prev)
		} else {
			if err := ScanJob(job, string(res)); err != nil {
				log.Printf("error scanning %v", err)
				return err
			}
			job.ComputeNext(job.Prev)
		}
		if err := d.db.Set([]byte(job.Key()), []byte(job.Value())); err != nil {
			return err
		}
		d.jobs = append(d.jobs, job)
	}
	return nil
}
