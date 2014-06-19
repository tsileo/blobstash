/*

Package scheduler implements a cron using robfig/cron parser [1]
designed to perform snapshots.

Configuration is "hot reloaded" so the scheduler doesn't need to be restarted to load the new config.

It stores the last run time in a kv database [2].

The scheduler support an "anacron mode" [3] (intended for laptop users)
where a job will be run if the delay has been exceeded (for simple recurring cycle
like "@every 24h") of if the next schedule is over (for con-like spec,
but the next schedule will follow the spec).

Links

	[1]: https://github.com/robfig/cron
	[2]: https://github.com/cznic/kv
	[3]: http://anacron.sourceforge.net/

*/
package scheduler

import (
	"fmt"
	"github.com/cznic/kv"
	"github.com/robfig/cron"
	dclient "github.com/tsileo/blobstash/client"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

type Job struct {
	schedulerConfig *Config
	config       *ConfigEntry
	sched        cron.Schedule
	Prev         time.Time
	Next         time.Time
}

// NewJob initialize a Job
func NewJob(conf *ConfigEntry, sched cron.Schedule) *Job {
	return &Job{config: conf, sched: sched}
}

// ComputeNext determine the next scheduled time for running the Job.
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

// Key generate a unique Key for the Job (used as an ID in the DB).
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

// Value serialize the job.Prev/job.Next to store as a string in the DB.
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

// Scan job parse the previously serialized value stored in the DB.
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

// New initialize a new KV database.
func NewDB(path string) (*kv.DB, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	return createOpen(path, opts())
}

type Scheduler struct {
	client  *dclient.Client
	stop    chan struct{}
	running bool
	jobs    []*Job
	db      *kv.DB
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

func New(client *dclient.Client) *Scheduler {
	db, err := NewDB("scheduler-db")
	if err != nil {
		panic(err)
	}
	return &Scheduler{client: client, stop: make(chan struct{}),
		jobs: []*Job{}, db: db}
}

// Stop shutdown the Scheduler cleanly.
func (d *Scheduler) Stop() {
	d.stop <- struct{}{}
}

// Run start the processing of jobs, and listen for config update.
func (d *Scheduler) Run() {
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
		sort.Sort(byTime(d.jobs))
		if len(d.jobs) == 0 {
			// Sleep for 5 years until the config change
			checkTime = now.AddDate(5, 0, 0)
		} else {
			log.Printf("sleep until %v", d.jobs[0].Next)
			checkTime = d.jobs[0].Next
		}
		select {
		case now = <-time.After(checkTime.Sub(now)):
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
		case <-d.stop:
			d.running = false
			return
		case <-configUpdated:
			log.Println("config updated")
			d.updateJobs()
		case sig := <-cs:
			log.Printf("captured %v\n", sig)
			d.db.Close()
			os.Exit(1)
		}
	}
}

// updateJobs parse the config and detect the next scheduled job.
func (d *Scheduler) updateJobs() error {
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
