package main

import (
    "fmt"
    "os"
    "time"
    "io"

    "github.com/spf13/cobra"
    "github.com/rdwilliamson/aws/glacier"
    "github.com/rdwilliamson/aws"
    _ "github.com/tsileo/datadatabase/backend/glacier/util"
)

func GetCon() *glacier.Connection {
    accessKey := os.Getenv("S3_ACCESS_KEY")
    secretKey := os.Getenv("S3_SECRET_KEY")
    if accessKey == "" || secretKey == "" {
        panic("S3_ACCESS_KEY or S3_SECRET_KEY not set")
    }
    con := glacier.NewConnection(secretKey, accessKey, aws.EU)
    return con
}

func main() {

    var cmdDescribe = &cobra.Command{
        Use:   "describe [vault]",
        Short: "Display information about the given vault",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            con := GetCon()
            vault, _ := con.DescribeVault(args[0])
            fmt.Printf("Vault name:         : %v\n", vault.VaultName)
            fmt.Printf("Creation date:      : %v\n", vault.CreationDate)
            fmt.Printf("Last inventory date : %v\n", vault.LastInventoryDate)
            fmt.Printf("Number of archives  : %v\n", vault.NumberOfArchives)
            fmt.Printf("Size                : %v\n", vault.SizeInBytes)
            //jobId, err := con.InitiateInventoryJob(args[0], "", "")
            //fmt.Printf("jobId: %v/%v", jobId, err)
        },
    }
    var cmdRestore = &cobra.Command{
        Use:   "restore [vault] [archive id]",
        Short: "",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            // TODO restore all archives in parallel from local (synced) inventory
            con := GetCon()
            var job *glacier.Job
            for {
                job, err := con.DescribeJob(args[0], args[1])
                if err != nil {
                    panic(err)
                }
                if job.Completed {
                    break
                }
                time.Sleep(30 * time.Second)
            }
            r, _, err := con.GetRetrievalJob(args[0], args[1], 0, job.ArchiveSizeInBytes - 1)
            if err != nil {
                panic(err)
            }
            fmt.Printf("r:%+v", r)
            defer r.Close()
            f, err := os.OpenFile(job.ArchiveId, os.O_RDWR|os.O_CREATE, 0666)
            if err != nil {
                panic(err)
            }
            defer f.Close()
            io.Copy(f, r)
            fmt.Printf("Restore %v done", job.ArchiveId)
            //jobId, err := con.InitiateRetrievalJob(args[0], args[1], "", "")
            //fmt.Printf("jobId: %v/%v", jobId, err)
        },
    }
    var cmdJobs = &cobra.Command{
        Use:   "jobs [vault]",
        Short: "",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            con := GetCon()
            jobs, ckoa, err := con.ListJobs(args[0], "", "", "", 0)
            fmt.Printf("jobs:%q, %v, %v", jobs, ckoa, err)
            jobId, err := con.InitiateInventoryJob(args[0], "", "")
            fmt.Printf("jobId: %v/%v", jobId, err)
        },
    }

    var cmdJob = &cobra.Command{
        Use:   "job [vault] [job id]",
        Short: "",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            con := GetCon()
            job, err := con.DescribeJob(args[0], args[1])
            if err != nil {
                panic(err)
            }
            fmt.Printf("###########\nJob Summary\n###########\n")
            fmt.Printf("Job Id          : %v\n", job.JobId)
            fmt.Printf("Completed       : %v\n", job.Completed)
            fmt.Printf("Status code     : %v\n", job.StatusCode)
            fmt.Printf("Creation date   : %v\n", job.CreationDate)
            fmt.Printf("Initiated       : %v ago\n", time.Now().Sub(job.CreationDate))
            fmt.Printf("Completion date : %v\n", job.CompletionDate)
            fmt.Printf("Status message  : %v\n", job.StatusMessage)
            fmt.Printf("Archive Id      : %v\n", job.ArchiveId)
            fmt.Printf("Archive size    : %v\n", job.ArchiveSizeInBytes)
            fmt.Printf("Inventory size  : %v\n", job.InventorySizeInBytes)
        },
    }

    var cmdSync = &cobra.Command{
        Use:   "sync [vault]",
        Short: "Fetch the latest inventory and sync local DB",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            con := GetCon()
            jobId, err := con.InitiateInventoryJob(args[0], "", "")
            if err != nil {
                panic(err)
            }
            //var job *glacier.Job
            for {
                job, err := con.DescribeJob(args[0], jobId)
                if err != nil {
                    panic(err)
                }
                if job.Completed {
                    break
                }
                time.Sleep(30 * time.Second)
            }
            inventory, err := con.GetInventoryJob(args[0], jobId)
            if err != nil {
                panic(err)
            }
            fmt.Printf("Inventory:%+v", inventory)
            // TODO save the inventory in DB
            //      mapping ArchiveId => blobsfile name
            //      and latest sync data
        },
    }

    var rootCmd = &cobra.Command{Use: "datadb-glacier"}
    rootCmd.AddCommand(cmdDescribe)
    rootCmd.AddCommand(cmdRestore)
    rootCmd.AddCommand(cmdJobs)
    rootCmd.AddCommand(cmdJob)
    rootCmd.AddCommand(cmdSync)
    rootCmd.Execute()
}
