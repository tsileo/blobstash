package main

import (
    "fmt"
    "time"

    "github.com/spf13/cobra"
    "github.com/rdwilliamson/aws/glacier"
    "github.com/tsileo/datadatabase/backend/glacier/util"
)

func printJob(job *glacier.Job) {
    fmt.Printf("Job Id          : %v\n", job.JobId)
    fmt.Printf("Action          : %v\n", job.Action)
    fmt.Printf("Completed       : %v\n", job.Completed)
    fmt.Printf("Status code     : %v\n", job.StatusCode)
    fmt.Printf("Creation date   : %v\n", job.CreationDate)
    fmt.Printf("Initiated       : %v ago\n", time.Now().Sub(job.CreationDate))
    fmt.Printf("Completion date : %v\n", job.CompletionDate)
    fmt.Printf("Status message  : %v\n", job.StatusMessage)
    fmt.Printf("Archive Id      : %v\n", job.ArchiveId)
    fmt.Printf("Archive size    : %v\n", job.ArchiveSizeInBytes)
    fmt.Printf("Inventory size  : %v\n", job.InventorySizeInBytes)
}

func main() {

    var cmdDescribe = &cobra.Command{
        Use:   "describe [vault]",
        Short: "Display information about the given vault",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            con := util.GetCon()
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
    var cmdRestorearchive = &cobra.Command{
        Use:   "get [vault] [archive id]",
        Short: "",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            // TODO restore all archives in parallel from local (synced) inventory
            con := util.GetCon()
            if err := util.RestoreArchive(con, args[0], args[1], "", nil); err != nil {
                panic(err)
            }
        },
    }
    var cmdJobs = &cobra.Command{
        Use:   "jobs [vault]",
        Short: "",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            con := util.GetCon()
            jobs, _, err := con.ListJobs(args[0], "", "", "", 0)
            if err != nil {
                panic(err)
            }
            for _, job := range jobs {
                printJob(&job)
                fmt.Printf("\n")
            }
        },
    }
    var cmdJob = &cobra.Command{
        Use:   "job [vault] [job id]",
        Short: "",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            con := util.GetCon()
            job, err := con.DescribeJob(args[0], args[1])
            if err != nil {
                panic(err)
            }
            printJob(job)
        },
    }

    var cmdInventory = &cobra.Command{
        Use:   "inventory [vault] [job id]",
        Short: "",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            con := util.GetCon()
            inventory, err := con.GetInventoryJob(args[0], args[1])
            if err != nil {
                panic(err)
            }
            fmt.Printf("Inventory date: %v\n", inventory.InventoryDate)
            fmt.Printf("Archives:\n")
            for _, archive := range inventory.ArchiveList {
                fmt.Printf("  Archive id              : %+v\n", archive.ArchiveId)
                fmt.Printf("  Archive description     : %+v\n", archive.ArchiveDescription)
                fmt.Printf("  Archive creation date   : %+v\n", archive.CreationDate)
                fmt.Printf("  Size                    : %+v\n", archive.Size)
                fmt.Printf("\n")
            }

        },
    }

    var cmdSync = &cobra.Command{
        Use:   "sync [vault]",
        Short: "Fetch the latest inventory and sync local DB",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            con := util.GetCon()
            db, err := util.GetDB()
            defer db.Close()
            if err != nil {
                panic(err)
            }
            if err := util.Sync(con, db, args[0]); err != nil {
                panic(err)
            }
        },
    }
    var cmdRestore = &cobra.Command{
        Use:   "restore [vault]",
        Short: "Restore previously synced archives from local DB.",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            con := util.GetCon()
            db, err := util.GetDB()
            defer db.Close()
            if err != nil {
                panic(err)
            }
            if err := util.Restore(con, db, args[0]); err != nil {
                panic(err)
            }
        },
    }

    var rootCmd = &cobra.Command{Use: "datadb-glacier"}
    rootCmd.AddCommand(cmdDescribe)
    rootCmd.AddCommand(cmdRestore)
    rootCmd.AddCommand(cmdRestorearchive)
    rootCmd.AddCommand(cmdJobs)
    rootCmd.AddCommand(cmdJob)
    rootCmd.AddCommand(cmdSync)
    rootCmd.AddCommand(cmdInventory)
    rootCmd.Execute()
}
