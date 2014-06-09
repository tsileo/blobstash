package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/rdwilliamson/aws/glacier"
	"github.com/rdwilliamson/aws"
)


func main() {

    var cmdDescribe = &cobra.Command{
        Use:   "describe [vault]",
        Short: "Display information about the given vault",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
	       	accessKey := os.Getenv("S3_ACCESS_KEY")
			secretKey := os.Getenv("S3_SECRET_KEY")
			if accessKey == "" || secretKey == "" {
				panic("S3_ACCESS_KEY or S3_SECRET_KEY not set")
			}
			con := glacier.NewConnection(secretKey, accessKey, aws.EU)
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
            accessKey := os.Getenv("S3_ACCESS_KEY")
            secretKey := os.Getenv("S3_SECRET_KEY")
            if accessKey == "" || secretKey == "" {
                panic("S3_ACCESS_KEY or S3_SECRET_KEY not set")
            }
            con := glacier.NewConnection(secretKey, accessKey, aws.EU)
            jobId, err := con.InitiateRetrievalJob(args[0], args[1], "", "")
            fmt.Printf("jobId: %v/%v", jobId, err)
        },
    }
    var cmdJobs = &cobra.Command{
        Use:   "jobs [vault]",
        Short: "",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            accessKey := os.Getenv("S3_ACCESS_KEY")
            secretKey := os.Getenv("S3_SECRET_KEY")
            if accessKey == "" || secretKey == "" {
                panic("S3_ACCESS_KEY or S3_SECRET_KEY not set")
            }
            con := glacier.NewConnection(secretKey, accessKey, aws.EU)
            jobs, ckoa, err := con.ListJobs(args[0], "", "", "", 0)
            fmt.Printf("jobs:%q, %v, %v", jobs, ckoa, err)
            //jobId, err := con.InitiateRetrievalJob(args[0], args[1], "", "")
            //fmt.Printf("jobId: %v/%v", jobId, err)
        },
    }
    var cmdJob = &cobra.Command{
        Use:   "job [vault] [job id]",
        Short: "",
        Long:  "",
        Run: func(cmd *cobra.Command, args []string) {
            accessKey := os.Getenv("S3_ACCESS_KEY")
            secretKey := os.Getenv("S3_SECRET_KEY")
            if accessKey == "" || secretKey == "" {
                panic("S3_ACCESS_KEY or S3_SECRET_KEY not set")
            }
            con := glacier.NewConnection(secretKey, accessKey, aws.EU)
            job, err := con.DescribeJob(args[0], args[1])
            if err != nil {
                panic(err)
            }
            fmt.Printf("Job Id          : %v\n", job.JobId)
            fmt.Printf("Status code     : %v\n", job.StatusCode)
            fmt.Printf("Creation date   : %v\n", job.CreationDate)
            fmt.Printf("Completion date : %v\n", job.CompletionDate)
            fmt.Printf("Status message  : %v\n", job.StatusMessage)
            fmt.Printf("Archive Id      : %v\n", job.ArchiveId)
            fmt.Printf("Archive size    : %v\n", job.ArchiveSizeInBytes)
            fmt.Printf("Completed       : %v\n", job.Completed)
        },
    }
    var rootCmd = &cobra.Command{Use: "datadb-glacier"}
    rootCmd.AddCommand(cmdDescribe)
    rootCmd.AddCommand(cmdRestore)
    rootCmd.AddCommand(cmdJobs)
    rootCmd.AddCommand(cmdJob)
    rootCmd.Execute()
}
