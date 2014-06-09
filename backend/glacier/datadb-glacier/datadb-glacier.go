package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/rdwilliamson/aws/glacier"
	"github.com/rdwilliamson/aws"
)


func main() {

    var cmdPrint = &cobra.Command{
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
            fmt.Printf("vault: %+v", vault)
            jobId, err := con.InitiateInventoryJob(args[0], "", "")
            fmt.Printf("jobId: %v/%v", jobId, err)
        },
    }

    var rootCmd = &cobra.Command{Use: "datadb-glacier"}
    rootCmd.AddCommand(cmdPrint)
    rootCmd.Execute()
}
