package main

import (
    "fmt"

    "github.com/spf13/cobra"
    "github.com/tsileo/datadatabase/backend/glacier/util"
)

func main() {
    var cmdStatus = &cobra.Command{
        Use:   "status [vault]",
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
    rootCmd.AddCommand(cmdStatus)
    rootCmd.AddCommand(cmdRestore)
    rootCmd.AddCommand(cmdSync)
    rootCmd.Execute()
}
