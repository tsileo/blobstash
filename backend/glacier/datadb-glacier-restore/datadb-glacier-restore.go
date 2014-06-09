package main

import (
	"fmt"
	"strings"
	"github.com/spf13/cobra"
)


func main() {

    var cmdPrint = &cobra.Command{
        Use:   "print [string to print]",
        Short: "Print anything to the screen",
        Long:  `print is for printing anything back to the screen.
        For many years people have printed back to the screen.
        `,
        Run: func(cmd *cobra.Command, args []string) {
            fmt.Println("Print: " + strings.Join(args, " "))
        },
    }

    var rootCmd = &cobra.Command{Use: "datadb-glacier-restore"}
    rootCmd.AddCommand(cmdPrint)
    rootCmd.Execute()
}
