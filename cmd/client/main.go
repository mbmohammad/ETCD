package main

import (
	"fmt"
	"os"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/pkg/client"
	"github.com/spf13/cobra"
)

var baseURL string

func main() {
	rootCmd := &cobra.Command{
		Use:   "clientcli",
		Short: "CLI tool to interact with the client server",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if baseURL == "" {
				baseURL = os.Getenv("CLIENT_BASE_URL")
				if baseURL == "" {
					baseURL = "http://loadbalancer:9001"
				}
			}
		},
	}

	rootCmd.PersistentFlags().StringVar(&baseURL, "url", "", "Base URL of the client server (can also use CLIENT_BASE_URL env variable)")

	rootCmd.AddCommand(
		pingCmd(),
		setCmd(),
		getCmd(),
		deleteCmd(),
		partitionsCmd(),
	)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func partitionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partitions",
		Short: "Manage partitions",
	}
	cmd.AddCommand(partitionsAddCmd())
	return cmd
}

func partitionsAddCmd() *cobra.Command {
	var count int
	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add new partitions",
		Run: func(cmd *cobra.Command, args []string) {
			if count <= 0 {
				cmd.Help()
				os.Exit(1)
			}
			c := newClient()
			if err := c.AddPartitions(count); err != nil {
				fmt.Println("Add partitions failed:", err)
				os.Exit(1)
			}
			fmt.Println("Partitions added successfully")
		},
	}
	cmd.Flags().IntVar(&count, "count", 0, "Number of partitions to add")
	cmd.MarkFlagRequired("count")
	return cmd
}

func newClient() *client.Client {
	return client.NewClient(baseURL)
}

func pingCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ping",
		Short: "Check connection to the server",
		Run: func(cmd *cobra.Command, args []string) {
			c := newClient()
			if err := c.CheckConnection(); err != nil {
				fmt.Println("Ping failed:", err)
				os.Exit(1)
			}
			fmt.Println("Ping successful")
		},
	}
}

func setCmd() *cobra.Command {
	var key, value string
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Set a key-value pair",
		Run: func(cmd *cobra.Command, args []string) {
			if key == "" || value == "" {
				cmd.Help()
				os.Exit(1)
			}
			c := newClient()
			if err := c.Set(key, value); err != nil {
				fmt.Println("Set failed:", err)
				os.Exit(1)
			}
			fmt.Println("Set successful")
		},
	}
	cmd.Flags().StringVar(&key, "key", "", "Key to set")
	cmd.Flags().StringVar(&value, "value", "", "Value to set")
	cmd.MarkFlagRequired("key")
	cmd.MarkFlagRequired("value")
	return cmd
}

func getCmd() *cobra.Command {
	var key string
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get the value of a key",
		Run: func(cmd *cobra.Command, args []string) {
			if key == "" {
				cmd.Help()
				os.Exit(1)
			}
			c := newClient()
			value, err := c.Get(key)
			if err != nil {
				fmt.Println("Get failed:", err)
				os.Exit(1)
			}
			fmt.Println("Value:", value)
		},
	}
	cmd.Flags().StringVar(&key, "key", "", "Key to retrieve")
	cmd.MarkFlagRequired("key")
	return cmd
}

func deleteCmd() *cobra.Command {
	var key string
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a key",
		Run: func(cmd *cobra.Command, args []string) {
			if key == "" {
				cmd.Help()
				os.Exit(1)
			}
			c := newClient()
			if err := c.Delete(key); err != nil {
				fmt.Println("Delete failed:", err)
				os.Exit(1)
			}
			fmt.Println("Delete successful")
		},
	}
	cmd.Flags().StringVar(&key, "key", "", "Key to delete")
	cmd.MarkFlagRequired("key")
	return cmd
}
