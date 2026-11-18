// Package main is the entry point for the identifier CLI application.
package main

import "github.com/ocfl-archive/identifier/cmd/identifier/commands"

// main executes the root Cobra command for identifier.
func main() {
	commands.Execute()
}
