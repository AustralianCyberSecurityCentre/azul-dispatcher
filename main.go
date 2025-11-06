package main

import (
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/cmd"
	_ "go.uber.org/automaxprocs"
)

func main() {
	cmd.Execute()
}
