package middleware

import (
	"log"
	"os"
)

var stdoutLog = log.New(os.Stdout, "", log.LstdFlags)
