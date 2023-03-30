package shoveler

import (
	"time"
)

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var (
	ShovelerVersion string
	ShovelerCommit  string
	ShovelerDate    string
	ShovelerBuiltBy string
)
