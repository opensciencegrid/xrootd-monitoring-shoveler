package shoveler

import "github.com/sirupsen/logrus"

var log logrus.FieldLogger

func init() {
	// Give a default logger at the start to avoid null pointer error
	log = logrus.New()
}

func SetLogger(logger logrus.FieldLogger) {
	log = logger
}
