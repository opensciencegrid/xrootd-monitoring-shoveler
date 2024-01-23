package shoveler

import "github.com/sirupsen/logrus"

var log logrus.FieldLogger

func SetLogger(logger logrus.FieldLogger) {
	log = logger
}
