package main

import (
	_ "embed"
	"errors"
	"io"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/jessevdk/go-flags"
	shoveler "github.com/opensciencegrid/xrootd-monitoring-shoveler"
	"github.com/pterm/pterm"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

//go:embed shoveler-public.pem
var publicKey []byte

var (
	version string
	commit  string
	date    string
	builtBy string
)

var logger *logrus.Logger

type Options struct {
	Verbose []bool `short:"v" long:"verbose" description:"Show verbose debug information"`
	Version bool   `short:"V" long:"version" description:"Print version information"`
	Config  string `short:"c" long:"config" description:"Configuration file to use" default:"/etc/xrootd-monitoring-shoveler/config.yaml"`
	Period  int    `short:"p" long:"period" description:"Period in seconds to check the shoveler status" default:"10"`
	Host    string `short:"H" long:"host" description:"Host to check the shoveler status, by default will use the port from the detected shoveler configuration" default:"localhost:8000"`
}

type ShovelerStats struct {
	packetsReceived       int64
	rabbitmqReconnections int64
	shoveler_queue_size   int64
}

var options Options
var parser = flags.NewParser(&options, flags.Default)

func main() {

	shoveler.ShovelerVersion = version
	shoveler.ShovelerCommit = commit
	shoveler.ShovelerDate = date
	shoveler.ShovelerBuiltBy = builtBy

	logger := logrus.New()
	shoveler.SetLogger(logger)

	// Parse flags from `args'. Note that here we use flags.ParseArgs for
	// the sake of making a working example. Normally, you would simply use
	// flags.Parse(&opts) which uses os.Args
	if _, err := parser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			logger.Errorln(err)
			os.Exit(1)
		}
	}

	spinnerConfig, _ := pterm.DefaultSpinner.Start("Checking the shoveler configuration")

	// Load the configuration
	config := shoveler.Config{}
	config.ReadConfig()

	if len(options.Verbose) > 0 {
		logger.SetLevel(logrus.DebugLevel)
		viper.Debug()
	} else {
		logger.SetLevel(logrus.WarnLevel)
	}
	logger.Debugln("Using configuration file:", viper.ConfigFileUsed())
	spinnerConfig.Success()

	CheckToken(config)

	// Try to connect to the prometheus endpoint
	if !config.Metrics {
		pterm.Error.Println("Metrics are disabled in the configuration file")
		logger.Errorln("Metrics are disabled in the configuration file, unable to determine if shoveler is running")
	}
	// Try downloading the metrics page
	initialStats, err := CheckPrometheusEndpoint(config.MetricsPort)
	if err != nil {
		//pterm.Error.Println("Unable to connect to the shoveler metrics endpoint")
		logger.Errorln("Unable to connect to the shoveler metrics endpoint, unable to determine if shoveler is running", err)
		os.Exit(1)
	}

	// Check the stats
	if initialStats.packetsReceived == 0 {
		pterm.Warning.Println("The shoveler has not receiving any packets since it was started")
		//os.Exit(1)
	}

	// Check the queue size
	if initialStats.shoveler_queue_size > 100 {
		pterm.Error.Println("The shoveler has", strconv.FormatInt(initialStats.shoveler_queue_size, 10), " packets in the queue, which indicates that the shoveler is not keeping up with the incoming packets")
		os.Exit(1)
	} else {
		pterm.Success.Println("The shoveler is running and keeping up with the incoming packets (if any)")
	}

	// Wait for the next period
	spinnerPeriod, _ := pterm.DefaultSpinner.Start("Checking the shoveler after period of " + strconv.Itoa(options.Period) + " seconds")
	// Sleep for the period
	time.Sleep(time.Duration(options.Period) * time.Second)
	spinnerPeriod.Success()
	// Query the metrics endpoint again
	secondStats, err := CheckPrometheusEndpoint(config.MetricsPort)
	if err != nil {
		spinnerPeriod.Fail("Unable to connect to the shoveler metrics endpoint: ", err)
		//logger.Errorln("Unable to connect to the shoveler metrics endpoint, unable to determine if shoveler is running", err)
		os.Exit(1)
	}

	// Check the stats
	if secondStats.packetsReceived == 0 {
		pterm.Error.Println("The shoveler has not receiving any packets since it was started")
		//os.Exit(1)
	}

	// Check the queue size
	if secondStats.shoveler_queue_size > 100 {
		pterm.Error.Println("The shoveler has", strconv.FormatInt(secondStats.shoveler_queue_size, 10), " packets in the queue, which indicates that the shoveler is not keeping up with the incoming packets")
		//os.Exit(1)
	} else {
		pterm.Success.Println("The shoveler queue is less than the error threshold of 100, keeping up with the incoming packets (if any)")
	}

	// Check the number of packets received
	if secondStats.packetsReceived == initialStats.packetsReceived {
		pterm.Error.Println("The shoveler has not received any packets since the first check")
	} else {
		pterm.Success.Println("The shoveler has received", strconv.FormatInt(secondStats.packetsReceived-initialStats.packetsReceived, 10), " packets since the last check")
	}

}

func CheckToken(config shoveler.Config) {
	// Check if the token is valid
	if config.MQ != "amqp" {
		pterm.Success.Println("The shoveler is not using RabbitMQ, skipping token check")
		return
	}
	spinnerToken, _ := pterm.DefaultSpinner.Start("Checking the shoveler token validity")
	// Check if the token is available
	if _, err := os.Stat(config.AmqpToken); errors.Is(err, os.ErrNotExist) {
		spinnerToken.Fail("Token file not found: ", err)
		return
	}

	// Read the token
	tokenBytes, err := os.ReadFile(config.AmqpToken)
	if err != nil {
		spinnerToken.Fail("Unable to open and read the token file: ", err)
		return
	}
	tokenString := string(tokenBytes)
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))
	token, err := parser.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		pubKey, err := jwt.ParseRSAPublicKeyFromPEM(publicKey)
		if err != nil {
			logger.Errorln("Unable to parse the public key:", err)
			return nil, err
		}
		return pubKey, nil
	})
	if err != nil {
		if err.Error() == "crypto/rsa: verification error" {
			spinnerToken.Fail("Invalid token signature, likely signed by wrong issuer or private key, please check the token file")
		} else {
			spinnerToken.Fail("Unable to parse the token:", err)
		}
		return
	}

	if errors.Is(err, jwt.ErrTokenMalformed) {
		spinnerToken.Fail("Token is malformed: ", err)
	} else if errors.Is(err, jwt.ErrTokenExpired) || errors.Is(err, jwt.ErrTokenNotValidYet) {
		// Token is either expired or not active yet
		spinnerToken.Fail("Token is expired or not active yet: ", err)
	} else if err != nil {
		spinnerToken.Fail("Couldn't handle this token:", err)
	}

	// Check the header
	token.Header["alg"] = "RS256"

	if !token.Claims.(jwt.MapClaims).VerifyAudience("my_rabbit_server", true) {
		if token.Claims.(jwt.MapClaims)["aud"] == nil {
			spinnerToken.Fail("Token doesn't have an audience, should be my_rabbit_server")
		} else {
			spinnerToken.Fail("Token audience doesn't match: ", token.Claims.(jwt.MapClaims)["aud"].(string)+" != my_rabbit_server")
		}
	}

	// Check that the issuer is correct
	if token.Claims.(jwt.MapClaims)["scope"] != "my_rabbit_server.write:xrd-mon/shoveled-xrd" {
		if token.Claims.(jwt.MapClaims)["scope"] == nil {
			spinnerToken.Fail("Token doesn't have the scope claim, should be my_rabbit_server.write:xrd-mon/shoveled-xrd")
		} else {
			spinnerToken.Fail("Token scope is not correct: " + token.Claims.(jwt.MapClaims)["scope"].(string) + " should be my_rabbit_server.write:xrd-mon/shoveled-xrd")
		}
	}

	//token.Claims.(jwt.MapClaims).VerifyIssuer(config.AmqpIssuer, true)
	spinnerToken.Success()
}

func CheckPrometheusEndpoint(metricsPort int) (ShovelerStats, error) {
	// Download from the metrics endpoint
	metricsURL := "http://localhost:" + strconv.Itoa(metricsPort) + "/metrics"
	spinnerInitialConnect, _ := pterm.DefaultSpinner.Start("Checking the shoveler metrics endpoint: " + metricsURL)
	resp, err := http.Get(metricsURL)
	if err != nil {
		spinnerInitialConnect.Fail()
		return ShovelerStats{}, err
	}
	defer resp.Body.Close()

	// Read all the body and return it
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		spinnerInitialConnect.Fail("Unable to read the metrics endpoint")
		return ShovelerStats{}, err
	}
	spinnerInitialConnect.Success()
	return parseShovelerStats(string(body)), nil

}

func parsePrometheusMetric(line string) int64 {
	flt, _, err := big.ParseFloat(strings.Split(line, " ")[1], 10, 0, big.ToNearestEven)
	if err != nil {
		logger.Errorln("Unable to parse prometheus metric", line, ":", err)
		return 0
	}
	int, _ := flt.Int64()
	return int
}

func parseShovelerStats(body string) ShovelerStats {
	// Loop through the body and parse the stats
	var stats ShovelerStats
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "shoveler_packets_received") {
			stats.packetsReceived = parsePrometheusMetric(line)
		} else if strings.HasPrefix(line, "shoveler_rabbitmq_reconnects") {
			stats.rabbitmqReconnections = parsePrometheusMetric(line)
		} else if strings.HasPrefix(line, "shoveler_queue_size") {
			stats.shoveler_queue_size = parsePrometheusMetric(line)
		}
	}
	return stats
}
