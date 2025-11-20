package shoveler

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/viper"
)

type InputConfig struct {
	Type        string // "udp" or "message_bus"
	Host        string
	Port        int
	BufferSize  int
	BrokerURL   string
	Topic       string
	Subscription string
	Base64Encoded bool
}

type StateConfig struct {
	EntryTTL   int // TTL in seconds for state entries
	MaxEntries int // Max entries in state map (0 for unlimited)
}

type Config struct {
	Mode          string   // "shoveling" or "collector"
	Input         InputConfig
	State         StateConfig
	MQ            string   // Which technology to use for the MQ connection
	AmqpURL       *url.URL // AMQP URL (password comes from the token)
	AmqpExchange  string   // Exchange to shovel messages
	AmqpToken     string   // File location of the token
	ListenPort    int
	ListenIp      string
	DestUdp       []string
	Debug         bool
	Verify        bool
	StompUser     string
	StompPassword string
	StompURL      *url.URL
	StompTopic    string
	Metrics       bool
	MetricsPort   int
	StompCert     string
	StompCertKey  string
	QueueDir      string
	IpMapAll      string
	IpMap         map[string]string
}

func (c *Config) ReadConfig() {
	viper.SetConfigName("config")                            // name of config file (without extension)
	viper.SetConfigType("yaml")                              // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("/etc/xrootd-monitoring-shoveler/")  // path to look for the config file in
	viper.AddConfigPath("$HOME/.xrootd-monitoring-shoveler") // call multiple times to add many search paths
	viper.AddConfigPath(".")                                 // optionally look for config in the working directory
	viper.AddConfigPath("config/")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Warningln("Unable to read in config file, will check environment for configuration:", err)
	}
	viper.SetEnvPrefix("SHOVELER")

	// Autmatically look to the ENV for all "Gets"
	viper.AutomaticEnv()
	// Look for environment variables with underscores
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Mode configuration - default to "shoveling" for backward compatibility
	viper.SetDefault("mode", "shoveling")
	c.Mode = viper.GetString("mode")

	// Input configuration
	viper.SetDefault("input.type", "udp")
	c.Input.Type = viper.GetString("input.type")
	c.Input.Host = viper.GetString("input.host")
	c.Input.Port = viper.GetInt("input.port")
	viper.SetDefault("input.buffer_size", 65536)
	c.Input.BufferSize = viper.GetInt("input.buffer_size")
	c.Input.BrokerURL = viper.GetString("input.broker_url")
	c.Input.Topic = viper.GetString("input.topic")
	c.Input.Subscription = viper.GetString("input.subscription")
	viper.SetDefault("input.base64_encoded", true)
	c.Input.Base64Encoded = viper.GetBool("input.base64_encoded")

	// State configuration (for collector mode)
	viper.SetDefault("state.entry_ttl", 300) // 5 minutes default
	c.State.EntryTTL = viper.GetInt("state.entry_ttl")
	viper.SetDefault("state.max_entries", 0) // unlimited by default
	c.State.MaxEntries = viper.GetInt("state.max_entries")

	viper.SetDefault("mq", "amqp")
	c.MQ = viper.GetString("mq")

	if c.MQ == "amqp" {
		viper.SetDefault("amqp.exchange", "shoveled-xrd")
		viper.SetDefault("amqp.token_location", "/etc/xrootd-monitoring-shoveler/token")

		// Get the AMQP URL
		c.AmqpURL, err = url.Parse(viper.GetString("amqp.url"))
		if err != nil {
			panic(fmt.Errorf("Fatal error parsing AMQP URL: %s \n", err))
		}
		log.Debugln("AMQP URL:", c.AmqpURL.String())

		// Get the AMQP Exchange
		c.AmqpExchange = viper.GetString("amqp.exchange")
		log.Debugln("AMQP Exchange:", c.AmqpExchange)

		// Get the Token location
		c.AmqpToken = viper.GetString("amqp.token_location")
		log.Debugln("AMQP Token location:", c.AmqpToken)
	} else if c.MQ == "stomp" {
		viper.SetDefault("stomp.topic", "xrootd.shoveler")

		c.StompUser = viper.GetString("stomp.user")
		log.Debugln("STOMP User:", c.StompUser)
		c.StompPassword = viper.GetString("stomp.password")

		// Get the STOMP URL
		c.StompURL, err = url.Parse(viper.GetString("stomp.url"))
		if err != nil {
			panic(fmt.Errorf("Fatal error parsing STOMP URL: %s \n", err))
		}
		log.Debugln("STOMP URL:", c.StompURL.String())

		c.StompTopic = viper.GetString("stomp.topic")
		log.Debugln("STOMP Topic:", c.StompTopic)

		// Get the STOMP cert
		c.StompCert = viper.GetString("stomp.cert")
		log.Debugln("STOMP CERT:", c.StompCert)

		// Get the STOMP certkey
		c.StompCertKey = viper.GetString("stomp.certkey")
		log.Debugln("STOMP CERTKEY:", c.StompCertKey)
	} else {
		log.Panic("MQ option is not one of the allowed ones (amqp, stomp)")
	}
	// Get the UDP listening parameters
	viper.SetDefault("listen.port", 9993)
	c.ListenPort = viper.GetInt("listen.port")
	c.ListenIp = viper.GetString("listen.ip")

	c.DestUdp = viper.GetStringSlice("outputs.destinations")

	c.Debug = viper.GetBool("debug")

	viper.SetDefault("verify", true)
	c.Verify = viper.GetBool("verify")

	// Metrics defaults
	viper.SetDefault("metrics.enable", true)
	c.Metrics = viper.GetBool("metrics.enable")
	viper.SetDefault("metrics.port", 8000)
	c.MetricsPort = viper.GetInt("metrics.port")

	viper.SetDefault("queue_directory", "/var/spool/xrootd-monitoring-shoveler/queue")
	c.QueueDir = viper.GetString("queue_directory")

	// Configure the mapper
	// First, check for the map environment variable
	c.IpMapAll = viper.GetString("map.all")

	// If the map is not set
	c.IpMap = viper.GetStringMapString("map")
}
