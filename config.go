package shoveler

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/viper"
)

// WLCGConfig holds both the routing rules that decide whether a record is a
// WLCG packet and the producer/type values written into the metadata block of
// WLCG-formatted records. These differ between deployments, so they are
// configurable instead of hardcoded.
type WLCGConfig struct {
	// Routing: which records are treated as WLCG packets.
	VOs          []string // case-insensitive exact match; defaults to ["cms"]
	PathPrefixes []string // HasPrefix match; defaults to ["/store", "/user/dteam"]

	// Metadata: producer/type values written into WLCG-formatted records.
	Producer        string // metadata.producer for file-transfer (file-close) records
	Type            string // metadata.type for file-transfer records
	GStreamProducer string // metadata.producer for gstream cache & TPC records
}

// SiteConfig configures the CRIC-backed src/dst site resolver (collector mode).
// The resolved sites are emitted on WLCG-formatted records only, so only those
// records are resolved. When Source is empty the embedded CRIC domains snapshot
// (preset=domains&besttier=1) is used; set it to a file path or http(s):// URL
// to override it, re-fetched every RefreshInterval seconds. Enabled=false
// disables src_site/dst_site resolution entirely.
type SiteConfig struct {
	Enabled         bool   // resolve src_site/dst_site on WLCG records (default true)
	Source          string // domains map: file path or http(s):// URL; empty = embedded snapshot
	RefreshInterval int    // seconds between re-fetches of the domains URL source (default 86400; 0 disables)

	// IP/CIDR resolution: matches an endpoint address against the CRIC netroutes
	// CIDR blocks. IPSource expects CRIC's rcsite/query/?json structure
	// (netroutes per site); empty uses the embedded snapshot.
	IPEnabled         bool   // enable the IP/CIDR method (default true)
	IPSource          string // netroutes: file path or http(s):// URL; empty = embedded snapshot
	IPRefreshInterval int    // seconds between re-fetches of the netroutes URL source (default 86400; 0 disables)

	// Hostname resolution: exact match against CRIC storage-element protocol
	// endpoints (scheme and port stripped). HostnameSource expects CRIC's
	// service/query/?json&type=SE structure; empty uses the embedded snapshot.
	HostnameEnabled         bool   // enable the hostname method (default true)
	HostnameSource          string // SE endpoints: file path or http(s):// URL; empty = embedded snapshot
	HostnameRefreshInterval int    // seconds between re-fetches of the SE URL source (default 86400; 0 disables)

	// OverridesEnabled gates Overrides. It defaults to FALSE, unlike the other
	// site.*_enabled flags: a pin outrules everything CRIC says, so it is an
	// escape hatch for a site that is wrong or ambiguous, not something a
	// collector should be doing by accident. With it off the pins are ignored
	// entirely and every endpoint resolves from CRIC alone.
	OverridesEnabled bool

	// Overrides is the operator's own answer for an endpoint, applied only when
	// OverridesEnabled: site.overrides: {"ccsrm.in2p3.fr": "IN2P3-CC"}. A key is
	// an exact host, a domain suffix, an address, or a CIDR. Host keys are matched
	// like the domains map (full host first, then suffixes, longest key wins);
	// address keys by longest-prefix containment.
	Overrides map[string]string

	// LocalSite is the RCSite this collector runs at, e.g. "CERN-PROD". Every
	// server reporting to this collector sits at that site by definition, so when
	// LocalSite is set the "config" method resolves the server end outright, with
	// no DNS or CRIC lookup. Leave it empty on a collector that aggregates
	// several sites, since there is then no single answer.
	LocalSite string
	// LocalSiteLANClients also applies LocalSite to clients on a private,
	// loopback or link-local address (default true). Those clients are at the
	// reporting server's site by definition, and CRIC declares no worker-node
	// ranges, so otherwise they stay unresolved. Set false to leave them
	// unresolved instead of assumed local.
	LocalSiteLANClients bool
	// ResolutionOrder is the order the per-endpoint methods are tried in, first
	// hit wins: "override" (Overrides), "config" (LocalSite), "hostname" (exact CRIC SE endpoint host),
	// "ip" (netroutes CIDR containment), "domain" (longest domain-suffix match).
	// Unknown or repeated names are dropped, and an empty list falls back to the
	// default order.
	ResolutionOrder []string
}

type FilterConfig struct {
	DropPathPrefixes []string // records whose path matches any prefix are dropped entirely
	DropVOs          []string // records whose VO matches (case-insensitive) are dropped entirely
}

type InputConfig struct {
	Type          string // "udp", "file", or "rabbitmq"
	Host          string
	Port          int
	BufferSize    int
	BrokerURL     string
	Topic         string // Topic name for STOMP, or queue name for RabbitMQ
	Queue         string // Alias for Topic when using RabbitMQ (for clarity)
	Subscription  string
	Base64Encoded bool
	Path          string // File path for "file" input type
	Follow        bool   // Follow mode (tail-like) for "file" input type
}

type StateConfig struct {
	EntryTTL   int // TTL in seconds for state entries
	MaxEntries int // Max entries in state map (0 for unlimited)

	// DNS Enrichment configuration
	EnableDNSEnrichment bool // Enable DNS enrichment with caching and worker pool
	DNSCacheTTL         int  // DNS cache TTL in seconds (default: 3600)
	DNSTimeout          int  // DNS lookup timeout in seconds (default: 2)

	// Enrichment pipeline configuration
	EnrichmentWorkers   int // Number of enrichment worker goroutines (default: 5)
	EnrichmentQueueSize int // Maximum number of pending enrichment requests (default: 1000000)

	// GStream pipeline configuration
	GStreamWorkers   int // Number of gstream worker goroutines (default: 4)
	GStreamQueueSize int // Maximum number of pending gstream packets (default: 20000)
}

type OutputConfig struct {
	Type string // "mq" (default), "file", or "both"
	Path string // File path for "file" or "both" output types
}

type Config struct {
	Input                 InputConfig
	State                 StateConfig
	Output                OutputConfig
	WLCG                  WLCGConfig
	Site                  SiteConfig
	Mode                  string   // Operating mode: "shoveler" or "collector"
	MQ                    string   // Which technology to use for the MQ connection
	AmqpURL               *url.URL // AMQP URL (password comes from the token)
	AmqpExchange          string   // Exchange to shovel file-close messages
	AmqpExchangeCache     string   // Exchange for cache gstream events
	AmqpExchangeTCP       string   // Exchange for TCP gstream events
	AmqpExchangeTPC       string   // Exchange for TPC gstream events
	AmqpExchangeWLCG      string   // Exchange for WLCG formatted events
	AmqpExchangeWLCGCache string   // Exchange for WLCG formatted cache gstream events
	AmqpExchangeWLCGTPC   string   // Exchange for WLCG formatted TPC events
	AmqpToken             string   // File location of the token
	AmqpPublishWorkers    int      // Number of concurrent publishing workers
	ListenPort            int
	ListenIp              string
	DestUdp               []string
	Debug                 bool
	Verify                bool
	StompUser             string
	StompPassword         string
	StompURL              *url.URL
	StompTopic            string
	Metrics               bool
	MetricsPort           int
	Profile               bool
	ProfilePort           int
	StompCert             string
	StompCertKey          string
	QueueDir              string
	IpMapAll              string
	IpMap                 map[string]string
	Filter                FilterConfig
}

func (c *Config) ReadConfig() {
	c.ReadConfigWithPathAndPrefix("", "SHOVELER")
}

func (c *Config) ReadConfigWithPath(configPath string) {
	c.ReadConfigWithPathAndPrefix(configPath, "SHOVELER")
}

func (c *Config) ReadConfigWithPathAndPrefix(configPath string, envPrefix string) {
	if configPath != "" {
		// Use the specified config file
		viper.SetConfigFile(configPath)
	} else {
		// Use default search paths
		viper.SetConfigName("config")                            // name of config file (without extension)
		viper.SetConfigType("yaml")                              // REQUIRED if the config file does not have the extension in the name
		viper.AddConfigPath("/etc/xrootd-monitoring-shoveler/")  // path to look for the config file in
		viper.AddConfigPath("$HOME/.xrootd-monitoring-shoveler") // call multiple times to add many search paths
		viper.AddConfigPath(".")                                 // optionally look for config in the working directory
		viper.AddConfigPath("config/")
	}
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Warningln("Unable to read in config file, will check environment for configuration:", err)
	}
	viper.SetEnvPrefix(envPrefix)

	// Set the mode based on the environment prefix
	switch envPrefix {
	case "SHOVELER":
		c.Mode = "shoveler"
	case "COLLECTOR":
		c.Mode = "collector"
	default:
		c.Mode = "unknown"
	}

	// Autmatically look to the ENV for all "Gets"
	viper.AutomaticEnv()
	// Look for environment variables with underscores
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Input configuration
	viper.SetDefault("input.type", "udp")
	c.Input.Type = viper.GetString("input.type")
	c.Input.Host = viper.GetString("input.host")
	c.Input.Port = viper.GetInt("input.port")
	viper.SetDefault("input.buffer_size", 65536)
	c.Input.BufferSize = viper.GetInt("input.buffer_size")
	c.Input.BrokerURL = viper.GetString("input.broker_url")
	c.Input.Topic = viper.GetString("input.topic")
	c.Input.Queue = viper.GetString("input.queue")
	// If queue is specified but topic is not, use queue as topic (for RabbitMQ)
	if c.Input.Queue != "" && c.Input.Topic == "" {
		c.Input.Topic = c.Input.Queue
	}
	c.Input.Subscription = viper.GetString("input.subscription")
	viper.SetDefault("input.base64_encoded", true)
	c.Input.Base64Encoded = viper.GetBool("input.base64_encoded")
	c.Input.Path = viper.GetString("input.path")
	c.Input.Follow = viper.GetBool("input.follow")

	// State configuration (for collector mode)
	viper.SetDefault("state.entry_ttl", 300) // 5 minutes default
	c.State.EntryTTL = viper.GetInt("state.entry_ttl")
	viper.SetDefault("state.max_entries", 0) // unlimited by default
	c.State.MaxEntries = viper.GetInt("state.max_entries")

	// DNS Enrichment configuration
	viper.SetDefault("state.enable_dns_enrichment", false) // disabled by default
	c.State.EnableDNSEnrichment = viper.GetBool("state.enable_dns_enrichment")
	viper.SetDefault("state.dns_cache_ttl", 3600) // 1 hour default
	c.State.DNSCacheTTL = viper.GetInt("state.dns_cache_ttl")
	if c.State.DNSCacheTTL <= 0 {
		c.State.DNSCacheTTL = 3600
	}
	viper.SetDefault("state.dns_timeout", 2) // 2 seconds default
	c.State.DNSTimeout = viper.GetInt("state.dns_timeout")
	if c.State.DNSTimeout <= 0 {
		c.State.DNSTimeout = 2
	}

	// Enrichment pipeline configuration
	viper.SetDefault("state.enrichment_workers", 5) // 5 enrichment workers default
	c.State.EnrichmentWorkers = viper.GetInt("state.enrichment_workers")
	if c.State.EnrichmentWorkers <= 0 {
		c.State.EnrichmentWorkers = 5
	}
	viper.SetDefault("state.enrichment_queue_size", 1000000) // 1M queue default
	c.State.EnrichmentQueueSize = viper.GetInt("state.enrichment_queue_size")
	if c.State.EnrichmentQueueSize <= 0 {
		c.State.EnrichmentQueueSize = 1000000
	}

	// GStream pipeline configuration
	viper.SetDefault("state.gstream_workers", 4) // 4 gstream workers default
	c.State.GStreamWorkers = viper.GetInt("state.gstream_workers")
	if c.State.GStreamWorkers <= 0 {
		c.State.GStreamWorkers = 4
	}
	viper.SetDefault("state.gstream_queue_size", 20000) // 20k queue default
	c.State.GStreamQueueSize = viper.GetInt("state.gstream_queue_size")
	if c.State.GStreamQueueSize <= 0 {
		c.State.GStreamQueueSize = 20000
	}

	// Output configuration (for collector mode)
	viper.SetDefault("output.type", "mq") // message queue by default
	c.Output.Type = viper.GetString("output.type")
	c.Output.Path = viper.GetString("output.path")

	viper.SetDefault("mq", "amqp")
	c.MQ = viper.GetString("mq")

	switch c.MQ {
	case "amqp":
		viper.SetDefault("amqp.exchange", "shoveled-xrd")
		viper.SetDefault("amqp.exchange_cache", "xrd-cache-events")
		viper.SetDefault("amqp.exchange_tcp", "xrd-tcp-events")
		viper.SetDefault("amqp.exchange_tpc", "xrd-tpc-events")
		viper.SetDefault("amqp.token_location", "/etc/xrootd-monitoring-shoveler/token")

		// Get the AMQP URL
		c.AmqpURL, err = url.Parse(viper.GetString("amqp.url"))
		if err != nil {
			panic(fmt.Errorf("fatal error parsing AMQP URL: %w", err))
		}
		log.Debugln("AMQP URL:", c.AmqpURL.String())

		// Get the AMQP Exchanges
		c.AmqpExchange = viper.GetString("amqp.exchange")
		log.Debugln("AMQP Exchange:", c.AmqpExchange)

		c.AmqpExchangeCache = viper.GetString("amqp.exchange_cache")
		log.Debugln("AMQP Cache Exchange:", c.AmqpExchangeCache)

		c.AmqpExchangeTCP = viper.GetString("amqp.exchange_tcp")
		log.Debugln("AMQP TCP Exchange:", c.AmqpExchangeTCP)

		c.AmqpExchangeTPC = viper.GetString("amqp.exchange_tpc")
		log.Debugln("AMQP TPC Exchange:", c.AmqpExchangeTPC)

		viper.SetDefault("amqp.exchange_wlcg", "xrd-wlcg-events")
		c.AmqpExchangeWLCG = viper.GetString("amqp.exchange_wlcg")
		log.Debugln("AMQP WLCG Exchange:", c.AmqpExchangeWLCG)

		viper.SetDefault("amqp.exchange_wlcg_cache", "xrd-wlcg-cache-events")
		c.AmqpExchangeWLCGCache = viper.GetString("amqp.exchange_wlcg_cache")
		log.Debugln("AMQP WLCG Cache Exchange:", c.AmqpExchangeWLCGCache)

		viper.SetDefault("amqp.exchange_wlcg_tpc", "xrd-wlcg-tpc-events")
		c.AmqpExchangeWLCGTPC = viper.GetString("amqp.exchange_wlcg_tpc")
		log.Debugln("AMQP WLCG TPC Exchange:", c.AmqpExchangeWLCGTPC)

		// Get the Token location
		c.AmqpToken = viper.GetString("amqp.token_location")
		log.Debugln("AMQP Token location:", c.AmqpToken)

		// Get the number of publish workers
		viper.SetDefault("amqp.publish_workers", 10)
		c.AmqpPublishWorkers = viper.GetInt("amqp.publish_workers")
		log.Debugln("AMQP Publish Workers:", c.AmqpPublishWorkers)
	case "stomp":
		viper.SetDefault("stomp.topic", "xrootd.shoveler")

		c.StompUser = viper.GetString("stomp.user")
		log.Debugln("STOMP User:", c.StompUser)
		c.StompPassword = viper.GetString("stomp.password")

		// Get the STOMP URL
		c.StompURL, err = url.Parse(viper.GetString("stomp.url"))
		if err != nil {
			panic(fmt.Errorf("fatal error parsing STOMP URL: %w", err))
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
	default:
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

	// Profile defaults
	viper.SetDefault("profile.enable", false)
	c.Profile = viper.GetBool("profile.enable")
	viper.SetDefault("profile.port", 3030)
	c.ProfilePort = viper.GetInt("profile.port")

	// WLCG metadata configuration (producer/type used to create WLCG-formatted records).
	// These viper defaults are the single source of truth for the values and match the OSG
	// upstream collector; a config file or COLLECTOR_WLCG_* environment variable
	// overrides them for a WLCG (or other) deployment.
	viper.SetDefault("wlcg.producer", "cms")
	c.WLCG.Producer = viper.GetString("wlcg.producer")
	viper.SetDefault("wlcg.type", "aaa-ng")
	c.WLCG.Type = viper.GetString("wlcg.type")
	viper.SetDefault("wlcg.gstream_producer", "cms-xrootd-cache")
	c.WLCG.GStreamProducer = viper.GetString("wlcg.gstream_producer")

	// Src/dst site resolver configuration (collector mode). By default the
	// embedded CRIC domains snapshot (preset=domains&besttier=1) is used; set
	// site.source to a file path or an http(s):// URL (e.g. the CRIC domains
	// endpoint) to override it. A URL source is re-fetched every
	// site.refresh_interval seconds (0 disables refreshing). Set site.enabled to
	// false to skip src_site/dst_site resolution entirely.
	viper.SetDefault("site.enabled", true)
	c.Site.Enabled = viper.GetBool("site.enabled")
	c.Site.Source = viper.GetString("site.source")
	viper.SetDefault("site.refresh_interval", 86400)
	c.Site.RefreshInterval = viper.GetInt("site.refresh_interval")

	// IP/CIDR resolution. By default the embedded CRIC netroutes snapshot is used;
	// set site.ip_source to a file path or an http(s):// URL serving CRIC's
	// rcsite/query/?json (netroutes per site) for current coverage, re-fetched
	// every site.ip_refresh_interval seconds.
	viper.SetDefault("site.ip_enabled", true)
	c.Site.IPEnabled = viper.GetBool("site.ip_enabled")
	c.Site.IPSource = viper.GetString("site.ip_source")
	viper.SetDefault("site.ip_refresh_interval", 86400)
	c.Site.IPRefreshInterval = viper.GetInt("site.ip_refresh_interval")

	// Hostname resolution. By default the embedded CRIC SE snapshot is used; set
	// site.hostname_source to a file path or an http(s):// URL serving CRIC's
	// service/query/?json&type=SE for current coverage, re-fetched every
	// site.hostname_refresh_interval seconds.
	// Site pins for endpoints CRIC reports ambiguously (or gets wrong).
	// Off by default: a pin beats every CRIC answer, so it must be switched on
	// deliberately rather than taking effect just because a key was left in a
	// config file.
	viper.SetDefault("site.overrides_enabled", false)
	c.Site.OverridesEnabled = viper.GetBool("site.overrides_enabled")
	c.Site.Overrides = viper.GetStringMapString("site.overrides")

	viper.SetDefault("site.hostname_enabled", true)
	c.Site.HostnameEnabled = viper.GetBool("site.hostname_enabled")
	c.Site.HostnameSource = viper.GetString("site.hostname_source")
	viper.SetDefault("site.hostname_refresh_interval", 86400)
	c.Site.HostnameRefreshInterval = viper.GetInt("site.hostname_refresh_interval")

	// The site this collector runs at (e.g. site.local_site: CERN-PROD). Setting
	// it lets the resolver label the reporting server from the config instead of
	// inferring it, which is both certain and free; it also covers LAN clients
	// unless site.local_site_lan_clients is false. site.resolution_order sets
	// which methods are tried and in what order (first hit wins).
	c.Site.LocalSite = viper.GetString("site.local_site")
	viper.SetDefault("site.local_site_lan_clients", true)
	c.Site.LocalSiteLANClients = viper.GetBool("site.local_site_lan_clients")
	viper.SetDefault("site.resolution_order", []string{"config", "hostname", "ip", "domain"})
	c.Site.ResolutionOrder = viper.GetStringSlice("site.resolution_order")

	viper.SetDefault("queue_directory", "/var/spool/xrootd-monitoring-shoveler/queue")
	c.QueueDir = viper.GetString("queue_directory")

	// Configure the mapper
	// First, check for the map environment variable
	c.IpMapAll = viper.GetString("map.all")

	// If the map is not set
	c.IpMap = viper.GetStringMapString("map")

	// WLCG routing configuration (collector mode only)
	// Defaults preserve current behavior: CMS VO and /store, /user/dteam paths.
	viper.SetDefault("wlcg.vos", []string{"cms"})
	viper.SetDefault("wlcg.path_prefixes", []string{"/store", "/user/dteam"})
	c.WLCG.VOs = viper.GetStringSlice("wlcg.vos")
	c.WLCG.PathPrefixes = viper.GetStringSlice("wlcg.path_prefixes")

	// Record drop filter (collector mode only); defaults to drop nothing.
	c.Filter.DropPathPrefixes = viper.GetStringSlice("filter.drop_path_prefixes")
	c.Filter.DropVOs = viper.GetStringSlice("filter.drop_vos")
}
