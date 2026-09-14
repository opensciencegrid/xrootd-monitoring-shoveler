package shoveler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
)

// readTrafficConfig writes body to a temp config file and reads it back the way
// the collector does, returning the parsed Config.
func readTrafficConfig(t *testing.T, body string) *Config {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	viper.Reset()
	t.Cleanup(viper.Reset)

	config := &Config{}
	config.ReadConfigWithPathAndPrefix(path, "COLLECTOR")
	return config
}

// TestTrafficConfigDefaults covers the production defaults for the traffic
// classification keys: it is on as soon as WLCG mode is, and the rules
// themselves are left unset here so the collector package supplies them.
func TestTrafficConfigDefaults(t *testing.T) {
	config := readTrafficConfig(t, "wlcg:\n  enabled: true\n")

	if !config.WLCG.TrafficEnabled {
		t.Error("TrafficEnabled = false, expected the classification on by default")
	}
	if len(config.WLCG.TrafficInternalUsers) != 0 {
		t.Errorf("TrafficInternalUsers = %v, expected unset (the collector applies the default)", config.WLCG.TrafficInternalUsers)
	}
	if len(config.WLCG.TrafficReplicationPrefixes) != 0 {
		t.Errorf("TrafficReplicationPrefixes = %v, expected unset", config.WLCG.TrafficReplicationPrefixes)
	}
	if config.WLCG.TrafficJobAgentMin != 0 || config.WLCG.TrafficJobAgentMax != 0 {
		t.Errorf("job-agent range = %d-%d, expected unset", config.WLCG.TrafficJobAgentMin, config.WLCG.TrafficJobAgentMax)
	}
	if config.WLCG.TrafficCaseSensitive {
		t.Error("TrafficCaseSensitive = true, expected case-insensitive matching by default")
	}
}

// TestTrafficConfigOverrides covers the rules being deployment values: each one
// can be set in the config file.
func TestTrafficConfigOverrides(t *testing.T) {
	config := readTrafficConfig(t, `wlcg:
  enabled: true
  traffic_enabled: false
  traffic_internal_users: ["root", "xrootd"]
  traffic_job_agent_min: 1
  traffic_job_agent_max: 16
  traffic_replication_prefixes: ["replicate", "xfer"]
  traffic_case_sensitive: true
`)

	if config.WLCG.TrafficEnabled {
		t.Error("TrafficEnabled = true, expected the config file to switch it off")
	}
	if got := config.WLCG.TrafficInternalUsers; len(got) != 2 || got[0] != "root" || got[1] != "xrootd" {
		t.Errorf("TrafficInternalUsers = %v, expected [root xrootd]", got)
	}
	if config.WLCG.TrafficJobAgentMin != 1 || config.WLCG.TrafficJobAgentMax != 16 {
		t.Errorf("job-agent range = %d-%d, expected 1-16", config.WLCG.TrafficJobAgentMin, config.WLCG.TrafficJobAgentMax)
	}
	if got := config.WLCG.TrafficReplicationPrefixes; len(got) != 2 || got[1] != "xfer" {
		t.Errorf("TrafficReplicationPrefixes = %v, expected [replicate xfer]", got)
	}
	if !config.WLCG.TrafficCaseSensitive {
		t.Error("TrafficCaseSensitive = false, expected true from the config file")
	}
}

// TestTrafficConfigFromEnvironment covers the COLLECTOR_ environment overrides,
// which is how a container deployment sets these.
func TestTrafficConfigFromEnvironment(t *testing.T) {
	t.Setenv("COLLECTOR_WLCG_TRAFFIC_ENABLED", "false")
	t.Setenv("COLLECTOR_WLCG_TRAFFIC_JOB_AGENT_MAX", "4")

	config := readTrafficConfig(t, "wlcg:\n  enabled: true\n")

	if config.WLCG.TrafficEnabled {
		t.Error("TrafficEnabled = true, expected COLLECTOR_WLCG_TRAFFIC_ENABLED to switch it off")
	}
	if config.WLCG.TrafficJobAgentMax != 4 {
		t.Errorf("TrafficJobAgentMax = %d, expected 4 from the environment", config.WLCG.TrafficJobAgentMax)
	}
}
