package shoveler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestCollectorPerServerMetricsEnvironment(t *testing.T) {
	for _, tt := range []struct {
		name         string
		config       string
		collectorEnv string
		shovelerEnv  string
		want         bool
	}{
		{name: "disabled by default", config: "{}"},
		{name: "collector prefix enables", config: "metrics:\n  per_server: false\n", collectorEnv: "true", want: true},
		{name: "collector prefix disables", config: "metrics:\n  per_server: true\n", collectorEnv: "false"},
		{name: "shoveler prefix ignored", config: "{}", shovelerEnv: "true"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			t.Setenv("COLLECTOR_METRICS_PER_SERVER", tt.collectorEnv)
			t.Setenv("SHOVELER_METRICS_PER_SERVER", tt.shovelerEnv)
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tt.config), 0600))

			config := Config{}
			config.ReadConfigWithPathAndPrefix(path, "COLLECTOR")
			require.Equal(t, tt.want, config.MetricsPerServer)
		})
	}
}
