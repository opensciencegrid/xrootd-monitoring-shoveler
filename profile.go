package shoveler

import (
	"net/http"
	_ "net/http/pprof"
	"strconv"
)

// StartProfile starts the pprof profiling HTTP server
// The pprof endpoints will be available at:
//   - /debug/pprof/           - index page
//   - /debug/pprof/profile    - CPU profile
//   - /debug/pprof/heap       - heap profile
//   - /debug/pprof/goroutine  - goroutine profile
//   - /debug/pprof/block      - block profile
//   - /debug/pprof/mutex      - mutex profile
//   - /debug/pprof/trace      - execution trace
func StartProfile(profilePort int) {
	// Listen to the pprof requests in a separate goroutine
	go func() {
		listenAddress := ":" + strconv.Itoa(profilePort)
		log.Infoln("Starting pprof profiling server at http://localhost" + listenAddress + "/debug/pprof/")
		// net/http/pprof automatically registers its handlers to DefaultServeMux when imported
		// We use nil to use DefaultServeMux
		err := http.ListenAndServe(listenAddress, nil)
		if err != nil {
			log.Errorln("Failed to start pprof server:", err)
		}
	}()
}
