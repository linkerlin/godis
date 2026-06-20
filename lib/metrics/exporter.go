package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync/atomic"

	"github.com/linkerlin/godis/database"
	"github.com/linkerlin/godis/lib/stats"
	"github.com/linkerlin/godis/tcp"
)

// WritePrometheus writes Godis metrics in Prometheus text exposition format.
func WritePrometheus(w io.Writer) {
	inputBytes, outputBytes := stats.GetStats()
	connected := atomic.LoadInt32(&tcp.ClientCounter)

	fmt.Fprintf(w, "# HELP godis_net_input_bytes Total network input bytes\n")
	fmt.Fprintf(w, "# TYPE godis_net_input_bytes counter\n")
	fmt.Fprintf(w, "godis_net_input_bytes %d\n", inputBytes)

	fmt.Fprintf(w, "# HELP godis_net_output_bytes Total network output bytes\n")
	fmt.Fprintf(w, "# TYPE godis_net_output_bytes counter\n")
	fmt.Fprintf(w, "godis_net_output_bytes %d\n", outputBytes)

	fmt.Fprintf(w, "# HELP godis_connected_clients Current connected clients\n")
	fmt.Fprintf(w, "# TYPE godis_connected_clients gauge\n")
	fmt.Fprintf(w, "godis_connected_clients %d\n", connected)

	fmt.Fprintf(w, "# HELP godis_tracking_clients Clients with CLIENT TRACKING enabled\n")
	fmt.Fprintf(w, "# TYPE godis_tracking_clients gauge\n")
	fmt.Fprintf(w, "godis_tracking_clients %d\n", database.GetTrackingClientsCount())

	fmt.Fprintf(w, "# HELP godis_slowlog_length Current slowlog entry count\n")
	fmt.Fprintf(w, "# TYPE godis_slowlog_length gauge\n")
	fmt.Fprintf(w, "godis_slowlog_length %d\n", database.SlowLogLength())

	fmt.Fprintf(w, "# HELP godis_rejected_connections_total Connections rejected (e.g. maxclients)\n")
	fmt.Fprintf(w, "# TYPE godis_rejected_connections_total counter\n")
	fmt.Fprintf(w, "godis_rejected_connections_total %d\n", tcp.GetRejectedConnections())

	cmdStats := database.GetAllCommandStats()
	names := make([]string, 0, len(cmdStats))
	for name := range cmdStats {
		names = append(names, name)
	}
	sort.Strings(names)

	fmt.Fprintf(w, "# HELP godis_commands_total Total executed commands\n")
	fmt.Fprintf(w, "# TYPE godis_commands_total counter\n")
	for _, name := range names {
		stat := cmdStats[name]
		fmt.Fprintf(w, "godis_commands_total{command=%q} %d\n", name, stat.Calls())
	}

	fmt.Fprintf(w, "# HELP godis_command_failures_total Total failed command replies\n")
	fmt.Fprintf(w, "# TYPE godis_command_failures_total counter\n")
	for _, name := range names {
		stat := cmdStats[name]
		fmt.Fprintf(w, "godis_command_failures_total{command=%q} %d\n", name, stat.FailedCalls())
	}

	fmt.Fprintf(w, "# HELP godis_command_duration_usec_total Total command duration in microseconds\n")
	fmt.Fprintf(w, "# TYPE godis_command_duration_usec_total counter\n")
	for _, name := range names {
		stat := cmdStats[name]
		fmt.Fprintf(w, "godis_command_duration_usec_total{command=%q} %d\n", name, stat.UsecTotal())
	}
}
