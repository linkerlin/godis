package database

// clusterInfoSectionProvider returns CLUSTER INFO body lines (newlines as \n).
// Registered by cluster.NewCluster so INFO cluster shares FSM topology with CLUSTER INFO.
var clusterInfoSectionProvider func() string

// SetClusterInfoSectionProvider registers or clears the cluster INFO section callback.
func SetClusterInfoSectionProvider(fn func() string) {
	clusterInfoSectionProvider = fn
}
