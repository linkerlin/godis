package core

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/hdt3213/godis/lib/logger"
)

// RebalanceManager manages cluster slot rebalancing
type RebalanceManager struct {
	mu sync.RWMutex

	cluster *Cluster

	// Rebalance state
	isRebalancing   bool
	rebalanceTask   *RebalanceTask
	rebalanceConfig *RebalanceConfig
}

// RebalanceConfig contains configuration for rebalancing
type RebalanceConfig struct {
	// Threshold for triggering automatic rebalancing
	// If max slots - min slots > Threshold, trigger rebalance
	Threshold int

	// Max concurrent migrations
	MaxConcurrentMigrations int

	// Whether to enable auto-rebalancing
	AutoRebalance bool

	// Interval for checking if rebalancing is needed
	CheckInterval time.Duration

	// Whether to use weighted rebalancing based on node capacity
	UseWeighted bool

	// Node weights (node address -> weight)
	NodeWeights map[string]int
}

// DefaultRebalanceConfig returns default rebalancing configuration
func DefaultRebalanceConfig() *RebalanceConfig {
	return &RebalanceConfig{
		Threshold:               10,
		MaxConcurrentMigrations: 1,
		AutoRebalance:           false,
		CheckInterval:           time.Minute * 5,
		UseWeighted:             false,
		NodeWeights:             make(map[string]int),
	}
}

// RebalanceTask represents an ongoing rebalancing task
type RebalanceTask struct {
	ID        string
	StartTime time.Time
	EndTime   *time.Time

	// Source -> Dest migrations
	Migrations []SlotMigration

	// Current migration index
	CurrentIndex int

	// Status
	Status RebalanceStatus

	// Progress
	CompletedMigrations int
	FailedMigrations    int

	// Error message if failed
	Error string
}

// RebalanceStatus represents the status of a rebalance task
type RebalanceStatus int

const (
	RebalanceStatusPending RebalanceStatus = iota
	RebalanceStatusRunning
	RebalanceStatusCompleted
	RebalanceStatusFailed
	RebalanceStatusCancelled
)

// SlotMigration represents a single slot migration
type SlotMigration struct {
	Slot    uint32
	From    string
	To      string
	Status  MigrationStatus
	Message string
}

// MigrationStatus represents the status of a slot migration
type MigrationStatus int

const (
	MigrationStatusPending MigrationStatus = iota
	MigrationStatusRunning
	MigrationStatusCompleted
	MigrationStatusFailed
)

// SlotDistribution represents the distribution of slots across nodes
type SlotDistribution struct {
	Node       string
	SlotCount  int
	Slots      []uint32
	Percentage float64
}

// NewRebalanceManager creates a new rebalancing manager
func NewRebalanceManager(cluster *Cluster) *RebalanceManager {
	return &RebalanceManager{
		cluster:         cluster,
		rebalanceConfig: DefaultRebalanceConfig(),
	}
}

// SetConfig sets the rebalancing configuration
func (rm *RebalanceManager) SetConfig(config *RebalanceConfig) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.rebalanceConfig = config
	logger.Infof("Rebalance configuration updated: threshold=%d, auto=%v, weighted=%v",
		config.Threshold, config.AutoRebalance, config.UseWeighted)
}

// GetConfig gets the current rebalancing configuration
func (rm *RebalanceManager) GetConfig() *RebalanceConfig {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	return rm.rebalanceConfig
}

// AnalyzeDistribution analyzes the current slot distribution
func (rm *RebalanceManager) AnalyzeDistribution() ([]SlotDistribution, error) {
	rm.mu.RLock()
	cluster := rm.cluster
	rm.mu.RUnlock()

	// Get slot distribution using raft FSM
	slotToNode := make(map[uint32]string)
	for i := uint32(0); i < uint32(SlotCount); i++ {
		node := cluster.PickNode(i)
		slotToNode[i] = node
	}

	// Count slots per node
	nodeSlotCounts := make(map[string][]uint32)
	for slot, node := range slotToNode {
		if _, ok := nodeSlotCounts[node]; !ok {
			nodeSlotCounts[node] = make([]uint32, 0)
		}
		nodeSlotCounts[node] = append(nodeSlotCounts[node], slot)
	}

	distributions := make([]SlotDistribution, 0, len(nodeSlotCounts))
	totalSlots := len(slotToNode)

	for node, slots := range nodeSlotCounts {
		distributions = append(distributions, SlotDistribution{
			Node:       node,
			SlotCount:  len(slots),
			Slots:      slots,
			Percentage: float64(len(slots)) * 100 / float64(totalSlots),
		})
	}

	// Sort by slot count descending
	sort.Slice(distributions, func(i, j int) bool {
		return distributions[i].SlotCount > distributions[j].SlotCount
	})

	return distributions, nil
}

// IsBalanced checks if the cluster is balanced
func (rm *RebalanceManager) IsBalanced() (bool, *BalanceInfo, error) {
	distributions, err := rm.AnalyzeDistribution()
	if err != nil {
		return false, nil, err
	}

	if len(distributions) == 0 {
		return true, nil, nil
	}

	maxSlots := distributions[0].SlotCount
	minSlots := distributions[len(distributions)-1].SlotCount
	diff := maxSlots - minSlots

	threshold := rm.rebalanceConfig.Threshold

	info := &BalanceInfo{
		MaxSlots:      maxSlots,
		MinSlots:      minSlots,
		Difference:    diff,
		Threshold:     threshold,
		IsBalanced:    diff <= threshold,
		Distributions: distributions,
	}

	return info.IsBalanced, info, nil
}

// BalanceInfo contains information about cluster balance
type BalanceInfo struct {
	MaxSlots      int
	MinSlots      int
	Difference    int
	Threshold     int
	IsBalanced    bool
	Distributions []SlotDistribution
}

// CalculateRebalancePlan calculates a rebalancing plan
func (rm *RebalanceManager) CalculateRebalancePlan() (*RebalanceTask, error) {
	distributions, err := rm.AnalyzeDistribution()
	if err != nil {
		return nil, err
	}

	if len(distributions) < 2 {
		return nil, fmt.Errorf("need at least 2 nodes to rebalance")
	}

	task := &RebalanceTask{
		ID:           generateTaskID(),
		StartTime:    time.Now(),
		Migrations:   make([]SlotMigration, 0),
		Status:       RebalanceStatusPending,
	}

	// Calculate target slots per node
	targetSlots := rm.calculateTargetSlots(distributions)

	// Separate nodes into donors (have too many slots) and receivers (need more slots)
	donors := make([]SlotDistribution, 0)
	receivers := make([]SlotDistribution, 0)

	for _, dist := range distributions {
		target := targetSlots[dist.Node]
		if dist.SlotCount > target {
			donors = append(donors, dist)
		} else if dist.SlotCount < target {
			receivers = append(receivers, dist)
		}
	}

	// Sort donors by excess slots (descending)
	sort.Slice(donors, func(i, j int) bool {
		return donors[i].SlotCount-targetSlots[donors[i].Node] >
			donors[j].SlotCount-targetSlots[donors[j].Node]
	})

	// Sort receivers by deficit slots (descending)
	sort.Slice(receivers, func(i, j int) bool {
		return targetSlots[receivers[i].Node]-receivers[i].SlotCount >
			targetSlots[receivers[j].Node]-receivers[j].SlotCount
	})

	// Generate migrations
	for _, donor := range donors {
		target := targetSlots[donor.Node]
		excess := donor.SlotCount - target

		if excess <= 0 {
			continue
		}

		// Allocate slots to receivers
		for i := 0; i < excess && len(receivers) > 0; {
			receiver := receivers[0]
			receiverTarget := targetSlots[receiver.Node]
			deficit := receiverTarget - receiver.SlotCount

			if deficit <= 0 {
				receivers = receivers[1:]
				continue
			}

			// Move one slot
			slot := donor.Slots[0]
			donor.Slots = donor.Slots[1:]

			task.Migrations = append(task.Migrations, SlotMigration{
				Slot:   slot,
				From:   donor.Node,
				To:     receiver.Node,
				Status: MigrationStatusPending,
			})

			receiver.Slots = append(receiver.Slots, slot)
			receiver.SlotCount++
			donor.SlotCount--
			i++

			if receiver.SlotCount >= receiverTarget {
				receivers = receivers[1:]
			}
		}
	}

	logger.Infof("Rebalance plan calculated: %d migrations needed", len(task.Migrations))

	return task, nil
}

// calculateTargetSlots calculates the target number of slots for each node
func (rm *RebalanceManager) calculateTargetSlots(distributions []SlotDistribution) map[string]int {
	totalSlots := 0
	for _, dist := range distributions {
		totalSlots += dist.SlotCount
	}

	targetSlots := make(map[string]int)

	if !rm.rebalanceConfig.UseWeighted {
		// Equal distribution
		avg := totalSlots / len(distributions)
		remainder := totalSlots % len(distributions)

		for i, dist := range distributions {
			target := avg
			if i < remainder {
				target++
			}
			targetSlots[dist.Node] = target
		}
	} else {
		// Weighted distribution
		totalWeight := 0
		nodeWeights := make(map[string]int)

		for _, dist := range distributions {
			weight := rm.rebalanceConfig.NodeWeights[dist.Node]
			if weight == 0 {
				weight = 1 // Default weight
			}
			nodeWeights[dist.Node] = weight
			totalWeight += weight
		}

		allocated := 0
		for _, dist := range distributions {
			weight := nodeWeights[dist.Node]
			target := (totalSlots * weight) / totalWeight
			targetSlots[dist.Node] = target
			allocated += target
		}

		// Distribute remaining slots
		remainder := totalSlots - allocated
		for i := 0; i < remainder; i++ {
			node := distributions[i%len(distributions)].Node
			targetSlots[node]++
		}
	}

	return targetSlots
}

// StartRebalance starts the rebalancing process
func (rm *RebalanceManager) StartRebalance() (*RebalanceTask, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.isRebalancing {
		return nil, fmt.Errorf("rebalancing already in progress")
	}

	// Check if balanced
	balanced, info, err := rm.IsBalanced()
	if err != nil {
		return nil, err
	}

	if balanced {
		return nil, fmt.Errorf("cluster is already balanced (max diff: %d, threshold: %d)",
			info.Difference, info.Threshold)
	}

	// Calculate plan
	task, err := rm.CalculateRebalancePlan()
	if err != nil {
		return nil, err
	}

	if len(task.Migrations) == 0 {
		return nil, fmt.Errorf("no migrations needed")
	}

	rm.rebalanceTask = task
	rm.isRebalancing = true

	// Start rebalancing in background
	go rm.executeRebalance(task)

	logger.Infof("Rebalance started: task=%s, migrations=%d", task.ID, len(task.Migrations))

	return task, nil
}

// executeRebalance executes the rebalancing task
func (rm *RebalanceManager) executeRebalance(task *RebalanceTask) {
	defer func() {
		rm.mu.Lock()
		rm.isRebalancing = false
		rm.mu.Unlock()

		now := time.Now()
		task.EndTime = &now
	}()

	task.Status = RebalanceStatusRunning

	for i := range task.Migrations {
		task.CurrentIndex = i
		migration := &task.Migrations[i]

		logger.Infof("Executing migration: slot %d from %s to %s",
			migration.Slot, migration.From, migration.To)

		migration.Status = MigrationStatusRunning

		// Execute migration using existing cluster migration mechanism
		err := rm.migrateSlot(migration.Slot, migration.From, migration.To)

		if err != nil {
			migration.Status = MigrationStatusFailed
			migration.Message = err.Error()
			task.FailedMigrations++
			logger.Errorf("Migration failed: slot %d, error: %v", migration.Slot, err)

			// Continue with next migration
		} else {
			migration.Status = MigrationStatusCompleted
			task.CompletedMigrations++
			logger.Infof("Migration completed: slot %d", migration.Slot)
		}
	}

	// Check if all migrations completed
	if task.FailedMigrations == 0 {
		task.Status = RebalanceStatusCompleted
		logger.Infof("Rebalance completed successfully: task=%s", task.ID)
	} else if task.CompletedMigrations == 0 {
		task.Status = RebalanceStatusFailed
		task.Error = "all migrations failed"
		logger.Errorf("Rebalance failed: task=%s", task.ID)
	} else {
		task.Status = RebalanceStatusCompleted
		logger.Warn("Rebalance completed with errors: task=", task.ID, ", completed=", task.CompletedMigrations, ", failed=", task.FailedMigrations)
	}
}

// migrateSlot migrates a single slot using cluster's existing mechanism
func (rm *RebalanceManager) migrateSlot(slot uint32, from, to string) error {
	rm.mu.RLock()
	cluster := rm.cluster
	rm.mu.RUnlock()

	// Use the cluster's existing migration mechanism
	// This calls the existing rebalance logic in node_manager.go
	return cluster.doMigrateSlot(slot, from, to)
}

// CancelRebalance cancels the current rebalancing
func (rm *RebalanceManager) CancelRebalance() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.isRebalancing {
		return fmt.Errorf("no rebalancing in progress")
	}

	if rm.rebalanceTask != nil {
		rm.rebalanceTask.Status = RebalanceStatusCancelled
	}

	rm.isRebalancing = false
	logger.Info("Rebalance cancelled")

	return nil
}

// GetRebalanceStatus gets the current rebalancing status
func (rm *RebalanceManager) GetRebalanceStatus() *RebalanceTask {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	return rm.rebalanceTask
}

// IsRebalancing returns true if rebalancing is in progress
func (rm *RebalanceManager) IsRebalancing() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	return rm.isRebalancing
}

// generateTaskID generates a unique task ID
func generateTaskID() string {
	return fmt.Sprintf("rebalance-%d", time.Now().UnixNano())
}

// StartAutoRebalance starts the auto-rebalancing goroutine
func (rm *RebalanceManager) StartAutoRebalance() {
	go func() {
		ticker := time.NewTicker(rm.rebalanceConfig.CheckInterval)
		defer ticker.Stop()

		for range ticker.C {
			rm.mu.RLock()
			autoEnabled := rm.rebalanceConfig.AutoRebalance
			isRebalancing := rm.isRebalancing
			rm.mu.RUnlock()

			if !autoEnabled || isRebalancing {
				continue
			}

			balanced, _, err := rm.IsBalanced()
			if err != nil {
				logger.Errorf("Failed to check balance: %v", err)
				continue
			}

			if !balanced {
				logger.Info("Cluster is unbalanced, starting auto-rebalance")
				_, err := rm.StartRebalance()
				if err != nil {
					logger.Errorf("Failed to start auto-rebalance: %v", err)
				}
			}
		}
	}()
}

// GetMigrationProgress returns the progress of current rebalancing
func (rm *RebalanceManager) GetMigrationProgress() (completed, total int, percentage float64) {
	rm.mu.RLock()
	task := rm.rebalanceTask
	rm.mu.RUnlock()

	if task == nil {
		return 0, 0, 0
	}

	completed = task.CompletedMigrations
	total = len(task.Migrations)
	if total > 0 {
		percentage = float64(completed) * 100 / float64(total)
	}

	return completed, total, math.Round(percentage*100) / 100
}

// doMigrateSlot is a wrapper for the cluster's internal slot migration
// This should be implemented to use the existing migration mechanism
func (cluster *Cluster) doMigrateSlot(slot uint32, from, to string) error {
	// This is a placeholder - actual implementation should use
	// the existing migration mechanism from node_manager.go
	logger.Infof("Migrating slot %d from %s to %s", slot, from, to)
	// TODO: Implement using existing cluster migration logic
	return nil
}
