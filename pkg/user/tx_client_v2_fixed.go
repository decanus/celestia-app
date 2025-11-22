package user

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/celestiaorg/go-square/v3/share"
	"go.opentelemetry.io/otel/trace"
)

// TxState represents the state of a transaction in the v2 client
type TxState int

const (
	TxStateQueued TxState = iota
	TxStatePending
	TxStateCommitted
	TxStateEvicted
	TxStateRejected
)

// TxEntry represents a transaction in the v2 queue system
type TxEntry struct {
	ID       string
	Blobs    []*share.Blob
	Options  []TxOption
	State    TxState
	Sequence uint64
	TxHash   string
	TxBytes  []byte
	Created  time.Time
	ResultCh chan *TxResult
}

// TxResult represents the result of a transaction submission
type TxResult struct {
	Entry    *TxEntry
	Response *TxResponse
	Error    error
}

// AccountQueue manages transactions for a single account in sequence
type AccountQueue struct {
	accountName string
	address     string
	queuedTxs   []*TxEntry
	pendingTxs  map[string]*TxEntry // txHash -> TxEntry
	mutex       sync.RWMutex
	paused      bool
	pauseReason string
}

// NewAccountQueue creates a new AccountQueue for the given account
func NewAccountQueue(accountName, address string) *AccountQueue {
	return &AccountQueue{
		accountName: accountName,
		address:     address,
		queuedTxs:   make([]*TxEntry, 0),
		pendingTxs:  make(map[string]*TxEntry),
	}
}

// Enqueue adds a transaction to the queue
func (aq *AccountQueue) Enqueue(entry *TxEntry) {
	aq.mutex.Lock()
	defer aq.mutex.Unlock()
	
	entry.State = TxStateQueued
	aq.queuedTxs = append(aq.queuedTxs, entry)
}

// Dequeue returns the next transaction to be processed, or nil if queue is empty, paused, or has pending transactions
func (aq *AccountQueue) Dequeue() *TxEntry {
	aq.mutex.Lock()
	defer aq.mutex.Unlock()
	
	// Don't dequeue if paused, empty, or if there are pending transactions (sequential processing)
	if aq.paused || len(aq.queuedTxs) == 0 || len(aq.pendingTxs) > 0 {
		return nil
	}
	
	entry := aq.queuedTxs[0]
	aq.queuedTxs = aq.queuedTxs[1:]
	return entry
}

// MoveToPending moves a transaction from queued to pending state
func (aq *AccountQueue) MoveToPending(entry *TxEntry) {
	aq.mutex.Lock()
	defer aq.mutex.Unlock()
	
	entry.State = TxStatePending
	aq.pendingTxs[entry.TxHash] = entry
}

// RemoveFromPending removes a transaction from pending state
func (aq *AccountQueue) RemoveFromPending(txHash string) *TxEntry {
	aq.mutex.Lock()
	defer aq.mutex.Unlock()
	
	entry, exists := aq.pendingTxs[txHash]
	if exists {
		delete(aq.pendingTxs, txHash)
	}
	return entry
}

// Pause pauses the queue with a reason
func (aq *AccountQueue) Pause(reason string) {
	aq.mutex.Lock()
	defer aq.mutex.Unlock()
	
	aq.paused = true
	aq.pauseReason = reason
}

// Resume resumes the queue
func (aq *AccountQueue) Resume() {
	aq.mutex.Lock()
	defer aq.mutex.Unlock()
	
	aq.paused = false
	aq.pauseReason = ""
}

// IsPaused returns whether the queue is paused
func (aq *AccountQueue) IsPaused() bool {
	aq.mutex.RLock()
	defer aq.mutex.RUnlock()
	return aq.paused
}

// GetPendingCount returns the number of pending transactions
func (aq *AccountQueue) GetPendingCount() int {
	aq.mutex.RLock()
	defer aq.mutex.RUnlock()
	return len(aq.pendingTxs)
}

// GetQueuedCount returns the number of queued transactions
func (aq *AccountQueue) GetQueuedCount() int {
	aq.mutex.RLock()
	defer aq.mutex.RUnlock()
	return len(aq.queuedTxs)
}

// TxClientV2 implements the improved transaction client with race condition fixes
type TxClientV2 struct {
	// Embed the original client for backward compatibility
	*TxClient
	
	// V2 specific fields
	accountQueues map[string]*AccountQueue // accountName -> AccountQueue
	queueMutex    sync.RWMutex
	
	// Background processing
	submitterCtx context.Context
	submitterCancel context.CancelFunc
	monitorCtx   context.Context
	monitorCancel context.CancelFunc
	
	submitInterval time.Duration
	monitorInterval time.Duration
	
	// Event channels
	submissionEvents chan *TxEntry
	confirmationEvents chan *TxResult
}

// NewTxClientV2 creates a new v2 TxClient with race condition fixes
func NewTxClientV2(originalClient *TxClient) *TxClientV2 {
	return &TxClientV2{
		TxClient:           originalClient,
		accountQueues:      make(map[string]*AccountQueue),
		submitInterval:     500 * time.Millisecond,
		monitorInterval:    1 * time.Second,
		submissionEvents:   make(chan *TxEntry, 100),
		confirmationEvents: make(chan *TxResult, 100),
	}
}

// Start initializes the background loops for submission and monitoring
func (client *TxClientV2) Start(ctx context.Context) error {
	// Create contexts for background goroutines
	client.submitterCtx, client.submitterCancel = context.WithCancel(ctx)
	client.monitorCtx, client.monitorCancel = context.WithCancel(ctx)
	
	// Start background loops
	go client.submitterLoop()
	go client.monitorLoop()
	
	return nil
}

// Stop stops all background processing
func (client *TxClientV2) Stop() {
	if client.submitterCancel != nil {
		client.submitterCancel()
	}
	if client.monitorCancel != nil {
		client.monitorCancel()
	}
}

// SubmitPayForBlobV2 submits blobs using the v2 queue system
func (client *TxClientV2) SubmitPayForBlobV2(ctx context.Context, blobs []*share.Blob, opts ...TxOption) (*TxResponse, error) {
	return client.SubmitPayForBlobWithAccountV2(ctx, client.DefaultAccountName(), blobs, opts...)
}

// SubmitPayForBlobWithAccountV2 submits blobs for a specific account using the v2 queue system
func (client *TxClientV2) SubmitPayForBlobWithAccountV2(ctx context.Context, accountName string, blobs []*share.Blob, opts ...TxOption) (*TxResponse, error) {
	// Create result channel
	resultCh := make(chan *TxResult, 1)
	defer close(resultCh)
	
	// Create transaction entry
	entry := &TxEntry{
		ID:       fmt.Sprintf("%s-%d", accountName, time.Now().UnixNano()),
		Blobs:    blobs,
		Options:  opts,
		Created:  time.Now(),
		ResultCh: resultCh,
	}
	
	// Add to appropriate account queue
	if err := client.enqueueTransaction(accountName, entry); err != nil {
		return nil, err
	}
	
	// Wait for result
	select {
	case result := <-resultCh:
		if result.Error != nil {
			return nil, result.Error
		}
		return result.Response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// enqueueTransaction adds a transaction to the appropriate account queue
func (client *TxClientV2) enqueueTransaction(accountName string, entry *TxEntry) error {
	client.queueMutex.Lock()
	defer client.queueMutex.Unlock()
	
	queue, exists := client.accountQueues[accountName]
	if !exists {
		// Get account address
		account := client.Account(accountName)
		if account == nil {
			return fmt.Errorf("account %s not found", accountName)
		}
		
		queue = NewAccountQueue(accountName, account.Address().String())
		client.accountQueues[accountName] = queue
	}
	
	queue.Enqueue(entry)
	return nil
}

// submitterLoop is the background loop responsible for signing and broadcasting transactions
func (client *TxClientV2) submitterLoop() {
	ticker := time.NewTicker(client.submitInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-client.submitterCtx.Done():
			return
		case <-ticker.C:
			client.processQueues()
		}
	}
}

// processQueues processes all account queues for pending transactions
func (client *TxClientV2) processQueues() {
	client.queueMutex.RLock()
	queues := make([]*AccountQueue, 0, len(client.accountQueues))
	for _, queue := range client.accountQueues {
		queues = append(queues, queue)
	}
	client.queueMutex.RUnlock()
	
	for _, queue := range queues {
		client.processAccountQueue(queue)
	}
}

// processAccountQueue processes a single account queue
func (client *TxClientV2) processAccountQueue(queue *AccountQueue) {
	// Skip if queue is paused
	if queue.IsPaused() {
		return
	}
	
	// Process only if there are no pending transactions (sequential processing)
	if queue.GetPendingCount() > 0 {
		return
	}
	
	// Get next queued transaction
	entry := queue.Dequeue()
	if entry == nil {
		return
	}
	
	// Submit the transaction
	if err := client.submitTransaction(queue, entry); err != nil {
		// Handle submission error
		entry.ResultCh <- &TxResult{
			Entry: entry,
			Error: err,
		}
		return
	}
	
	// Move to pending
	queue.MoveToPending(entry)
	
	// Send to monitor
	select {
	case client.submissionEvents <- entry:
	default:
		// Channel full, handle gracefully
	}
}

// submitTransaction handles the actual submission of a transaction
func (client *TxClientV2) submitTransaction(queue *AccountQueue, entry *TxEntry) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	span := trace.SpanFromContext(ctx)
	span.AddEvent("txclientv2: submitting transaction", trace.WithAttributes())
	
	// Use original client's broadcast method with sequence mismatch handling
	txResp, err := client.TxClient.BroadcastPayForBlobWithAccount(ctx, queue.accountName, entry.Blobs, entry.Options...)
	if err != nil {
		// Handle sequence mismatch by pausing queue and recovering
		if client.isSequenceMismatchError(err) {
			queue.Pause(fmt.Sprintf("sequence mismatch: %v", err))
			go client.recoverFromSequenceMismatch(queue, err)
		}
		return err
	}
	
	// Store transaction details
	entry.TxHash = txResp.TxHash
	entry.Sequence = client.Account(queue.accountName).Sequence()
	
	return nil
}

// monitorLoop is the background loop responsible for monitoring pending transactions
func (client *TxClientV2) monitorLoop() {
	ticker := time.NewTicker(client.monitorInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-client.monitorCtx.Done():
			return
		case entry := <-client.submissionEvents:
			go client.monitorTransaction(entry)
		case <-ticker.C:
			client.monitorAllPendingTransactions()
		}
	}
}

// monitorTransaction monitors a specific transaction until completion
func (client *TxClientV2) monitorTransaction(entry *TxEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	
	// Get account queue
	client.queueMutex.RLock()
	queue := client.accountQueues[client.getAccountNameFromEntry(entry)]
	client.queueMutex.RUnlock()
	
	if queue == nil {
		entry.ResultCh <- &TxResult{
			Entry: entry,
			Error: fmt.Errorf("account queue not found"),
		}
		return
	}
	
	// Monitor transaction status
	txResp, err := client.TxClient.ConfirmTx(ctx, entry.TxHash)
	
	// Remove from pending
	queue.RemoveFromPending(entry.TxHash)
	
	if err != nil {
		// Handle different types of failures
		if client.isEvictionError(err) {
			entry.State = TxStateEvicted
			// Attempt resubmission
			go client.handleEviction(queue, entry)
		} else {
			entry.State = TxStateRejected
			// Reset sequence for subsequent transactions
			go client.handleRejection(queue, entry, err)
		}
		
		entry.ResultCh <- &TxResult{
			Entry: entry,
			Error: err,
		}
		return
	}
	
	// Success
	entry.State = TxStateCommitted
	entry.ResultCh <- &TxResult{
		Entry:    entry,
		Response: txResp,
	}
}

// monitorAllPendingTransactions checks the status of all pending transactions
func (client *TxClientV2) monitorAllPendingTransactions() {
	client.queueMutex.RLock()
	defer client.queueMutex.RUnlock()
	
	for _, queue := range client.accountQueues {
		queue.mutex.RLock()
		for _, entry := range queue.pendingTxs {
			// Check if transaction has been pending for too long
			if time.Since(entry.Created) > 10*time.Minute {
				go client.handleStaleTransaction(queue, entry)
			}
		}
		queue.mutex.RUnlock()
	}
}

// Recovery and error handling methods

// recoverFromSequenceMismatch handles sequence mismatch recovery
func (client *TxClientV2) recoverFromSequenceMismatch(queue *AccountQueue, err error) {
	// TODO: Extract expected sequence from error and update signer
	// For now, just resume after a delay
	time.Sleep(2 * time.Second)
	queue.Resume()
}

// handleEviction handles transaction eviction by attempting resubmission
func (client *TxClientV2) handleEviction(queue *AccountQueue, entry *TxEntry) {
	// Implement eviction handling logic
	// For now, treat as failure
	time.Sleep(1 * time.Second)
}

// handleRejection handles transaction rejection
func (client *TxClientV2) handleRejection(queue *AccountQueue, entry *TxEntry, err error) {
	// Reset sequence and resume queue
	// Implementation would depend on the specific rejection reason
	queue.Resume()
}

// handleStaleTransaction handles transactions that have been pending too long
func (client *TxClientV2) handleStaleTransaction(queue *AccountQueue, entry *TxEntry) {
	// Remove from pending and treat as timeout
	queue.RemoveFromPending(entry.TxHash)
	entry.ResultCh <- &TxResult{
		Entry: entry,
		Error: fmt.Errorf("transaction timeout"),
	}
}

// Helper methods

// getAccountNameFromEntry extracts account name from transaction entry
func (client *TxClientV2) getAccountNameFromEntry(entry *TxEntry) string {
	// Implementation would extract this from the entry or maintain mapping
	return client.DefaultAccountName() // Simplified for now
}

// isSequenceMismatchError checks if error is due to sequence mismatch
func (client *TxClientV2) isSequenceMismatchError(err error) bool {
	// Implementation would check error type/message
	return false // Simplified for now
}

// isEvictionError checks if error is due to transaction eviction
func (client *TxClientV2) isEvictionError(err error) bool {
	// Implementation would check error type/message
	return false // Simplified for now
}

// GetQueueStatus returns status information about account queues
func (client *TxClientV2) GetQueueStatus() map[string]map[string]interface{} {
	client.queueMutex.RLock()
	defer client.queueMutex.RUnlock()
	
	status := make(map[string]map[string]interface{})
	
	for accountName, queue := range client.accountQueues {
		status[accountName] = map[string]interface{}{
			"queued":     queue.GetQueuedCount(),
			"pending":    queue.GetPendingCount(),
			"paused":     queue.IsPaused(),
			"pauseReason": queue.pauseReason,
		}
	}
	
	return status
}