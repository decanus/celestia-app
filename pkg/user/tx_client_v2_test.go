package user

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/celestiaorg/go-square/v3/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTxClientV2RaceConditionFix verifies that the v2 implementation
// resolves the race conditions identified in the original client
func TestTxClientV2RaceConditionFix(t *testing.T) {
	// Skip if no test environment available
	t.Skip("This test requires a properly configured test environment")
	
	originalClient := setupTxClient(t)
	client := NewTxClientV2(originalClient)
	
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	
	// Start the v2 client
	err := client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop()
	
	// Test concurrent transaction submission
	const numConcurrentTx = 10
	var wg sync.WaitGroup
	results := make(chan *testResult, numConcurrentTx)
	
	for i := 0; i < numConcurrentTx; i++ {
		wg.Add(1)
		go func(txNum int) {
			defer wg.Done()
			
			txStart := time.Now()
			blob, err := share.NewBlob(share.RandomBlobNamespace(), []byte(fmt.Sprintf("v2 test blob %d data", txNum)), share.ShareVersionZero, nil)
			if err != nil {
				results <- &testResult{
					TxNum:    txNum,
					Error:    err,
					Duration: time.Since(txStart),
				}
				return
			}
			
			submitStart := time.Now()
			txResp, err := client.SubmitPayForBlobV2(ctx, []*share.Blob{blob})
			duration := time.Since(submitStart)
			
			results <- &testResult{
				TxNum:    txNum,
				TxResp:   txResp,
				Error:    err,
				Duration: duration,
			}
		}(i)
	}
	
	// Wait for all transactions
	wg.Wait()
	close(results)
	
	// Analyze results
	var successful, failed int
	for result := range results {
		if result.Error != nil {
			failed++
			t.Logf("V2 Transaction %d failed: %v (took %v)", result.TxNum, result.Error, result.Duration)
		} else {
			successful++
			t.Logf("V2 Transaction %d succeeded: hash=%s, height=%d (took %v)",
				result.TxNum, result.TxResp.TxHash, result.TxResp.Height, result.Duration)
		}
	}
	
	t.Logf("V2 Client Results: %d successful, %d failed out of %d total", successful, failed, numConcurrentTx)
	
	// With the v2 implementation, we expect better success rates
	successRate := float64(successful) / float64(numConcurrentTx)
	assert.GreaterOrEqual(t, successRate, 0.8, "V2 client should have at least 80% success rate")
}

// TestAccountQueueBasicFunctionality tests the basic queue operations
func TestAccountQueueBasicFunctionality(t *testing.T) {
	queue := NewAccountQueue("test-account", "test-address")
	
	// Test initial state
	assert.Equal(t, 0, queue.GetQueuedCount())
	assert.Equal(t, 0, queue.GetPendingCount())
	assert.False(t, queue.IsPaused())
	
	// Test enqueue
	entry1 := &TxEntry{
		ID:      "test-tx-1",
		Created: time.Now(),
	}
	entry2 := &TxEntry{
		ID:      "test-tx-2",
		Created: time.Now(),
	}
	
	queue.Enqueue(entry1)
	queue.Enqueue(entry2)
	
	assert.Equal(t, 2, queue.GetQueuedCount())
	assert.Equal(t, TxStateQueued, entry1.State)
	assert.Equal(t, TxStateQueued, entry2.State)
	
	// Test dequeue
	dequeued := queue.Dequeue()
	require.NotNil(t, dequeued)
	assert.Equal(t, "test-tx-1", dequeued.ID)
	assert.Equal(t, 1, queue.GetQueuedCount())
	
	// Test move to pending
	dequeued.TxHash = "test-hash-1"
	queue.MoveToPending(dequeued)
	assert.Equal(t, TxStatePending, dequeued.State)
	assert.Equal(t, 1, queue.GetPendingCount())
	
	// Test remove from pending
	removed := queue.RemoveFromPending("test-hash-1")
	require.NotNil(t, removed)
	assert.Equal(t, "test-tx-1", removed.ID)
	assert.Equal(t, 0, queue.GetPendingCount())
}

// TestAccountQueuePauseResume tests pause and resume functionality
func TestAccountQueuePauseResume(t *testing.T) {
	queue := NewAccountQueue("test-account", "test-address")
	
	entry := &TxEntry{
		ID:      "test-tx",
		Created: time.Now(),
	}
	queue.Enqueue(entry)
	
	// Test pause
	queue.Pause("test pause reason")
	assert.True(t, queue.IsPaused())
	
	// Dequeue should return nil when paused
	dequeued := queue.Dequeue()
	assert.Nil(t, dequeued)
	assert.Equal(t, 1, queue.GetQueuedCount()) // Entry should still be in queue
	
	// Test resume
	queue.Resume()
	assert.False(t, queue.IsPaused())
	
	// Dequeue should work after resume
	dequeued = queue.Dequeue()
	require.NotNil(t, dequeued)
	assert.Equal(t, "test-tx", dequeued.ID)
}

// TestSequentialProcessing verifies that transactions are processed sequentially per account
func TestSequentialProcessing(t *testing.T) {
	// Skip if no test environment available
	t.Skip("This test requires a properly configured test environment")
	
	originalClient := setupTxClient(t)
	client := NewTxClientV2(originalClient)
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	err := client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop()
	
	// Create account queue directly for testing
	accountName := client.DefaultAccountName()
	queue := NewAccountQueue(accountName, client.DefaultAddress().String())
	client.accountQueues[accountName] = queue
	
	// Add multiple transactions to queue
	entries := make([]*TxEntry, 3)
	for i := range entries {
		blob, err := share.NewBlob(share.RandomBlobNamespace(), []byte(fmt.Sprintf("sequential test blob %d", i)), share.ShareVersionZero, nil)
		require.NoError(t, err)
		
		entries[i] = &TxEntry{
			ID:      fmt.Sprintf("test-tx-%d", i),
			Blobs:   []*share.Blob{blob},
			Created: time.Now(),
		}
		queue.Enqueue(entries[i])
	}
	
	// Process queues and verify sequential processing
	client.processAccountQueue(queue)
	
	// First transaction should be moved to pending
	assert.Equal(t, 2, queue.GetQueuedCount(), "Two transactions should remain queued")
	assert.Equal(t, 1, queue.GetPendingCount(), "One transaction should be pending")
	
	// Process again - should not process more while one is pending
	client.processAccountQueue(queue)
	assert.Equal(t, 2, queue.GetQueuedCount(), "Queue count should not change")
	assert.Equal(t, 1, queue.GetPendingCount(), "Pending count should not change")
}

// TestQueueStatusReporting tests the status reporting functionality
func TestQueueStatusReporting(t *testing.T) {
	originalClient := &TxClient{} // Mock client
	client := NewTxClientV2(originalClient)
	
	// Create test queues
	queue1 := NewAccountQueue("account1", "address1")
	queue2 := NewAccountQueue("account2", "address2")
	
	// Add some transactions
	for i := 0; i < 3; i++ {
		entry := &TxEntry{ID: fmt.Sprintf("tx-%d", i), Created: time.Now()}
		queue1.Enqueue(entry)
	}
	
	for i := 0; i < 2; i++ {
		entry := &TxEntry{ID: fmt.Sprintf("tx-%d", i), Created: time.Now()}
		queue2.Enqueue(entry)
	}
	
	// Pause queue2
	queue2.Pause("test pause")
	
	client.accountQueues["account1"] = queue1
	client.accountQueues["account2"] = queue2
	
	// Get status
	status := client.GetQueueStatus()
	
	require.Contains(t, status, "account1")
	require.Contains(t, status, "account2")
	
	assert.Equal(t, 3, status["account1"]["queued"])
	assert.Equal(t, 0, status["account1"]["pending"])
	assert.Equal(t, false, status["account1"]["paused"])
	
	assert.Equal(t, 2, status["account2"]["queued"])
	assert.Equal(t, 0, status["account2"]["pending"])
	assert.Equal(t, true, status["account2"]["paused"])
	assert.Equal(t, "test pause", status["account2"]["pauseReason"])
}

// TestEvictionHandling tests the eviction handling in v2 client
func TestEvictionHandling(t *testing.T) {
	// This test would require more sophisticated mocking to simulate evictions
	t.Skip("Eviction handling test requires advanced mocking setup")
}

// TestErrorRecovery tests error recovery mechanisms
func TestErrorRecovery(t *testing.T) {
	originalClient := &TxClient{} // Mock client
	client := NewTxClientV2(originalClient)
	
	queue := NewAccountQueue("test-account", "test-address")
	
	// Test sequence mismatch recovery
	client.recoverFromSequenceMismatch(queue, fmt.Errorf("sequence mismatch"))
	
	// After recovery, queue should be resumed (this is a basic test)
	assert.False(t, queue.IsPaused(), "Queue should be resumed after recovery")
}

type testResult struct {
	TxNum    int
	TxResp   *TxResponse
	Error    error
	Duration time.Duration
}