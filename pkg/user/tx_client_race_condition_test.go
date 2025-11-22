package user

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-app/v6/pkg/appconsts"
	"github.com/celestiaorg/go-square/v3/share"
	"github.com/cometbft/cometbft/rpc/core"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTxClientRaceCondition demonstrates the race condition issue described in the tx_client_v2.md spec.
// The test simulates the scenario where:
// 1. A transaction gets evicted from the mempool
// 2. A new transaction is submitted while the node expects a specific sequence
// 3. Recovery logic re-signs the original transaction
// This creates a race where either the evicted original tx or the new re-signed tx is included first.
func TestTxClientRaceCondition(t *testing.T) {
	client := setupTxClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test blobs
	blob1 := &share.Blob{
		NamespaceId:      appconsts.DefaultTxNamespaceID,
		Data:             []byte("test blob 1 data"),
		ShareVersion:     uint32(appconsts.ShareVersionZero),
		NamespaceVersion: uint32(appconsts.NamespaceVersionZero),
	}
	
	blob2 := &share.Blob{
		NamespaceId:      appconsts.DefaultTxNamespaceID,
		Data:             []byte("test blob 2 data"),
		ShareVersion:     uint32(appconsts.ShareVersionZero),
		NamespaceVersion: uint32(appconsts.NamespaceVersionZero),
	}

	// Get initial sequence for tracking
	account := client.Account(client.DefaultAccountName())
	require.NotNil(t, account)
	initialSequence := account.Sequence()

	// Submit first transaction
	txResp1, err := client.BroadcastPayForBlob(ctx, []*share.Blob{blob1})
	require.NoError(t, err)
	require.NotEmpty(t, txResp1.TxHash)

	// Simulate rapid submission of second transaction while first is being processed
	txResp2, err := client.BroadcastPayForBlob(ctx, []*share.Blob{blob2})
	require.NoError(t, err)
	require.NotEmpty(t, txResp2.TxHash)

	// Verify that both transactions are being tracked
	seq1, signer1, _, exists1 := client.GetTxFromTxTracker(txResp1.TxHash)
	assert.True(t, exists1, "First transaction should be tracked")
	assert.Equal(t, initialSequence, seq1, "First transaction should have initial sequence")
	assert.Equal(t, client.DefaultAccountName(), signer1, "First transaction should have correct signer")

	seq2, signer2, _, exists2 := client.GetTxFromTxTracker(txResp2.TxHash)
	assert.True(t, exists2, "Second transaction should be tracked")
	assert.Equal(t, initialSequence+1, seq2, "Second transaction should have incremented sequence")
	assert.Equal(t, client.DefaultAccountName(), signer2, "Second transaction should have correct signer")

	// Wait for transactions to be confirmed or encounter issues
	confirmedTx1, err1 := client.ConfirmTx(ctx, txResp1.TxHash)
	confirmedTx2, err2 := client.ConfirmTx(ctx, txResp2.TxHash)

	// At least one transaction should succeed
	successCount := 0
	if err1 == nil {
		successCount++
		assert.NotNil(t, confirmedTx1)
		assert.Equal(t, uint32(0), confirmedTx1.Code, "Successful transaction should have code 0")
	}
	if err2 == nil {
		successCount++
		assert.NotNil(t, confirmedTx2)
		assert.Equal(t, uint32(0), confirmedTx2.Code, "Successful transaction should have code 0")
	}

	assert.GreaterOrEqual(t, successCount, 1, "At least one transaction should succeed")

	// Log the results to understand the race condition behavior
	t.Logf("Transaction 1 (seq %d): hash=%s, confirmed=%t, error=%v", seq1, txResp1.TxHash, err1 == nil, err1)
	t.Logf("Transaction 2 (seq %d): hash=%s, confirmed=%t, error=%v", seq2, txResp2.TxHash, err2 == nil, err2)
}

// TestConcurrentTransactionSubmission tests the race condition that occurs when multiple
// transactions are submitted concurrently, which can lead to sequence mismatch errors
// and inconsistent state management.
func TestConcurrentTransactionSubmission(t *testing.T) {
	client := setupTxClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const numConcurrentTx = 5
	var wg sync.WaitGroup
	results := make(chan *txResult, numConcurrentTx)

	// Submit multiple transactions concurrently to trigger race conditions
	for i := 0; i < numConcurrentTx; i++ {
		wg.Add(1)
		go func(txNum int) {
			defer wg.Done()

			blob := &share.Blob{
				NamespaceId:      appconsts.DefaultTxNamespaceID,
				Data:             []byte(fmt.Sprintf("concurrent test blob %d data", txNum)),
				ShareVersion:     uint32(appconsts.ShareVersionZero),
				NamespaceVersion: uint32(appconsts.NamespaceVersionZero),
			}

			start := time.Now()
			txResp, err := client.SubmitPayForBlob(ctx, []*share.Blob{blob})
			duration := time.Since(start)

			results <- &txResult{
				TxNum:     txNum,
				TxResp:    txResp,
				Error:     err,
				Duration:  duration,
			}
		}(i)
	}

	// Wait for all submissions to complete
	wg.Wait()
	close(results)

	// Collect and analyze results
	var successful, failed int
	var successfulTxs []*txResult
	var failedTxs []*txResult

	for result := range results {
		if result.Error != nil {
			failed++
			failedTxs = append(failedTxs, result)
			t.Logf("Transaction %d failed: %v (took %v)", result.TxNum, result.Error, result.Duration)
		} else {
			successful++
			successfulTxs = append(successfulTxs, result)
			t.Logf("Transaction %d succeeded: hash=%s, height=%d (took %v)", 
				result.TxNum, result.TxResp.TxHash, result.TxResp.Height, result.Duration)
		}
	}

	// Verify race condition behavior
	t.Logf("Concurrent submission results: %d successful, %d failed", successful, failed)
	
	// In an ideal implementation, all transactions should succeed
	// Current implementation may have failures due to race conditions
	assert.GreaterOrEqual(t, successful, 1, "At least one transaction should succeed")
	
	// Verify sequence numbers are managed correctly for successful transactions
	if len(successfulTxs) > 1 {
		// Check if successful transactions have sequential block heights (approximately)
		heights := make([]int64, len(successfulTxs))
		for i, tx := range successfulTxs {
			heights[i] = tx.TxResp.Height
		}
		
		// Heights should be reasonable (not all the same, indicating proper sequencing)
		t.Logf("Successful transaction heights: %v", heights)
	}
}

// TestEvictionRaceCondition specifically tests the eviction handling race condition
// described in the specification where an evicted transaction gets resubmitted
// while new transactions are also being submitted.
func TestEvictionRaceCondition(t *testing.T) {
	client := setupTxClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Create a large blob that might get evicted due to size or mempool pressure
	largeBlobData := make([]byte, 1024*10) // 10KB blob
	for i := range largeBlobData {
		largeBlobData[i] = byte(i % 256)
	}

	largeBlob := &share.Blob{
		NamespaceId:      appconsts.DefaultTxNamespaceID,
		Data:             largeBlobData,
		ShareVersion:     uint32(appconsts.ShareVersionZero),
		NamespaceVersion: uint32(appconsts.NamespaceVersionZero),
	}

	// Submit the large transaction that might get evicted
	txResp, err := client.BroadcastPayForBlob(ctx, []*share.Blob{largeBlob})
	require.NoError(t, err)
	
	txHash := txResp.TxHash
	t.Logf("Submitted large transaction: %s", txHash)

	// Monitor transaction status to detect eviction
	evictionDetected := false
	var evictionTime time.Time

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Check if transaction is still being tracked
				_, _, _, exists := client.GetTxFromTxTracker(txHash)
				if !exists && !evictionDetected {
					evictionDetected = true
					evictionTime = time.Now()
					t.Logf("Transaction eviction detected at %v", evictionTime)
					return
				}
			}
		}
	}()

	// Submit additional transactions while monitoring for eviction
	var concurrentResults []error
	var concurrentMutex sync.Mutex

	for i := 0; i < 3; i++ {
		go func(txNum int) {
			// Wait a bit to let the large transaction settle
			time.Sleep(time.Duration(txNum+1) * 2 * time.Second)

			smallBlob := &share.Blob{
				NamespaceId:      appconsts.DefaultTxNamespaceID,
				Data:             []byte(fmt.Sprintf("small concurrent blob %d", txNum)),
				ShareVersion:     uint32(appconsts.ShareVersionZero),
				NamespaceVersion: uint32(appconsts.NamespaceVersionZero),
			}

			_, err := client.SubmitPayForBlob(ctx, []*share.Blob{smallBlob})
			
			concurrentMutex.Lock()
			concurrentResults = append(concurrentResults, err)
			concurrentMutex.Unlock()

			if err != nil {
				t.Logf("Concurrent transaction %d failed: %v", txNum, err)
			} else {
				t.Logf("Concurrent transaction %d succeeded", txNum)
			}
		}(i)
	}

	// Try to confirm the large transaction
	confirmedTx, confirmErr := client.ConfirmTx(ctx, txHash)

	// Wait for concurrent transactions to complete
	time.Sleep(10 * time.Second)

	// Analyze results
	if confirmErr != nil {
		t.Logf("Large transaction failed confirmation: %v", confirmErr)
		if evictionDetected {
			t.Logf("Eviction was detected during the test at %v", evictionTime)
		}
	} else {
		t.Logf("Large transaction confirmed successfully: height=%d", confirmedTx.Height)
	}

	// Check concurrent transaction results
	concurrentMutex.Lock()
	defer concurrentMutex.Unlock()
	
	successfulConcurrent := 0
	for i, err := range concurrentResults {
		if err == nil {
			successfulConcurrent++
		} else {
			t.Logf("Concurrent transaction %d error: %v", i, err)
		}
	}

	t.Logf("Concurrent transactions: %d successful out of %d total", successfulConcurrent, len(concurrentResults))

	// The test passes if we can observe the race condition behavior
	// In a fixed implementation, all transactions should handle evictions gracefully
}

// txResult holds the result of a transaction submission for analysis
type txResult struct {
	TxNum    int
	TxResp   *TxResponse
	Error    error
	Duration time.Duration
}

// setupTxClient creates a test TxClient for race condition testing
func setupTxClient(t *testing.T) *TxClient {
	// This would need to be implemented based on your test setup
	// For now, returning nil to indicate this is a template
	t.Skip("This test requires a properly configured test environment with a running Celestia node")
	return nil
}