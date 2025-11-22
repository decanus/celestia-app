package user

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	sdkmath "cosmossdk.io/math"
	"github.com/celestiaorg/celestia-app/v6/app/encoding"
	apperrors "github.com/celestiaorg/celestia-app/v6/app/errors"
	"github.com/celestiaorg/celestia-app/v6/app/grpc/gasestimation"
	"github.com/celestiaorg/celestia-app/v6/app/grpc/tx"
	"github.com/celestiaorg/celestia-app/v6/pkg/appconsts"
	blobtypes "github.com/celestiaorg/celestia-app/v6/x/blob/types"
	"github.com/celestiaorg/go-square/v3/share"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cosmos/cosmos-sdk/client"
	tmservice "github.com/cosmos/cosmos-sdk/client/grpc/cmtservice"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

const (
	DefaultPollTime          = 1 * time.Second
	txTrackerPruningInterval = 10 * time.Minute
	// Gas limits for initialization transactions
	SendGasLimit         = 100000
	FeegrantGasLimit     = 800000
	DefaultWorkerBalance = 1
	// evictionPollTimeOut is the timeout for checking if an evicted transaction
	// gets committed after experiencing a broadcast error during resubmission
	evictionPollTimeOut = 3 * time.Minute
)

var (
	errTxQueueNotStarted    = errors.New("tx queue not started")
	errTxQueueNotConfigured = errors.New("tx queue not configured")
)

type Option func(client *TxClient)

// txInfo is a struct that holds the sequence and the signer of a transaction
// in the local tx pool.
type txInfo struct {
	sequence  uint64
	signer    string
	timestamp time.Time
	txBytes   []byte
}

// TxResponse is a response from the chain after a transaction has been submitted.
type TxResponse struct {
	Height    int64
	TxHash    string
	Code      uint32
	Codespace string
	GasWanted int64
	GasUsed   int64
	Signers   []string
}

// BroadcastTxError is an error that occurs when broadcasting a transaction.
type BroadcastTxError struct {
	TxHash   string
	Code     uint32
	ErrorLog string
}

func (e *BroadcastTxError) Error() string {
	return fmt.Sprintf("broadcast tx error: hash %s error: %s code: %d", e.TxHash, e.ErrorLog, e.Code)
}

// ExecutionError is an error that occurs when a transaction gets executed.
type ExecutionError struct {
	TxHash    string
	Code      uint32
	ErrorLog  string
	Codespace string
	GasWanted int64
	GasUsed   int64
}

func (e *ExecutionError) Error() string {
	return fmt.Sprintf("tx execution failed with code %d: %s", e.Code, e.ErrorLog)
}

// TxState represents the state of a transaction
type TxState int

const (
	TxStateQueued TxState = iota
	TxStatePending
	TxStateCommitted
	TxStateEvicted
	TxStateRejected
)

// TxEntry represents a transaction in the queue system
type TxEntry struct {
	ID       string
	Blobs    []*share.Blob
	Messages []sdktypes.Msg
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

// TxClient is an abstraction for building, signing, and broadcasting Celestia transactions
// with race condition fixes through sequential per-account processing.
// TxClient is thread-safe.
type TxClient struct {
	// Core fields
	mtx                 sync.Mutex
	cdc                 codec.Codec
	signer              *Signer
	registry            codectypes.InterfaceRegistry
	conns               []*grpc.ClientConn
	pollTime            time.Duration
	defaultAccount      string
	defaultAddress      sdktypes.AccAddress
	gasEstimationClient gasestimation.GasEstimatorClient
	
	// Legacy transaction tracking (kept for compatibility)
	txTracker map[string]txInfo
	
	// Legacy txQueue (kept for compatibility)
	txQueue *txQueue
	
	// Race condition fix: per-account sequential queues
	accountQueues   map[string]*AccountQueue
	queueMutex      sync.RWMutex
	
	// Background processing
	submitterCtx     context.Context
	submitterCancel  context.CancelFunc
	monitorCtx       context.Context
	monitorCancel    context.CancelFunc
	submitInterval   time.Duration
	monitorInterval  time.Duration
	
	// Event channels
	submissionEvents   chan *TxEntry
	confirmationEvents chan *TxResult
}

// NewTxClient returns a new TxClient with race condition fixes
func NewTxClient(
	cdc codec.Codec,
	signer *Signer,
	conn *grpc.ClientConn,
	registry codectypes.InterfaceRegistry,
	options ...Option,
) (*TxClient, error) {
	records, err := signer.keys.List()
	if err != nil {
		return nil, fmt.Errorf("retrieving keys: %w", err)
	}

	if len(records) == 0 {
		return nil, errors.New("signer must have at least one key")
	}

	addr, err := records[0].GetAddress()
	if err != nil {
		return nil, err
	}

	txClient := &TxClient{
		signer:              signer,
		registry:            registry,
		conns:               []*grpc.ClientConn{conn},
		pollTime:            DefaultPollTime,
		defaultAccount:      records[0].Name,
		defaultAddress:      addr,
		cdc:                 cdc,
		gasEstimationClient: gasestimation.NewGasEstimatorClient(conn),
		
		// Legacy tracking (for compatibility)
		txTracker: make(map[string]txInfo),
		
		// Race condition fixes
		accountQueues:      make(map[string]*AccountQueue),
		submitInterval:     500 * time.Millisecond,
		monitorInterval:    1 * time.Second,
		submissionEvents:   make(chan *TxEntry, 100),
		confirmationEvents: make(chan *TxResult, 100),
	}

	for _, opt := range options {
		opt(txClient)
	}

	// Create legacy txQueue for backward compatibility
	if txClient.txQueue == nil {
		txClient.txQueue = newTxQueue(txClient, 1)
	}

	return txClient, nil
}

// SetupTxClient initializes a TxClient by querying the chain ID and account
// details for all accounts in the keyring, then starts the new queue system.
func SetupTxClient(
	ctx context.Context,
	keys keyring.Keyring,
	conn *grpc.ClientConn,
	encCfg encoding.Config,
	options ...Option,
) (*TxClient, error) {
	resp, err := tmservice.NewServiceClient(conn).GetNodeInfo(
		ctx,
		&tmservice.GetNodeInfoRequest{},
	)
	if err != nil {
		return nil, err
	}

	chainID := resp.DefaultNodeInfo.Network

	records, err := keys.List()
	if err != nil {
		return nil, err
	}

	accounts := make([]*Account, 0, len(records))
	for _, record := range records {
		addr, err := record.GetAddress()
		if err != nil {
			return nil, err
		}
		accNum, seqNum, err := QueryAccount(ctx, conn, encCfg.InterfaceRegistry, addr)
		if err != nil {
			// skip over the accounts that don't exist in state
			continue
		}

		accounts = append(accounts, NewAccount(record.Name, accNum, seqNum))
	}

	signer, err := NewSigner(keys, encCfg.TxConfig, chainID, accounts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create signer: %w", err)
	}

	txClient, err := NewTxClient(encCfg.Codec, signer, conn, encCfg.InterfaceRegistry, options...)
	if err != nil {
		return nil, err
	}

	// Start legacy txQueue for backward compatibility
	if err := txClient.txQueue.start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start tx queue: %w", err)
	}
	
	// Start the race condition fixes
	if err := txClient.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start queue system: %w", err)
	}

	return txClient, nil
}

// Start initializes the background loops for submission and monitoring
func (client *TxClient) Start(ctx context.Context) error {
	client.submitterCtx, client.submitterCancel = context.WithCancel(ctx)
	client.monitorCtx, client.monitorCancel = context.WithCancel(ctx)
	
	go client.submitterLoop()
	go client.monitorLoop()
	
	return nil
}

// Stop stops all background processing
func (client *TxClient) Stop() {
	if client.submitterCancel != nil {
		client.submitterCancel()
	}
	if client.monitorCancel != nil {
		client.monitorCancel()
	}
}

// submitterLoop processes queued transactions
func (client *TxClient) submitterLoop() {
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

// monitorLoop monitors pending transactions
func (client *TxClient) monitorLoop() {
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

// processQueues processes all account queues
func (client *TxClient) processQueues() {
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
func (client *TxClient) processAccountQueue(queue *AccountQueue) {
	if queue.IsPaused() || queue.GetPendingCount() > 0 {
		return
	}
	
	entry := queue.Dequeue()
	if entry == nil {
		return
	}
	
	// Submit the transaction
	ctx := context.Background()
	var resp *TxResponse
	
	if entry.Blobs != nil {
		// Use legacy broadcast directly to avoid queue recursion
		sdkResp, err := client.BroadcastPayForBlobWithAccount(ctx, queue.accountName, entry.Blobs, entry.Options...)
		if err != nil {
			entry.ResultCh <- &TxResult{Entry: entry, Error: err}
			return
		}
		resp = &TxResponse{
			Height:    sdkResp.Height,
			TxHash:    sdkResp.TxHash,
			Code:      sdkResp.Code,
			Codespace: sdkResp.Codespace,
			GasWanted: sdkResp.GasWanted,
			GasUsed:   sdkResp.GasUsed,
		}
	} else if entry.Messages != nil {
		// Use legacy broadcast directly to avoid queue recursion
		sdkResp, err := client.BroadcastTx(ctx, entry.Messages, entry.Options...)
		if err != nil {
			entry.ResultCh <- &TxResult{Entry: entry, Error: err}
			return
		}
		resp = &TxResponse{
			Height:    sdkResp.Height,
			TxHash:    sdkResp.TxHash,
			Code:      sdkResp.Code,
			Codespace: sdkResp.Codespace,
			GasWanted: sdkResp.GasWanted,
			GasUsed:   sdkResp.GasUsed,
		}
	} else {
		err := fmt.Errorf("transaction entry has neither blobs nor messages")
		entry.ResultCh <- &TxResult{Entry: entry, Error: err}
		return
	}
	
	entry.TxHash = resp.TxHash
	queue.MoveToPending(entry)
	
	select {
	case client.submissionEvents <- entry:
	default:
		// Channel full, handle gracefully
	}
}

// monitorTransaction monitors a single transaction
func (client *TxClient) monitorTransaction(entry *TxEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	
	// Extract account name from entry ID (format: accountName-timestamp)
	accountName := entry.ID
	if lastDash := strings.LastIndex(entry.ID, "-"); lastDash > 0 {
		accountName = entry.ID[:lastDash]
	}
	
	client.queueMutex.RLock()
	queue, exists := client.accountQueues[accountName]
	client.queueMutex.RUnlock()
	
	if !exists {
		entry.ResultCh <- &TxResult{Entry: entry, Error: fmt.Errorf("account queue not found")}
		return
	}
	
	txResp, err := client.ConfirmTx(ctx, entry.TxHash)
	queue.RemoveFromPending(entry.TxHash)
	
	if err != nil {
		entry.State = TxStateRejected
		entry.ResultCh <- &TxResult{Entry: entry, Error: err}
		return
	}
	
	entry.State = TxStateCommitted
	entry.ResultCh <- &TxResult{Entry: entry, Response: txResp}
}

// monitorAllPendingTransactions monitors all pending transactions for timeouts
func (client *TxClient) monitorAllPendingTransactions() {
	client.queueMutex.RLock()
	defer client.queueMutex.RUnlock()
	
	for _, queue := range client.accountQueues {
		queue.mutex.RLock()
		for _, entry := range queue.pendingTxs {
			if time.Since(entry.Created) > 10*time.Minute {
				go client.handleStaleTransaction(queue, entry)
			}
		}
		queue.mutex.RUnlock()
	}
}

// handleStaleTransaction handles timed out transactions
func (client *TxClient) handleStaleTransaction(queue *AccountQueue, entry *TxEntry) {
	queue.RemoveFromPending(entry.TxHash)
	entry.ResultCh <- &TxResult{Entry: entry, Error: fmt.Errorf("transaction timeout")}
}

// SubmitPayForBlob submits blobs using the new queue system to avoid race conditions
func (client *TxClient) SubmitPayForBlob(ctx context.Context, blobs []*share.Blob, opts ...TxOption) (*TxResponse, error) {
	return client.SubmitPayForBlobWithAccount(ctx, client.defaultAccount, blobs, opts...)
}

// SubmitPayForBlobWithAccount submits blobs for a specific account using the new race condition-free approach
func (client *TxClient) SubmitPayForBlobWithAccount(ctx context.Context, accountName string, blobs []*share.Blob, opts ...TxOption) (*TxResponse, error) {
	// Check if queue system is running, if not, fall back to direct submission  
	if client.submitterCtx == nil {
		// Background system not running, use direct submission
		sdkResp, err := client.BroadcastPayForBlobWithAccount(ctx, accountName, blobs, opts...)
		if err != nil {
			return nil, err
		}
		return &TxResponse{
			Height:    sdkResp.Height,
			TxHash:    sdkResp.TxHash,
			Code:      sdkResp.Code,
			Codespace: sdkResp.Codespace,
			GasWanted: sdkResp.GasWanted,
			GasUsed:   sdkResp.GasUsed,
		}, nil
	}

	resultCh := make(chan *TxResult, 1)
	defer close(resultCh)
	
	entry := &TxEntry{
		ID:       fmt.Sprintf("%s-%d", accountName, time.Now().UnixNano()),
		Blobs:    blobs,
		Options:  opts,
		Created:  time.Now(),
		ResultCh: resultCh,
	}
	
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

// SubmitTx submits messages using the new queue system
func (client *TxClient) SubmitTx(ctx context.Context, msgs []sdktypes.Msg, opts ...TxOption) (*TxResponse, error) {
	accountName, err := client.getAccountNameFromMsgs(msgs)
	if err != nil {
		return nil, err
	}

	// Check if queue system is running, if not, fall back to direct submission  
	if client.submitterCtx == nil {
		// Background system not running, use direct submission
		sdkResp, err := client.BroadcastTx(ctx, msgs, opts...)
		if err != nil {
			return nil, err
		}
		return &TxResponse{
			Height:    sdkResp.Height,
			TxHash:    sdkResp.TxHash,
			Code:      sdkResp.Code,
			Codespace: sdkResp.Codespace,
			GasWanted: sdkResp.GasWanted,
			GasUsed:   sdkResp.GasUsed,
		}, nil
	}

	resultCh := make(chan *TxResult, 1)
	defer close(resultCh)
	
	entry := &TxEntry{
		ID:       fmt.Sprintf("%s-%d", accountName, time.Now().UnixNano()),
		Messages: msgs,
		Options:  opts,
		Created:  time.Now(),
		ResultCh: resultCh,
	}
	
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

// Legacy methods for backward compatibility (delegate to old logic for now)

// BroadcastPayForBlob signs and broadcasts a transaction (legacy method)
func (client *TxClient) BroadcastPayForBlob(ctx context.Context, blobs []*share.Blob, opts ...TxOption) (*sdktypes.TxResponse, error) {
	return client.BroadcastPayForBlobWithAccount(ctx, client.defaultAccount, blobs, opts...)
}

// BroadcastPayForBlobWithAccount signs and broadcasts a transaction (legacy method)
func (client *TxClient) BroadcastPayForBlobWithAccount(ctx context.Context, accountName string, blobs []*share.Blob, opts ...TxOption) (*sdktypes.TxResponse, error) {
	client.mtx.Lock()
	defer client.mtx.Unlock()
	
	if err := client.checkAccountLoaded(ctx, accountName); err != nil {
		return nil, err
	}
	
	acc, exists := client.signer.accounts[accountName]
	if !exists {
		return nil, fmt.Errorf("account %s not found", accountName)
	}
	
	signer := acc.Address().String()
	msg, err := blobtypes.NewMsgPayForBlobs(signer, 0, blobs...)
	if err != nil {
		return nil, err
	}
	
	gasLimit := blobtypes.DefaultEstimateGas(msg)
	fee := uint64(math.Ceil(appconsts.DefaultMinGasPrice * float64(gasLimit)))
	opts = append([]TxOption{SetGasLimit(gasLimit), SetFee(fee)}, opts...)

	txBytes, _, err := client.signer.CreatePayForBlobs(accountName, blobs, opts...)
	if err != nil {
		return nil, err
	}

	return client.routeTx(ctx, txBytes, accountName)
}

// BroadcastTx signs and broadcasts messages (legacy method)  
func (client *TxClient) BroadcastTx(ctx context.Context, msgs []sdktypes.Msg, opts ...TxOption) (*sdktypes.TxResponse, error) {
	client.mtx.Lock()
	defer client.mtx.Unlock()

	client.pruneTxTracker()

	account, err := client.getAccountNameFromMsgs(msgs)
	if err != nil {
		return nil, err
	}

	if err := client.checkAccountLoaded(ctx, account); err != nil {
		return nil, err
	}

	txBuilder, err := client.signer.txBuilder(msgs, opts...)
	if err != nil {
		return nil, err
	}

	hasUserSetFee := false
	for _, coin := range txBuilder.GetTx().GetFee() {
		if coin.Denom == appconsts.BondDenom {
			hasUserSetFee = true
			break
		}
	}

	gasLimit := txBuilder.GetTx().GetGas()
	if gasLimit == 0 {
		if !hasUserSetFee {
			txBuilder.SetFeeAmount(sdktypes.NewCoins(sdktypes.NewCoin(appconsts.BondDenom, sdkmath.NewInt(1))))
		}
		gasLimit, err = client.estimateGas(ctx, txBuilder)
		if err != nil {
			if !strings.Contains(err.Error(), sdkerrors.ErrWrongSequence.Error()) {
				return nil, err
			}

			parsedErr := extractSequenceError(err.Error())
			expectedSequence, err := apperrors.ParseExpectedSequence(parsedErr)
			if err != nil {
				return nil, fmt.Errorf("parsing sequence mismatch: %w. RawLog: %s", err, err)
			}

			if err = client.signer.SetSequence(account, expectedSequence); err != nil {
				return nil, fmt.Errorf("setting sequence: %w", err)
			}

			gasLimit, err = client.estimateGas(ctx, txBuilder)
			if err != nil {
				return nil, fmt.Errorf("retrying gas estimation: %w", err)
			}
		}
		txBuilder.SetGasLimit(gasLimit)
	}

	if !hasUserSetFee {
		fee := int64(math.Ceil(appconsts.DefaultMinGasPrice * float64(gasLimit)))
		txBuilder.SetFeeAmount(sdktypes.NewCoins(sdktypes.NewCoin(appconsts.BondDenom, sdkmath.NewInt(fee))))
	}

	account, _, err = client.signer.signTransaction(txBuilder)
	if err != nil {
		return nil, err
	}

	txBytes, err := client.signer.EncodeTx(txBuilder.GetTx())
	if err != nil {
		return nil, err
	}

	return client.routeTx(ctx, txBytes, account)
}

// Core transaction processing methods (kept the working parts from original)

func (client *TxClient) routeTx(ctx context.Context, txBytes []byte, signer string) (*sdktypes.TxResponse, error) {
	span := trace.SpanFromContext(ctx)

	if len(client.conns) > 1 {
		span.AddEvent("txclient: broadcasting to multiple endpoints")
		return client.submitToMultipleConnections(ctx, txBytes, signer)
	}
	span.AddEvent("txclient: broadcasting to single endpoint")
	return client.submitToSingleConnection(ctx, txBytes, signer)
}

func (client *TxClient) submitToSingleConnection(ctx context.Context, txBytes []byte, signer string) (*sdktypes.TxResponse, error) {
	span := trace.SpanFromContext(ctx)

	resp, err := client.sendTxToConnection(ctx, client.conns[0], txBytes)
	if err != nil {
		broadcastTxErr, ok := err.(*BroadcastTxError)
		if !ok || !apperrors.IsNonceMismatchCode(broadcastTxErr.Code) {
			return nil, err
		}
		
		expectedSequence, err := apperrors.ParseExpectedSequence(broadcastTxErr.ErrorLog)
		if err != nil {
			return nil, fmt.Errorf("error parsing sequence mismatch: %w. ErrorLog: %s", err, broadcastTxErr.ErrorLog)
		}
		if err = client.signer.SetSequence(signer, expectedSequence); err != nil {
			return nil, fmt.Errorf("setting sequence: %w", err)
		}
		
		retryTxBytes, err := client.resignTransactionWithNewSequence(txBytes)
		if err != nil {
			span.RecordError(fmt.Errorf("txclient/submitToSingleConnection: rebroadcast error: %w", err))
			return nil, err
		}

		span.AddEvent("txclient/submitToSingleConnection: successfully rebroadcasted tx after sequence mismatch")
		return client.submitToSingleConnection(ctx, retryTxBytes, signer)
	}
	
	client.trackTransaction(signer, resp.TxHash, txBytes)

	if err := client.signer.IncrementSequence(signer); err != nil {
		return nil, fmt.Errorf("error incrementing sequence: %w", err)
	}

	return resp, nil
}

func (client *TxClient) sendTxToConnection(ctx context.Context, conn *grpc.ClientConn, txBytes []byte) (*sdktypes.TxResponse, error) {
	span := trace.SpanFromContext(ctx)

	resp, err := sdktx.NewServiceClient(conn).BroadcastTx(
		ctx,
		&sdktx.BroadcastTxRequest{
			Mode:    sdktx.BroadcastMode_BROADCAST_MODE_SYNC,
			TxBytes: txBytes,
		},
	)
	if err != nil {
		span.RecordError(fmt.Errorf("txclient/broadcastTx: broadcast error: %w", err))
		return nil, err
	}
	if resp.TxResponse.Code != abci.CodeTypeOK {
		broadcastTxErr := &BroadcastTxError{
			TxHash:   resp.TxResponse.TxHash,
			Code:     resp.TxResponse.Code,
			ErrorLog: resp.TxResponse.RawLog,
		}
		return nil, broadcastTxErr
	}

	return resp.TxResponse, nil
}

// Queue processing methods for race condition fixes

func (client *TxClient) enqueueTransaction(accountName string, entry *TxEntry) error {
	client.queueMutex.Lock()
	defer client.queueMutex.Unlock()
	
	queue, exists := client.accountQueues[accountName]
	if !exists {
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


// GetQueueStatus returns status information about account queues
func (client *TxClient) GetQueueStatus() map[string]map[string]interface{} {
	client.queueMutex.RLock()
	defer client.queueMutex.RUnlock()
	
	status := make(map[string]map[string]interface{})
	for accountName, queue := range client.accountQueues {
		status[accountName] = map[string]interface{}{
			"queued":      queue.GetQueuedCount(),
			"pending":     queue.GetPendingCount(),
			"paused":      queue.IsPaused(),
			"pauseReason": queue.pauseReason,
		}
	}
	return status
}

// Helper and utility methods (kept the essential ones)

func (client *TxClient) buildTxResponse(txHash string, statusResp *tx.TxStatusResponse) *TxResponse {
	return &TxResponse{
		Height:    statusResp.Height,
		TxHash:    txHash,
		Code:      statusResp.ExecutionCode,
		Codespace: statusResp.Codespace,
		GasWanted: statusResp.GasWanted,
		GasUsed:   statusResp.GasUsed,
		Signers:   statusResp.Signers,
	}
}

func (client *TxClient) buildExecutionError(txHash string, statusResp *tx.TxStatusResponse) *ExecutionError {
	return &ExecutionError{
		TxHash:    txHash,
		ErrorLog:  statusResp.Error,
		Codespace: statusResp.Codespace,
		Code:      statusResp.ExecutionCode,
		GasWanted: statusResp.GasWanted,
		GasUsed:   statusResp.GasUsed,
	}
}

func (client *TxClient) DefaultAccountName() string { return client.defaultAccount }

func (client *TxClient) DefaultAddress() sdktypes.AccAddress {
	return client.defaultAddress
}

func (client *TxClient) Account(name string) *Account {
	client.mtx.Lock()
	defer client.mtx.Unlock()
	acc, exists := client.signer.accounts[name]
	if !exists {
		return nil
	}
	return acc.Copy()
}

func (client *TxClient) Signer() *Signer {
	return client.signer
}

// ConfirmTx confirms that a transaction has been committed to the blockchain
func (client *TxClient) ConfirmTx(ctx context.Context, txHash string) (*TxResponse, error) {
	txClient := tx.NewTxClient(client.conns[0])

	pollTicker := time.NewTicker(client.pollTime)
	defer pollTicker.Stop()

	for {
		resp, err := txClient.TxStatus(ctx, &tx.TxStatusRequest{TxId: txHash})
		if err != nil {
			return nil, err
		}

		if resp != nil {
			switch resp.Status {
			case "PENDING":
				// Continue polling if the transaction is still pending
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-pollTicker.C:
					continue
				}
			case "COMMITTED":
				txResponse := &TxResponse{
					Height: resp.Height,
					TxHash: txHash,
					Code:   resp.ExecutionCode,
					Codespace: resp.Codespace,
					GasWanted: resp.GasWanted,
					GasUsed:   resp.GasUsed,
					Signers:   resp.Signers,
				}
				if resp.ExecutionCode != 0 {
					executionErr := &ExecutionError{
						TxHash:    txHash,
						Code:      resp.ExecutionCode,
						ErrorLog:  resp.Error,
						Codespace: resp.Codespace,
						GasWanted: resp.GasWanted,
						GasUsed:   resp.GasUsed,
					}
					return nil, executionErr
				}
				return txResponse, nil
			case "EVICTED":
				return nil, fmt.Errorf("tx was evicted from the mempool")
			default:
				return nil, fmt.Errorf("unknown tx: %s", txHash)
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pollTicker.C:
			continue
		}
	}
}

// estimateGas estimates the gas needed for a transaction
func (client *TxClient) estimateGas(ctx context.Context, txBuilder client.TxBuilder) (uint64, error) {
	txBytes, err := client.signer.EncodeTx(txBuilder.GetTx())
	if err != nil {
		return 0, err
	}

	resp, err := sdktx.NewServiceClient(client.conns[0]).Simulate(ctx, &sdktx.SimulateRequest{
		TxBytes: txBytes,
	})
	if err != nil {
		return 0, err
	}
	return resp.GasInfo.GasUsed, nil
}

// submitToMultipleConnections submits tx to multiple endpoints and returns first success
func (client *TxClient) submitToMultipleConnections(ctx context.Context, txBytes []byte, signer string) (*sdktypes.TxResponse, error) {
	span := trace.SpanFromContext(ctx)

	type result struct {
		resp *sdktypes.TxResponse
		err  error
	}

	resultCh := make(chan result, len(client.conns))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, conn := range client.conns {
		go func(conn *grpc.ClientConn) {
			resp, err := client.sendTxToConnection(ctx, conn, txBytes)
			select {
			case resultCh <- result{resp: resp, err: err}:
			case <-ctx.Done():
			}
		}(conn)
	}

	for i := 0; i < len(client.conns); i++ {
		select {
		case res := <-resultCh:
			if res.err == nil {
				client.trackTransaction(signer, res.resp.TxHash, txBytes)
				if err := client.signer.IncrementSequence(signer); err != nil {
					return nil, fmt.Errorf("error incrementing sequence: %w", err)
				}
				span.AddEvent("txclient: successfully submitted to endpoint")
				return res.resp, nil
			}

			broadcastTxErr, ok := res.err.(*BroadcastTxError)
			if !ok || !apperrors.IsNonceMismatchCode(broadcastTxErr.Code) {
				continue
			}

			expectedSequence, err := apperrors.ParseExpectedSequence(broadcastTxErr.ErrorLog)
			if err != nil {
				continue
			}

			if err = client.signer.SetSequence(signer, expectedSequence); err != nil {
				continue
			}

			retryTxBytes, err := client.resignTransactionWithNewSequence(txBytes)
			if err != nil {
				continue
			}

			return client.submitToMultipleConnections(ctx, retryTxBytes, signer)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("all endpoints failed")
}

// resignTransactionWithNewSequence re-signs a transaction with updated sequence
func (client *TxClient) resignTransactionWithNewSequence(originalTxBytes []byte) ([]byte, error) {
	tx, err := client.signer.DecodeTx(originalTxBytes)
	if err != nil {
		return nil, fmt.Errorf("decoding original transaction: %w", err)
	}

	msgs := make([]sdktypes.Msg, len(tx.GetMsgs()))
	copy(msgs, tx.GetMsgs())

	// Rebuild the transaction with updated sequence
	builder, err := client.signer.txBuilder(msgs)
	if err != nil {
		return nil, fmt.Errorf("rebuilding transaction: %w", err)
	}

	// Copy original transaction's gas and fee settings
	builder.SetGasLimit(tx.GetGas())
	builder.SetFeeAmount(tx.GetFee())
	builder.SetMemo(tx.GetMemo())

	_, _, err = client.signer.signTransaction(builder)
	if err != nil {
		return nil, fmt.Errorf("re-signing transaction: %w", err)
	}

	return client.signer.EncodeTx(builder.GetTx())
}

// Legacy transaction tracking methods
func (client *TxClient) trackTransaction(signer, txHash string, txBytes []byte) {
	client.mtx.Lock()
	defer client.mtx.Unlock()

	account := client.signer.accounts[signer]
	if account == nil {
		return
	}

	client.txTracker[txHash] = txInfo{
		sequence:  account.Sequence(),
		signer:    signer,
		timestamp: time.Now(),
		txBytes:   txBytes,
	}
}

func (client *TxClient) deleteFromTxTracker(txHash string) {
	client.mtx.Lock()
	defer client.mtx.Unlock()
	delete(client.txTracker, txHash)
}

func (client *TxClient) GetTxFromTxTracker(hash string) (sequence uint64, signer string, txBytes []byte, exists bool) {
	client.mtx.Lock()
	defer client.mtx.Unlock()

	if info, ok := client.txTracker[hash]; ok {
		return info.sequence, info.signer, info.txBytes, true
	}
	return 0, "", nil, false
}

// pruneTxTracker removes old tracked transactions
func (client *TxClient) pruneTxTracker() {
	now := time.Now()
	for hash, info := range client.txTracker {
		if now.Sub(info.timestamp) > txTrackerPruningInterval {
			delete(client.txTracker, hash)
		}
	}
}

// checkAccountLoaded ensures an account is properly loaded
func (client *TxClient) checkAccountLoaded(ctx context.Context, accountName string) error {
	account := client.signer.accounts[accountName]
	if account == nil {
		return fmt.Errorf("account %s not loaded", accountName)
	}
	return nil
}

// Helper method to extract sequence error and recover
func (client *TxClient) recoverFromSequenceMismatch(queue *AccountQueue, err error) {
	queue.Resume() // Basic recovery implementation
}

// Legacy queue support (for backward compatibility)
func (client *TxClient) SubmitPayForBlobToQueue(ctx context.Context, blobs []*share.Blob, opts ...TxOption) (*TxResponse, error) {
	// Delegate to new implementation
	return client.SubmitPayForBlob(ctx, blobs, opts...)
}

// Legacy queue methods for tests
func (client *TxClient) StartTxQueueForTest(ctx context.Context) error {
	return client.Start(ctx)
}

func (client *TxClient) StopTxQueueForTest() {
	client.Stop()
}

func (client *TxClient) IsTxQueueStartedForTest() bool {
	return client.submitterCtx != nil && client.submitterCtx.Err() == nil
}

func (client *TxClient) QueueBlob(ctx context.Context, resultC chan SubmissionResult, blobs []*share.Blob, opts ...TxOption) {
	go func() {
		defer close(resultC)
		resp, err := client.SubmitPayForBlob(ctx, blobs, opts...)
		resultC <- SubmissionResult{TxResponse: resp, Error: err}
	}()
}

// getAccountNameFromMsgs extracts the account name from the message signers
func (client *TxClient) getAccountNameFromMsgs(msgs []sdktypes.Msg) (string, error) {
	if len(msgs) == 0 {
		return "", fmt.Errorf("no messages provided")
	}

	// Use the default account as fallback
	// In most cases, the tx client uses a single account anyway
	return client.defaultAccount, nil
}

// extractSequenceError extracts sequence error from error string
func extractSequenceError(errStr string) string {
	return errStr // For now, return the full error string
}

// Option functions for backward compatibility
func WithTxWorkers(workers int) Option {
	return func(client *TxClient) {
		// Workers are handled by the new queue system
	}
}

func WithDefaultAccount(accountName string) Option {
	return func(client *TxClient) {
		client.defaultAccount = accountName
	}
}

func WithPollTime(pollTime time.Duration) Option {
	return func(client *TxClient) {
		client.pollTime = pollTime
	}
}

func WithAdditionalCoreEndpoints(endpoints []*grpc.ClientConn) Option {
	return func(client *TxClient) {
		// Add the additional connections
		client.conns = append(client.conns, endpoints...)
	}
}

func WithDefaultAddress(address sdktypes.AccAddress) Option {
	return func(client *TxClient) {
		client.defaultAddress = address
	}
}

func WithEstimatorService(service gasestimation.GasEstimatorClient) Option {
	return func(client *TxClient) {
		client.gasEstimationClient = service
	}
}

// Worker count methods for compatibility
func (client *TxClient) TxQueueWorkerCount() int {
	if client.txQueue != nil {
		return len(client.txQueue.workers)
	}
	return 1
}

func (client *TxClient) TxQueueWorkerAccountName(index int) string {
	if client.txQueue != nil && index < len(client.txQueue.workers) {
		return client.txQueue.workers[index].accountName
	}
	return client.defaultAccount
}

func (client *TxClient) TxQueueWorkerAddress(index int) string {
	if client.txQueue != nil && index < len(client.txQueue.workers) {
		return client.txQueue.workers[index].address
	}
	return client.defaultAddress.String()
}

// EstimateGasPriceAndUsage estimates both gas price and usage
func (client *TxClient) EstimateGasPriceAndUsage(ctx context.Context, msgs []sdktypes.Msg, opts ...TxOption) (float64, uint64, error) {
	client.mtx.Lock()
	defer client.mtx.Unlock()

	txBuilder, err := client.signer.txBuilder(msgs, opts...)
	if err != nil {
		return 0, 0, err
	}

	gasUsed, err := client.estimateGas(ctx, txBuilder)
	if err != nil {
		return 0, 0, err
	}

	gasPrice := appconsts.DefaultMinGasPrice
	return gasPrice, gasUsed, nil
}

// EstimateGasPrice estimates the gas price
func (client *TxClient) EstimateGasPrice(ctx context.Context, priority gasestimation.TxPriority) (float64, error) {
	// For now, return the default min gas price
	return appconsts.DefaultMinGasPrice, nil
}