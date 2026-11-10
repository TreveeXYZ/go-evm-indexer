package indexer

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	internal_bus "github.com/evmi-cloud/go-evm-indexer/internal/bus"
	evmi_database "github.com/evmi-cloud/go-evm-indexer/internal/database/evmi-database"
	log_stores "github.com/evmi-cloud/go-evm-indexer/internal/database/log-stores"
	"github.com/evmi-cloud/go-evm-indexer/internal/metrics"
	"github.com/evmi-cloud/go-evm-indexer/internal/types"
	"github.com/lmittmann/w3"
	"github.com/lmittmann/w3/module/eth"
	"github.com/lmittmann/w3/w3types"
	"github.com/mustafaturan/bus/v3"
	"github.com/rs/zerolog"
)

type SourceIndexerService struct {
	db      *evmi_database.EvmiDatabase
	bus     *bus.Bus
	metrics *metrics.MetricService

	store *log_stores.IndexerStore

	chain        evmi_database.EvmBlockchain
	pipeline     evmi_database.EvmLogPipeline
	storeInfo    evmi_database.EvmLogStore
	source       evmi_database.EvmLogSource
	contractName string
	// head is the per-chain watcher shared with the other sources of the
	// same blockchain (set by the IndexerService); nil means this service
	// polls the head itself, the pre-watcher behaviour.
	head *HeadWatcher
	abi          abi.ABI

	logger zerolog.Logger
}

func NewSourceIndexerService(
	db *evmi_database.EvmiDatabase,
	bus *bus.Bus,
	metrics *metrics.MetricService,
	source evmi_database.EvmLogSource,
) *SourceIndexerService {

	logger := zerolog.New(
		zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
	).Level(zerolog.TraceLevel).With().Timestamp().Caller().Logger()

	return &SourceIndexerService{
		db:      db,
		bus:     bus,
		metrics: metrics,
		source:  source,
		logger:  logger,
	}
}

// factoryRules returns this FACTORY source's top-level creation rules.
func (p *SourceIndexerService) factoryRules() ([]evmi_database.EvmFactoryRule, error) {
	var rules []evmi_database.EvmFactoryRule
	err := p.db.Conn.
		Preload("Conditions").
		Where("evm_log_source_id = ? AND parent_rule_id IS NULL", p.source.ID).
		Order("id").
		Find(&rules).Error
	return rules, err
}

// ruleConditionsPass reports whether every condition of a rule holds for the
// decoded event args. An empty condition set always passes.
func ruleConditionsPass(rule evmi_database.EvmFactoryRule, args map[string]string) bool {
	for _, c := range rule.Conditions {
		if !evalCondition(args[c.Arg], c.Operator, c.Value) {
			return false
		}
	}
	return true
}

// evalCondition compares a decoded arg value against expected using operator.
// Numeric comparison is used when both parse as integers; otherwise a
// case-insensitive string comparison (so e.g. addresses match regardless of case).
func evalCondition(actual, operator, expected string) bool {
	if an, aok := new(big.Int).SetString(strings.TrimSpace(actual), 10); aok {
		if en, eok := new(big.Int).SetString(strings.TrimSpace(expected), 10); eok {
			switch cmp := an.Cmp(en); operator {
			case "eq":
				return cmp == 0
			case "neq":
				return cmp != 0
			case "gt":
				return cmp > 0
			case "gte":
				return cmp >= 0
			case "lt":
				return cmp < 0
			case "lte":
				return cmp <= 0
			}
		}
	}

	a := strings.ToLower(strings.TrimSpace(actual))
	e := strings.ToLower(strings.TrimSpace(expected))
	switch operator {
	case "eq":
		return a == e
	case "neq":
		return a != e
	case "contains":
		return strings.Contains(a, e)
	case "gt":
		return a > e
	case "gte":
		return a >= e
	case "lt":
		return a < e
	case "lte":
		return a <= e
	}
	return false
}

// cloneFactoryRuleSubtree copies the rules nested under templateParentRuleID onto a
// spawned FACTORY child, re-parenting them: the rule's immediate children become
// the child's top-level rules (EvmLogSourceID = childSourceID, ParentRuleID NULL),
// and deeper rules become nested rules of the clones. Recurses so the whole
// subtree is available for further nesting. A nil newParentRuleID marks the clones
// as the child's top-level rules.
func (p *SourceIndexerService) cloneFactoryRuleSubtree(childSourceID uint, templateParentRuleID uint, newParentRuleID *uint) error {
	var templates []evmi_database.EvmFactoryRule
	if err := p.db.Conn.Preload("Conditions").Where("parent_rule_id = ?", templateParentRuleID).Order("id").Find(&templates).Error; err != nil {
		return err
	}
	for _, t := range templates {
		clone := evmi_database.EvmFactoryRule{
			ParentRuleID:          newParentRuleID,
			CreationFunctionName:  t.CreationFunctionName,
			CreationAddressLogArg: t.CreationAddressLogArg,
			ChildType:             t.ChildType,
			EvmJsonAbiID:          t.EvmJsonAbiID,
		}
		if newParentRuleID == nil {
			sid := childSourceID
			clone.EvmLogSourceID = &sid // a top-level rule of the child
		}
		// Copy the rule's conditions onto the clone.
		for _, c := range t.Conditions {
			clone.Conditions = append(clone.Conditions, evmi_database.EvmFactoryRuleCondition{
				Arg: c.Arg, Operator: c.Operator, Value: c.Value,
			})
		}
		if err := p.db.Conn.Create(&clone).Error; err != nil {
			return err
		}
		cloneID := clone.ID
		if err := p.cloneFactoryRuleSubtree(childSourceID, t.ID, &cloneID); err != nil {
			return err
		}
	}
	return nil
}

// registerFactoryChild creates a source for a contract newly deployed by this
// FACTORY source, per one matched rule. Uniqueness is per (factory source id,
// child address): re-seeing the same deployment is a no-op. It returns an error
// on DB failure so the caller can block the factory cursor and let the supervisor
// retry the range (the child is created enabled and started best-effort via the
// enable topic, which is idempotent and also picked up on restart).
func (p *SourceIndexerService) registerFactoryChild(rule evmi_database.EvmFactoryRule, address string, startBlock uint64) error {
	var existing int64
	if err := p.db.Conn.Model(&evmi_database.EvmLogSource{}).
		Where("parent_source_id = ? AND address = ?", p.source.ID, address).
		Count(&existing).Error; err != nil {
		return err
	}
	if existing > 0 {
		return nil
	}

	childType := rule.ChildType
	if childType == "" {
		childType = string(evmi_database.ContractLogSourceType)
	}

	child := evmi_database.EvmLogSource{
		Enabled:          true,
		Status:           string(evmi_database.StoppedLogSourceStatus),
		Type:             childType,
		StartBlock:       startBlock - 1,
		SyncBlock:        startBlock - 1,
		Address:          sql.NullString{String: address, Valid: true},
		ParentSourceID:   p.source.ID,
		EvmLogPipelineID: p.pipeline.ID,
		EvmJsonAbiID:     rule.EvmJsonAbiID,
		EvmBlockchainID:  p.source.EvmBlockchainID,
	}
	if err := p.db.Conn.Create(&child).Error; err != nil {
		return err
	}

	// A FACTORY child gets its own copy of the rule's nested rules.
	if childType == string(evmi_database.FactoryLogSourceType) {
		if err := p.cloneFactoryRuleSubtree(child.ID, rule.ID, nil); err != nil {
			return err
		}
	}

	p.logger.Info().Msg("registered factory child source id " + fmt.Sprint(child.ID) + " (" + childType + ") for " + address)
	// Best-effort start; the manager creates+supervises the indexer. On failure the
	// child stays enabled in DB and is started on the next manager boot.
	p.bus.Emit(context.Background(), internal_bus.EnableSourceTopic, child.ID)
	return nil
}

// registerFactoryChildren scans a batch of decoded logs against every active
// creation rule and registers a child source for each matched deployment (one
// factory can create N kinds of contract). A failure must NOT advance the cursor:
// the error is returned so the supervisor retries the range and re-attempts.
func (p *SourceIndexerService) registerFactoryChildren(dbLogs []types.EvmLog) error {
	rules, err := p.factoryRules()
	if err != nil {
		return err
	}

	for _, log := range dbLogs {
		for _, rule := range rules {
			if log.Metadata.EventName != rule.CreationFunctionName {
				continue
			}

			// Only create the child when the rule's conditions on the event args hold.
			if !ruleConditionsPass(rule, log.Metadata.Data) {
				continue
			}

			newContractAddress, ok := log.Metadata.Data[rule.CreationAddressLogArg]
			if !ok {
				p.logger.Error().Fields(map[string]interface{}{
					"transaction": log.TransactionHash,
					"logIndex":    log.LogIndex,
					"block":       log.BlockNumber,
					"rule":        rule.ID,
				}).Msg("Unable to retrieve new contract from factory log")
				continue
			}

			if err := p.registerFactoryChild(rule, newContractAddress, log.BlockNumber); err != nil {
				p.logger.Error().Msg("factory child registration failed: " + err.Error())
				return err
			}
		}
	}

	return nil
}

func (p *SourceIndexerService) Serve(ctx context.Context) error {

	logParams := map[string]interface{}{
		"type": p.source.Type,
	}

	p.logger.Info().Fields(logParams).Msg("starting subprocess")

	p.logger.Info().Fields(logParams).Msg("loading blockchain")
	result := p.db.Conn.First(&p.chain, p.source.EvmBlockchainID)
	if result.Error != nil {
		return result.Error
	}

	p.logger.Info().Fields(logParams).Msg("loading pipeline")
	result = p.db.Conn.First(&p.pipeline, p.source.EvmLogPipelineID)
	if result.Error != nil {
		return result.Error
	}

	p.logger.Info().Fields(logParams).Msg("loading store")
	result = p.db.Conn.First(&p.storeInfo, p.pipeline.EvmLogStoreId)
	if result.Error != nil {
		return result.Error
	}

	// FULL sources index every log without decoding, so they need no ABI.
	if p.source.Type != string(evmi_database.FullLogSourceType) {
		p.logger.Info().Fields(logParams).Msg("loading abi")
		var abiEntry evmi_database.EvmJsonAbi
		result = p.db.Conn.First(&abiEntry, p.source.EvmJsonAbiID)
		if result.Error != nil {
			return result.Error
		}

		abi, err := abi.JSON(strings.NewReader(abiEntry.Content))
		if err != nil {
			return err
		}

		p.contractName = abiEntry.ContractName
		p.abi = abi
	}

	p.logger.Info().Fields(logParams).Msg("loading store config")
	var storeConfig map[string]string
	err := json.Unmarshal(p.storeInfo.StoreConfig, &storeConfig)
	if err != nil {
		return err
	}

	p.logger.Info().Fields(logParams).Msg("connecting store")
	p.store, err = log_stores.LoadStore(p.storeInfo.StoreType, storeConfig, p.logger)
	if err != nil {
		p.logger.Error().Msg(err.Error())
		return err
	}
	// The store lives exactly as long as this Serve call: its pool is
	// released on every exit path, so a supervisor restart never stacks a
	// fresh pool on top of an idling one.
	defer func() {
		if closeErr := p.store.Close(); closeErr != nil {
			p.logger.Error().Fields(logParams).Msg("store close error: " + closeErr.Error())
		}
	}()

	p.logger.Info().Fields(logParams).Msg("update source")
	// Only update the columns this worker owns (status, sync_block): the row is
	// shared with the manager (enabled) and a full-row Save would write a stale
	// copy of the other columns back.
	p.source.Status = string(evmi_database.RunningLogSourceStatus)
	result = p.db.Conn.Model(&p.source).Update("status", p.source.Status)
	if result.Error != nil {
		return result.Error
	}
	p.emitSourceUpdate()

	p.logger.Info().Fields(logParams).Msg("source updates")

	// Mark the source up for the lifetime of its indexing loop.
	p.metrics.SetSourceUp(p.sourceLabels(), true)
	defer p.metrics.SetSourceUp(p.sourceLabels(), false)

	switch p.source.Type {
	case string(evmi_database.ContractLogSourceType), string(evmi_database.FactoryLogSourceType):
		return p.serveIndexation(ctx, func(fromBlock, toBlock *big.Int) ethereum.FilterQuery {
			return ethereum.FilterQuery{
				FromBlock: fromBlock,
				ToBlock:   toBlock,
				Addresses: []common.Address{common.HexToAddress(p.source.Address.String)},
			}
		})
	case string(evmi_database.TopicLogSourceType):
		return p.serveIndexation(ctx, func(fromBlock, toBlock *big.Int) ethereum.FilterQuery {
			return ethereum.FilterQuery{
				FromBlock: fromBlock,
				ToBlock:   toBlock,
				Topics:    p.topicFilters(),
			}
		})
	case string(evmi_database.FullLogSourceType):
		return p.serveIndexation(ctx, func(fromBlock, toBlock *big.Int) ethereum.FilterQuery {
			return ethereum.FilterQuery{
				FromBlock: fromBlock,
				ToBlock:   toBlock,
			}
		})
	}

	return errors.New("config types invalid")
}

// topicFilters generates a positional topic request: topics[0] is the event
// signature (topic0); topics[1..] are per-indexed-argument filters in
// declaration order. An empty filter entry is a wildcard (match any value at
// that position), represented by a nil inner slice.
func (p *SourceIndexerService) topicFilters() [][]common.Hash {
	topics := [][]common.Hash{{common.HexToHash(p.source.Topic0.String)}}
	for _, topic := range p.source.TopicFilters {
		if len(topic) == 0 {
			topics = append(topics, nil)
		} else {
			topics = append(topics, []common.Hash{common.HexToHash(topic)})
		}
	}
	return topics
}

// serveIndexation is the poll loop shared by every source type: wait for the
// chain head to move, fetch the filtered logs in BlockRange windows, decode and
// store them, advance the SyncBlock cursor. Errors are returned (never fataled)
// so the supervisor restarts this source alone — the cursor was not advanced,
// so the failed range is replayed on restart.
func (p *SourceIndexerService) serveIndexation(ctx context.Context, filter func(fromBlock, toBlock *big.Int) ethereum.FilterQuery) error {
	client, err := w3.Dial(p.chain.RpcUrl)
	if err != nil {
		return err
	}
	defer client.Close()

	latestBlockNumberIndexed := p.source.SyncBlock
	if latestBlockNumberIndexed < p.source.StartBlock {
		latestBlockNumberIndexed = p.source.StartBlock
	}

	// seen is the head this loop last acted on. Starting from the current one
	// makes a (re)started source wait for the next block before its first
	// read, as the fixed PullInterval sleep did.
	var seen uint64
	if p.head != nil {
		seen, _ = p.head.Head()
	}

	for {
		if err := p.waitHead(ctx, seen); err != nil {
			return p.markStopped()
		}

		currentBlock, known, err := p.chainHead(client)
		if err != nil {
			return err
		}
		if !known {
			continue // the shared watcher has no answer yet
		}
		seen = currentBlock

		p.metrics.SetSourceProgress(p.sourceLabels(), currentBlock, p.source.SyncBlock)

		// The head must be at least BlockSlice ahead of the cursor. Compared
		// without subtracting first: a lagging RPC node can report a head below
		// the cursor and the unsigned difference would wrap around.
		if currentBlock <= latestBlockNumberIndexed || currentBlock-latestBlockNumberIndexed < p.chain.BlockSlice {
			continue
		}

		// The shared watcher already read these blocks and this source's
		// address emitted nothing in them: the cursor moves without a request.
		if addr, ok := p.watchedAddress(); ok && p.head.Quiet(latestBlockNumberIndexed+1, currentBlock, addr) {
			if err := p.advanceCursor(currentBlock, currentBlock); err != nil {
				return err
			}
			latestBlockNumberIndexed = currentBlock
			continue
		}

		for i := latestBlockNumberIndexed + 1; i <= currentBlock; i += p.chain.BlockRange {
			if ctx.Err() != nil {
				return p.markStopped()
			}

			toBlock := i + p.chain.BlockRange - 1
			if toBlock > currentBlock {
				toBlock = currentBlock
			}

			if err := p.indexRange(client, filter, i, toBlock, currentBlock); err != nil {
				return err
			}
		}

		latestBlockNumberIndexed = currentBlock
	}
}

// indexRange fetches, decodes and stores the logs of [fromBlock, toBlock], then
// advances the source cursor. The cursor is only written after a successful
// store write, so a failure anywhere replays the whole range.
func (p *SourceIndexerService) indexRange(client *w3.Client, filter func(fromBlock, toBlock *big.Int) ethereum.FilterQuery, from, to, head uint64) error {
	rangeStart := time.Now()
	fromBlock := new(big.Int).SetUint64(from)
	toBlock := new(big.Int).SetUint64(to)

	source := p.source.Address.String
	if source == "" {
		source = p.source.Topic0.String
	}

	logParams := map[string]interface{}{
		"store":     p.storeInfo.Identifier,
		"source":    source,
		"fromBlock": fromBlock,
		"toBlock":   toBlock,
	}

	p.logger.Info().Fields(logParams).Msg("Fetch logs")

	var logs []ethTypes.Log
	if err := p.timedRPC("eth_getLogs", func() error {
		return client.Call(eth.Logs(filter(fromBlock, toBlock)).Returns(&logs))
	}); err != nil {
		p.logger.Error().Fields(logParams).Msg(err.Error())
		return err
	}

	if err := p.checkAgainstSummaries(from, to, logs); err != nil {
		p.logger.Error().Fields(logParams).Msg(err.Error())
		return err
	}

	dbLogs, dbTxs, err := p.computeLogsAndTxs(client, logs)
	if err != nil {
		p.logger.Error().Fields(logParams).Msg(err.Error())
		return err
	}

	if len(dbLogs) > 0 {
		logsStart := time.Now()
		err = p.store.GetStorage().InsertLogs(dbLogs)
		p.metrics.ObserveStoreWrite(p.storeInfo.Identifier, "logs", time.Since(logsStart), err)
		if err != nil {
			p.logger.Error().Msg(err.Error())
			return err
		}

		p.bus.Emit(context.Background(), "logs.new", dbLogs)

		//If factory mode, check if there is new contract trigger
		if p.source.Type == string(evmi_database.FactoryLogSourceType) {
			if err := p.registerFactoryChildren(dbLogs); err != nil {
				return err
			}
		}

		p.metrics.AddLogsIndexed(p.sourceLabels(), uint64(len(dbLogs)))
	}

	if len(dbTxs) > 0 {
		txStart := time.Now()
		err = p.store.GetStorage().InsertTransactions(dbTxs)
		p.metrics.ObserveStoreWrite(p.storeInfo.Identifier, "transactions", time.Since(txStart), err)
		if err != nil {
			p.logger.Error().Msg(err.Error())
			return err
		}
		p.metrics.AddTransactionsIndexed(p.sourceLabels(), uint64(len(dbTxs)))
	}

	if err := p.advanceCursor(to, head); err != nil {
		return err
	}

	// Emitted after the cursor is persisted: an exporter woken earlier would
	// not see these logs as exportable yet. Only a source that has reached the
	// head wakes it: a backfill keeps the pace of the PullInterval timer, so an
	// exporter does not start on one source while the others are far behind.
	if len(dbLogs) > 0 && to == head {
		p.bus.Emit(context.Background(), internal_bus.LogsIndexedTopic, p.pipeline.ID)
	}

	p.metrics.ObserveBatchDuration(p.sourceLabels(), time.Since(rangeStart))
	return nil
}

// advanceCursor persists the source cursor at block to and publishes the
// progress; head is the chain head the progress is measured against.
func (p *SourceIndexerService) advanceCursor(to, head uint64) error {
	p.source.SyncBlock = to
	tx := p.db.Conn.Model(&p.source).Update("sync_block", to)
	if tx.Error != nil {
		p.logger.Error().Msg(tx.Error.Error())
		return tx.Error
	}

	p.emitSourceUpdate()

	p.metrics.SetSourceProgress(p.sourceLabels(), head, p.source.SyncBlock)
	return nil
}

// watchedAddress is the contract address the shared head watcher can answer
// for: only CONTRACT and FACTORY sources filter on one address. It reports
// false when the source has no watcher.
func (p *SourceIndexerService) watchedAddress() (common.Address, bool) {
	if p.head == nil || p.source.Address.String == "" {
		return common.Address{}, false
	}
	switch p.source.Type {
	case string(evmi_database.ContractLogSourceType), string(evmi_database.FactoryLogSourceType):
		return common.HexToAddress(p.source.Address.String), true
	}
	return common.Address{}, false
}

// checkAgainstSummaries refuses an eth_getLogs answer that misses a block in
// which the watcher saw this source's address emit: a load-balanced provider
// can answer from a node that is behind and return nothing for a block it does
// not have yet. The summary of the block is dropped before failing, so the
// replay is not held up should the summary be the stale side (a reorg).
func (p *SourceIndexerService) checkAgainstSummaries(from, to uint64, logs []ethTypes.Log) error {
	addr, ok := p.watchedAddress()
	if !ok {
		return nil
	}
	expected := p.head.EmittedIn(from, to, addr)
	if len(expected) == 0 {
		return nil
	}
	got := make(map[uint64]struct{}, len(logs))
	for i := range logs {
		got[logs[i].BlockNumber] = struct{}{}
	}
	for _, block := range expected {
		if _, found := got[block]; !found {
			p.head.Forget(block)
			return fmt.Errorf("eth_getLogs returned nothing for block %d, where this source has logs", block)
		}
	}
	return nil
}

// waitHead waits for a head other than seen when the chain head is shared,
// one PullInterval at most; a source polling the head itself sleeps one
// PullInterval.
func (p *SourceIndexerService) waitHead(ctx context.Context, seen uint64) error {
	if p.head == nil {
		return p.waitPullInterval(ctx)
	}
	return p.head.Wait(ctx, seen)
}

// chainHead is the latest block number: read from the shared per-chain watcher
// when the service was given one (one eth_blockNumber per chain per interval,
// however many sources), else fetched with the source's own client.
func (p *SourceIndexerService) chainHead(client *w3.Client) (uint64, bool, error) {
	if p.head != nil {
		n, ok := p.head.Head()
		return n, ok, nil
	}
	var block *big.Int
	if err := p.timedRPC("eth_blockNumber", func() error {
		return client.Call(eth.BlockNumber().Returns(&block))
	}); err != nil {
		return 0, false, err
	}
	p.metrics.SetChainHead(p.chain.ChainId, block.Uint64())
	return block.Uint64(), true, nil
}

// waitPullInterval sleeps one PullInterval, returning early with the context
// error when the supervisor stops this source, so disable/shutdown doesn't wait
// a full interval.
func (p *SourceIndexerService) waitPullInterval(ctx context.Context) error {
	timer := time.NewTimer(time.Duration(p.chain.PullInterval) * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// markStopped persists the STOPPED status when the supervisor cancels this
// service (source disable or shutdown).
func (p *SourceIndexerService) markStopped() error {
	p.source.Status = string(evmi_database.StoppedLogSourceStatus)
	if tx := p.db.Conn.Model(&p.source).Update("status", p.source.Status); tx.Error != nil {
		return tx.Error
	}
	p.emitSourceUpdate()
	return nil
}

// emitSourceUpdate broadcasts the source's current state (sync block, status, …)
// on the bus so subscribers (e.g. the StreamEvmLogSourceUpdates gRPC stream) can
// observe indexing progress live.
func (p *SourceIndexerService) emitSourceUpdate() {
	p.bus.Emit(context.Background(), internal_bus.SourceUpdateTopic, p.source)
}

// sourceLabels is the consistent metric label set for this source. All per-source
// metrics go through it so pipeline/store/chain/id/type are always in sync.
func (p *SourceIndexerService) sourceLabels() metrics.SourceLabels {
	return metrics.SourceLabels{
		ChainID:    p.chain.ChainId,
		Pipeline:   p.pipeline.Name,
		Store:      p.storeInfo.Identifier,
		SourceID:   p.source.ID,
		SourceType: p.source.Type,
	}
}

// timedRPC runs one JSON-RPC call, recording its latency and outcome under the
// given method name.
func (p *SourceIndexerService) timedRPC(method string, fn func() error) error {
	start := time.Now()
	err := fn()
	p.metrics.RecordRPC(p.chain.ChainId, method, time.Since(start), err)
	return err
}

func (p *SourceIndexerService) GetLogMetadata(log ethTypes.Log) types.EvmMetadata {

	// FULL sources are not decoded; anonymous events (no topic0) cannot be
	// matched against the ABI.
	if p.source.Type == string(evmi_database.FullLogSourceType) || len(log.Topics) == 0 {
		return types.EvmMetadata{
			ContractName: "Unknown",
			EventName:    "Unknown",
			Data:         map[string]string{},
		}
	}

	var eventName string = ""
	event := map[string]any{}
	for _, evInfo := range p.abi.Events {
		if evInfo.ID.Hex() != log.Topics[0].Hex() {
			continue
		}

		eventName = evInfo.RawName

		indexed := make([]abi.Argument, 0)
		for _, input := range evInfo.Inputs {
			if input.Indexed {
				indexed = append(indexed, input)
			}
		}

		// The raw topics and data are stored on the log regardless, so a decode
		// failure must not stop the source: keep whatever decoded and move on.
		if err := abi.ParseTopicsIntoMap(event, indexed, log.Topics[1:]); err != nil {
			p.logger.Warn().Str("event", eventName).Msg("topics decode failed: " + err.Error())
		}
		if err := p.abi.UnpackIntoMap(event, evInfo.Name, log.Data); err != nil {
			p.logger.Warn().Str("event", eventName).Msg("data decode failed: " + err.Error())
		}

		break
	}

	formatedEvent := map[string]string{}
	for k, v := range event {
		formatedEvent[k] = formatArgValue(v)
	}

	return types.EvmMetadata{
		ContractName: p.contractName,
		EventName:    eventName,
		Data:         formatedEvent,
	}
}

// formatArgValue renders one decoded ABI argument as a string. Byte arrays and
// slices are hex-encoded; anything without a dedicated case falls back to
// fmt.Sprint so an exotic Solidity type degrades to a readable value instead of
// stopping the source.
func formatArgValue(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case bool:
		return fmt.Sprint(val)
	case common.Address:
		return val.Hex()
	case *big.Int:
		return val.String()
	case []byte:
		return hex.EncodeToString(val)
	}

	// [N]uint8 fixed-byte arrays (bytes1..bytes32, common.Hash, …).
	rv := reflect.ValueOf(v)
	if (rv.Kind() == reflect.Array || rv.Kind() == reflect.Slice) && rv.Type().Elem().Kind() == reflect.Uint8 {
		bytes := make([]byte, rv.Len())
		for i := range bytes {
			bytes[i] = byte(rv.Index(i).Uint())
		}
		return hex.EncodeToString(bytes)
	}

	return fmt.Sprint(v)
}

func (p *SourceIndexerService) computeLogsAndTxs(client *w3.Client, logs []ethTypes.Log) ([]types.EvmLog, []types.EvmTransaction, error) {
	dbLogs := []types.EvmLog{}
	dbTxs := []types.EvmTransaction{}

	if len(logs) == 0 {
		p.logger.Info().Msg("No logs found")

		return dbLogs, dbTxs, nil
	}

	p.logger.Info().Msg(fmt.Sprintf("%d logs found", len(logs)))

	alreadyIndexedTx := make(map[string]bool)
	transactionToLoad := []common.Hash{}
	alreadyIndexedBlock := make(map[string]bool)
	blockToLoad := []common.Hash{}
	for _, log := range logs {
		txHash := log.TxHash.Hex()
		if !alreadyIndexedTx[txHash] {
			transactionToLoad = append(transactionToLoad, log.TxHash)
			alreadyIndexedTx[txHash] = true
		}
		blockHash := log.BlockHash.Hex()
		if !alreadyIndexedBlock[blockHash] {
			blockToLoad = append(blockToLoad, log.BlockHash)
			alreadyIndexedBlock[blockHash] = true
		}
	}

	// Load the block headers of the range's blocks to get their timestamps
	// (block.timestamp, not on the transaction itself), keyed by block hash.
	blockTimestamps, err := p.loadBlockTimestamps(client, blockToLoad)
	if err != nil {
		return nil, nil, err
	}

	p.logger.Info().Msg(fmt.Sprintf("%d transactions to load", len(transactionToLoad)))

	// build rpc calls
	maxBatchRequest := p.chain.RpcMaxBatchSize
	transactions := make([]*ethTypes.Transaction, len(transactionToLoad))

	var isEnded bool = false
	var currentTransactionIndex uint64 = 0
	for !isEnded {
		remainingRequests := uint64(len(transactionToLoad)) - currentTransactionIndex
		p.logger.Info().Msg(fmt.Sprintf("%d remaining requests", remainingRequests))
		var requestSize uint64
		if remainingRequests > maxBatchRequest {
			requestSize = maxBatchRequest
		} else {
			requestSize = remainingRequests
		}

		request := make([]w3types.RPCCaller, requestSize)
		for i := 0; i < int(requestSize); i++ {
			if currentTransactionIndex < uint64(len(transactionToLoad)) {
				request[i] = eth.Tx(transactionToLoad[currentTransactionIndex]).Returns(&transactions[currentTransactionIndex])
				currentTransactionIndex += 1
				continue
			}
		}

		batchStart := time.Now()
		batchCallErr := client.Call(request...)
		p.metrics.RecordRPC(p.chain.ChainId, "eth_getTransactionByHash", time.Since(batchStart), batchCallErr)
		if batchCallErr != nil {
			// A partial batch failure (w3.CallErrors) leaves nil transactions
			// behind: fail the whole range so the supervisor replays it.
			p.logger.Error().Msg(batchCallErr.Error())
			return nil, nil, batchCallErr
		}

		if currentTransactionIndex == uint64(len(transactionToLoad)) {
			isEnded = true
		}
	}

	for _, log := range logs {

		logData := map[string]interface{}{
			"LogSourceId": p.source.ID,
			"ChainId":     p.chain.ChainId,
			"BlockNumber": log.BlockNumber,

			"Address":         log.Address.Hex(),
			"TransactionHash": log.TxHash.Hex(),
			"LogIndex":        uint64(log.Index),
		}

		p.logger.Info().Fields(logData).Msg("Log found")

		var (
			transaction      *ethTypes.Transaction
			transactionIndex int
		)

		for index, tx := range transactions {
			if tx != nil && tx.Hash().Hex() == log.TxHash.Hex() {
				transaction = tx
				transactionIndex = index
			}
		}

		if transaction == nil {
			return nil, nil, errors.New("transaction not found")
		}

		sender, err := getTxSender(big.NewInt(int64(p.chain.ChainId)), transaction)
		if err != nil {
			p.logger.Error().Msg("TX: " + transaction.Hash().Hex())
			p.logger.Error().Msg(err.Error())
			return nil, nil, err
		}

		var to string
		if transaction.To() == nil {
			to = "0x0000000000000000000000000000000000000000"
		} else {
			to = transaction.To().Hex()
		}

		evmTx := types.EvmTransaction{
			Id:               fmt.Sprintf("%d:%s", transaction.ChainId().Uint64(), log.TxHash.Hex()),
			SourceId:         p.source.ID,
			BlockNumber:      log.BlockNumber,
			BlockTimestamp:   blockTimestamps[log.BlockHash.Hex()],
			ChainId:          transaction.ChainId().Uint64(),
			From:             sender,
			Data:             common.Bytes2Hex(transaction.Data()),
			Value:            transaction.Value().String(),
			TransactionIndex: uint64(transactionIndex),
			Nonce:            transaction.Nonce(),
			To:               to,
			Hash:             log.TxHash.Hex(),
		}

		dbTxs = append(dbTxs, evmTx)

		logTopics := []string{}
		for _, topic := range log.Topics {
			logTopics = append(logTopics, topic.Hex())
		}

		l := types.EvmLog{
			Id:               fmt.Sprintf("%d:%d:%d", transaction.ChainId().Uint64(), log.BlockNumber, log.Index),
			SourceId:         p.source.ID,
			ChainId:          p.chain.ChainId,
			Address:          log.Address.Hex(),
			Topics:           logTopics,
			Data:             common.Bytes2Hex(log.Data),
			BlockNumber:      log.BlockNumber,
			BlockTimestamp:   blockTimestamps[log.BlockHash.Hex()],
			TransactionHash:  log.TxHash.Hex(),
			TransactionFrom:  sender,
			TransactionIndex: uint64(log.TxIndex),
			BlockHash:        log.BlockHash.Hex(),
			LogIndex:         uint64(log.Index),
			Removed:          log.Removed,
			Metadata:         p.GetLogMetadata(log),
		}

		dbLogs = append(dbLogs, l)
	}

	return dbLogs, dbTxs, nil
}

// loadBlockTimestamps batch-fetches the headers of the given block hashes and
// returns a blockHash(hex) -> unix timestamp map, honoring RpcMaxBatchSize.
func (p *SourceIndexerService) loadBlockTimestamps(client *w3.Client, blockHashes []common.Hash) (map[string]uint64, error) {
	timestamps := make(map[string]uint64, len(blockHashes))
	if len(blockHashes) == 0 {
		return timestamps, nil
	}

	maxBatchRequest := p.chain.RpcMaxBatchSize
	if maxBatchRequest == 0 {
		maxBatchRequest = uint64(len(blockHashes))
	}

	headers := make([]*ethTypes.Header, len(blockHashes))
	for start := 0; start < len(blockHashes); start += int(maxBatchRequest) {
		end := start + int(maxBatchRequest)
		if end > len(blockHashes) {
			end = len(blockHashes)
		}

		request := make([]w3types.RPCCaller, 0, end-start)
		for i := start; i < end; i++ {
			request = append(request, eth.HeaderByHash(blockHashes[i]).Returns(&headers[i]))
		}

		batchStart := time.Now()
		batchCallErr := client.Call(request...)
		p.metrics.RecordRPC(p.chain.ChainId, "eth_getBlockByHash", time.Since(batchStart), batchCallErr)
		if batchCallErr != nil {
			p.logger.Error().Msg(batchCallErr.Error())
			return nil, batchCallErr
		}
	}

	for i, header := range headers {
		if header == nil {
			return nil, errors.New("block header not found for " + blockHashes[i].Hex())
		}
		timestamps[blockHashes[i].Hex()] = header.Time
	}
	return timestamps, nil
}

func getTxSender(chainId *big.Int, tx *ethTypes.Transaction) (string, error) {
	sender, err := ethTypes.Sender(ethTypes.NewPragueSigner(chainId), tx)
	if err != nil {
		return "", err
	}

	return sender.Hex(), nil
}
