package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	internal_bus "github.com/evmi-cloud/go-evm-indexer/internal/bus"
	evmi_database "github.com/evmi-cloud/go-evm-indexer/internal/database/evmi-database"
	log_stores "github.com/evmi-cloud/go-evm-indexer/internal/database/log-stores"
	"github.com/evmi-cloud/go-evm-indexer/internal/metrics"
	"github.com/evmi-cloud/go-evm-indexer/internal/types"
	pluginsdk "github.com/evmi-cloud/go-evm-indexer/pkg/exporter"
	"github.com/mustafaturan/bus/v3"
	"github.com/rs/zerolog"
	"gorm.io/gorm"
)

// defaultBlockBatch bounds how many blocks of logs are pulled from the store per
// iteration when the blockchain does not define a BlockRange.
const defaultBlockBatch uint64 = 1000

// wakeSettleDelay is how long an exporter woken by new logs waits before its
// next pass.
const wakeSettleDelay = 250 * time.Millisecond

// ExporterService runs a single plugin-backed exporter bound to one pipeline. It
// streams every stored log of the pipeline's sources into the plugin's
// NewLogEvent, committing the sync cursor after each log so it resumes cleanly
// after a restart.
//
// Progress is tracked per source (EvmiExporterSourceCursor), not as one
// pipeline-wide cursor: sources appear at runtime — a FACTORY rule or a plugin's
// Host.CreateLogSource can attach one at any time — and a shared cursor could only
// start such a source from wherever the exporter already stood, dropping every log
// already stored below that point. Each source instead carries its own cursor,
// seeded from its own StartBlock, and is exported up to its own SyncBlock.
//
// Ordering: logs of a single source are always delivered in ascending
// (block_number, log_index) order, and logs of sources that sit at the same cursor
// are merged so the batch is globally ordered too. A source that joins late is
// necessarily behind the others, so its backlog is delivered while they are
// already further ahead — catching up on a late source is worth more than a
// pipeline-wide ordering guarantee that cannot be kept anyway.
type ExporterService struct {
	db      *evmi_database.EvmiDatabase
	bus     *bus.Bus
	metrics *metrics.MetricService

	store  *log_stores.IndexerStore
	plugin pluginsdk.Exporter
	// pluginProcess is the go-plugin subprocess backing plugin. It is nil in
	// tests, which inject a plugin directly.
	pluginProcess *pluginProcess

	exporter  evmi_database.EvmiExporter
	pipeline  evmi_database.EvmLogPipeline
	chain     evmi_database.EvmBlockchain
	storeInfo evmi_database.EvmLogStore

	// cursors holds the in-memory position of every source currently being
	// exported, keyed by source id and mirrored into EvmiExporterSourceCursor
	// rows. Only the export loop touches it, so it needs no lock.
	cursors map[uint64]*sourceCursor

	logger zerolog.Logger
}

// sourceCursor is one source's export position for the lifetime of a Serve call.
type sourceCursor struct {
	sourceID uint64
	// rowID is the EvmiExporterSourceCursor row backing this cursor.
	rowID uint

	// block is the last fully-exported block of this source; logIndex is the last
	// log_index delivered within block+1, or -1 when none of it has been. Together
	// they pin the exact last log delivered for the source, so a restart resumes
	// mid-block rather than replaying it.
	block    uint64
	logIndex int64

	// source is the log source row, refreshed on every loadCursors pass. Its
	// SyncBlock is this cursor's head (the highest block safe to export); the rest
	// describes the source for the exporter.cursor bus updates.
	source evmi_database.EvmLogSource
}

// head is the highest block it is safe to export for this source: how far the
// indexer has actually stored it.
func (c *sourceCursor) head() uint64 { return c.source.SyncBlock }

// toUpdate renders the cursor as an exporter.cursor bus payload.
func (c *sourceCursor) toUpdate(exporterID uint) types.ExporterCursorUpdate {
	return types.ExporterCursorUpdate{
		ExporterID:      exporterID,
		SourceID:        c.source.ID,
		SyncBlock:       c.block,
		SyncLogIndex:    c.logIndex,
		SourceSyncBlock: c.source.SyncBlock,
		SourceType:      c.source.Type,
		SourceAddress:   c.source.Address.String,
		SourceTopic0:    c.source.Topic0.String,
		SourceEnabled:   c.source.Enabled,
		SourceStatus:    c.source.Status,
		ParentSourceID:  c.source.ParentSourceID,
	}
}

func NewExporterService(
	db *evmi_database.EvmiDatabase,
	bus *bus.Bus,
	metrics *metrics.MetricService,
	exporter evmi_database.EvmiExporter,
) *ExporterService {

	logger := zerolog.New(
		zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
	).Level(zerolog.TraceLevel).With().Timestamp().Caller().Logger()

	return &ExporterService{
		db:       db,
		bus:      bus,
		metrics:  metrics,
		exporter: exporter,
		logger:   logger,
	}
}

// emitUpdate broadcasts the exporter's current state (sync cursor, status) on the
// bus for the StreamEvmiExporterUpdates gRPC stream. Guarded so tests that
// construct an ExporterService without a bus don't panic.
func (p *ExporterService) emitUpdate() {
	if p.bus != nil {
		p.bus.Emit(context.Background(), internal_bus.ExporterUpdateTopic, p.exporter)
	}
}

func (p *ExporterService) Serve(ctx context.Context) error {

	logParams := map[string]interface{}{"exporter": p.exporter.Name}
	p.logger.Info().Fields(logParams).Msg("starting exporter")

	// Reload the exporter row so we resume from the persisted cursor.
	if result := p.db.Conn.First(&p.exporter, p.exporter.ID); result.Error != nil {
		return result.Error
	}

	// Cursors are rebuilt from the DB on every (re)start, so a supervisor restart
	// never carries a stale in-memory position forward.
	p.cursors = make(map[uint64]*sourceCursor)

	if result := p.db.Conn.First(&p.pipeline, p.exporter.EvmLogPipelineID); result.Error != nil {
		return result.Error
	}

	if result := p.db.Conn.First(&p.chain, p.pipeline.EvmBlockchainID); result.Error != nil {
		return result.Error
	}

	if result := p.db.Conn.First(&p.storeInfo, p.pipeline.EvmLogStoreId); result.Error != nil {
		return result.Error
	}

	var storeConfig map[string]string
	if err := json.Unmarshal(p.storeInfo.StoreConfig, &storeConfig); err != nil {
		return err
	}

	// The plugin first: while it is still being installed at boot, every
	// attempt fails here and costs nothing. Connecting the store before it
	// would open a pool per attempt — the supervisor retries in a tight
	// loop, and the pools of the failed attempts idled on until the
	// database's connection cap was hit (2026-09-12, 102 backends on a
	// 100-connection instance).
	p.logger.Info().Fields(logParams).Msg("starting plugin process")
	process, err := launchInstalledPlugin(p.db, p.exporter.PluginID, p.logger)
	if err != nil {
		p.fail(err)
		return err
	}
	// The plugin subprocess lives exactly as long as this Serve call, so
	// stopping an exporter releases its plugin entirely.
	defer process.Kill()
	p.pluginProcess = process
	p.plugin = process.Exporter()

	p.logger.Info().Fields(logParams).Msg("connecting store")
	store, err := log_stores.LoadStore(p.storeInfo.StoreType, storeConfig, p.logger)
	if err != nil {
		p.fail(err)
		return err
	}
	p.store = store
	// The store lives exactly as long as this Serve call too: whatever the
	// exit path, its pool is released before the supervisor restarts us.
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			p.logger.Error().Fields(logParams).Msg("store close error: " + closeErr.Error())
		}
	}()

	// Expose the host API before Init: a plugin is expected to register the ABIs
	// and look up the chain it needs from inside Init.
	process.SetHost(&exporterHost{
		db:           p.db,
		bus:          p.bus,
		pipelineID:   p.pipeline.ID,
		chain:        p.chain,
		exporterName: p.exporter.Name,
		logger:       p.logger,
	})

	if err := p.plugin.Init(pluginsdk.Context{
		ExporterName: p.exporter.Name,
		PipelineId:   uint64(p.pipeline.ID),
		ChainId:      p.chain.ChainId,
		Config:       []byte(p.exporter.PluginConfig),
	}); err != nil {
		p.fail(err)
		return err
	}

	p.setStatus(string(evmi_database.RunningExporterStatus))
	p.emitUpdate()

	// Mark the exporter up for the lifetime of its loop.
	p.metrics.SetExporterUp(p.exporterLabels(), true)
	defer p.metrics.SetExporterUp(p.exporterLabels(), false)

	err = p.run(ctx, logParams)

	// Always give the plugin a chance to flush/close.
	if closeErr := p.plugin.Close(); closeErr != nil {
		p.logger.Error().Fields(logParams).Msg("plugin close error: " + closeErr.Error())
	}
	return err
}

// setStatus persists the status column alone: the row is shared with the
// manager (enabled) and a full-row Save would write a stale copy back.
func (p *ExporterService) setStatus(status string) {
	p.exporter.Status = status
	p.db.Conn.Model(&p.exporter).Update("status", status)
}

// run is the main export loop. It returns nil on a clean stop (context
// cancelled by the supervisor) and an error if the plugin or store fails
// (letting the supervisor restart it).
//
// Every iteration re-reads the pipeline's enabled sources, so a source created
// mid-run — by a FACTORY rule or by a plugin calling Host.CreateLogSource — joins
// the export on the very next pass, with its own cursor, and its backlog is
// delivered from its first stored log instead of being skipped.
func (p *ExporterService) run(ctx context.Context, logParams map[string]interface{}) error {
	batch := p.chain.BlockRange
	if batch == 0 {
		batch = defaultBlockBatch
	}

	pullInterval := time.Duration(p.chain.PullInterval) * time.Second
	if pullInterval == 0 {
		pullInterval = time.Second
	}

	// wake is signalled when a source of this pipeline has stored new logs, so
	// an idle exporter delivers them now instead of at its next PullInterval.
	// The interval stays as the fallback: a missed signal only costs a delay.
	wake := make(chan struct{}, 1)
	if p.bus != nil {
		key := fmt.Sprintf("exporter-wake-%d", p.exporter.ID)
		p.bus.RegisterHandler(key, bus.Handler{
			Matcher: internal_bus.LogsIndexedTopic,
			Handle: func(_ context.Context, e bus.Event) {
				if id, ok := e.Data.(uint); !ok || id != p.pipeline.ID {
					return
				}
				// Never block the emitting source: one pending signal is enough.
				select {
				case wake <- struct{}{}:
				default:
				}
			},
		})
		defer p.bus.DeregisterHandler(key)
	}

	for {
		if ctx.Err() != nil {
			p.setStatus(string(evmi_database.StoppedExporterStatus))
			p.emitUpdate()
			return nil
		}

		// A plugin that crashed while idle has no in-flight call to report it, so
		// check the process here and let the supervisor restart the exporter
		// (which starts a fresh plugin process) rather than idling against a dead
		// one.
		if p.pluginProcess != nil && p.pluginProcess.Exited() {
			err := errors.New("plugin process exited unexpectedly")
			p.fail(err)
			return err
		}

		cursors, err := p.loadCursors()
		if err != nil {
			p.fail(err)
			return err
		}

		exported, err := p.exportStep(cursors, batch)
		if err != nil {
			return err
		}

		if !exported {
			// Nothing to do: either the pipeline has no enabled source, or every
			// source is already exported up to its own SyncBlock. Interruptible
			// sleep so a disable/shutdown doesn't wait a full pull interval.
			select {
			case <-ctx.Done():
			case <-time.After(pullInterval):
			case <-wake:
				// The sources of a pipeline store the same block one after the
				// other: a short pause lets the next pass take the whole block.
				select {
				case <-ctx.Done():
				case <-time.After(wakeSettleDelay):
				}
			}
			continue
		}

		block, logIndex := aggregateCursor(cursors)
		if err := p.persistExporterCursor(block, logIndex); err != nil {
			return err
		}

		p.emitUpdate()
		// One update per source per batch — the detail view's live feed. Per
		// delivered log would be far too chatty for a stream.
		p.emitCursorUpdates(cursors)
		p.metrics.SetExporterProgress(p.exporterLabels(), pipelineHead(cursors), block)
		p.logger.Info().Fields(map[string]interface{}{
			"exporter": p.exporter.Name, "sources": len(cursors), "syncBlock": block,
		}).Msg("exported block range")
	}
}

// loadCursors reconciles the cursor set with the pipeline's currently enabled
// sources: it attaches a cursor to every source that does not have one yet,
// refreshes each source's exportable head, and forgets sources that are no longer
// enabled (their rows stay, so re-enabling one resumes where it left off).
//
// The returned slice is ordered by source id so a batch is assembled
// deterministically.
func (p *ExporterService) loadCursors() ([]*sourceCursor, error) {
	var sources []evmi_database.EvmLogSource
	result := p.db.Conn.Model(&evmi_database.EvmLogSource{}).
		Where("evm_log_pipeline_id = ? AND enabled = ?", p.pipeline.ID, true).
		Find(&sources)
	if result.Error != nil {
		return nil, result.Error
	}

	active := make(map[uint64]*sourceCursor, len(sources))
	out := make([]*sourceCursor, 0, len(sources))
	var added []*sourceCursor
	for _, s := range sources {
		cursor, ok := p.cursors[uint64(s.ID)]
		if !ok {
			var err error
			if cursor, err = p.cursorFor(s); err != nil {
				return nil, err
			}
			added = append(added, cursor)
		}
		cursor.source = s
		active[cursor.sourceID] = cursor
		out = append(out, cursor)
	}
	p.cursors = active

	// Announce sources joining the exporter right away rather than waiting for
	// their first exported batch: a source attached by a plugin or a factory
	// should appear in the UI as soon as it is being tracked, even while it has
	// nothing to export yet.
	p.emitCursorUpdates(added)

	sort.Slice(out, func(i, j int) bool { return out[i].sourceID < out[j].sourceID })
	return out, nil
}

// emitCursorUpdates broadcasts each cursor's position on the bus for the
// StreamEvmiExporterSourceCursors gRPC stream. Guarded so tests that construct an
// ExporterService without a bus don't panic.
func (p *ExporterService) emitCursorUpdates(cursors []*sourceCursor) {
	if p.bus == nil {
		return
	}
	for _, c := range cursors {
		p.bus.Emit(context.Background(), internal_bus.ExporterCursorTopic, c.toUpdate(p.exporter.ID))
	}
}

// cursorFor loads the persisted cursor of one source, creating it on first sight.
//
// A source this exporter has never seen starts at its own StartBlock — the block
// *before* the first one the indexer stores for it — so a source attached long
// after the exporter started is exported from its very first log rather than from
// the exporter's current position. The exporter's own StartBlock still wins when
// it is further ahead: it is an explicit "export nothing before this block"
// instruction and applies to every source.
func (p *ExporterService) cursorFor(source evmi_database.EvmLogSource) (*sourceCursor, error) {
	var row evmi_database.EvmiExporterSourceCursor
	err := p.db.Conn.
		Where("evmi_exporter_id = ? AND evm_log_source_id = ?", p.exporter.ID, source.ID).
		First(&row).Error
	if err == nil {
		return &sourceCursor{
			sourceID: uint64(source.ID),
			rowID:    row.ID,
			block:    row.SyncBlock,
			logIndex: row.SyncLogIndex,
		}, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	start := source.StartBlock
	if p.exporter.StartBlock > 0 && start < p.exporter.StartBlock-1 {
		start = p.exporter.StartBlock - 1
	}

	row = evmi_database.EvmiExporterSourceCursor{
		EvmiExporterID: p.exporter.ID,
		EvmLogSourceID: source.ID,
		SyncBlock:      start,
		SyncLogIndex:   -1,
	}
	if err := p.db.Conn.Create(&row).Error; err != nil {
		return nil, err
	}

	p.logger.Info().Fields(map[string]interface{}{
		"exporter": p.exporter.Name, "source": source.ID, "fromBlock": start + 1,
	}).Msg("tracking new log source for export")

	return &sourceCursor{
		sourceID: uint64(source.ID),
		rowID:    row.ID,
		block:    row.SyncBlock,
		logIndex: row.SyncLogIndex,
	}, nil
}

// exportStep exports one batch: for every source that has stored blocks beyond its
// cursor it fetches up to batch blocks, merges the results into one ordered stream
// and delivers them to the plugin. It reports whether anything was exportable at
// all, so the caller can idle when the whole pipeline is caught up.
//
// Delivery is strictly sequential: logs are handed to NewLogEvent one by one in a
// plain loop, and a failure returns immediately with each source's cursor at its
// last successfully delivered log, so the failing log is replayed on restart
// (at-least-once). It never delivers logs concurrently, nor out of order for a
// given source.
func (p *ExporterService) exportStep(cursors []*sourceCursor, batch uint64) (bool, error) {
	// Record how long the batch took and how many events it delivered, on every
	// return path (success or mid-batch failure).
	start := time.Now()
	var delivered uint64
	defer func() {
		p.metrics.ObserveExporterProcess(p.exporterLabels(), time.Since(start))
		p.metrics.AddExporterEvents(p.exporterLabels(), delivered)
	}()

	// Sources sitting at the same cursor and target block are fetched together, so
	// the steady state (every source in lockstep) is still a single store query;
	// only a source that is behind the others costs an extra one.
	type fetchKey struct{ afterBlock, afterLogIndex, toBlock uint64 }
	groups := make(map[fetchKey][]uint64)
	order := make([]fetchKey, 0, 1)
	targets := make(map[uint64]uint64, len(cursors))

	for _, c := range cursors {
		if c.head() <= c.block {
			continue
		}
		toBlock := c.block + batch
		if toBlock > c.head() {
			toBlock = c.head()
		}
		afterBlock, afterLogIndex := cursorBound(c.block, c.logIndex)
		key := fetchKey{afterBlock, afterLogIndex, toBlock}
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], c.sourceID)
		targets[c.sourceID] = toBlock
	}

	if len(targets) == 0 {
		return false, nil
	}

	var logs []types.EvmLog
	for _, key := range order {
		fetched, err := p.store.GetStorage().GetLogsAfter(
			groups[key], key.afterBlock, key.afterLogIndex, key.toBlock)
		if err != nil {
			p.fail(err)
			return false, err
		}
		logs = append(logs, fetched...)
	}

	// Each group comes back ordered; merging them keeps the batch ordered across
	// sources too, with the source id breaking ties so the order is stable.
	sort.SliceStable(logs, func(i, j int) bool {
		if logs[i].BlockNumber != logs[j].BlockNumber {
			return logs[i].BlockNumber < logs[j].BlockNumber
		}
		if logs[i].LogIndex != logs[j].LogIndex {
			return logs[i].LogIndex < logs[j].LogIndex
		}
		return logs[i].SourceId < logs[j].SourceId // stable tie-break across sources
	})

	for _, l := range logs {
		cursor, ok := p.cursors[uint64(l.SourceId)]
		if !ok {
			// The source was disabled or deleted between the fetch and now; its
			// cursor is gone, so there is nothing to commit the delivery against.
			continue
		}
		if err := p.plugin.NewLogEvent(toLogEvent(l)); err != nil {
			p.fail(err)
			return false, err
		}
		delivered++
		// A delivered log at (B, I) means every block < B is complete for this
		// source and block B is in progress at index I.
		cursor.block = blockBefore(l.BlockNumber)
		cursor.logIndex = int64(l.LogIndex)
		if err := p.persistCursor(cursor); err != nil {
			return false, err
		}
	}

	// Every source's range has now been scanned up to its target: that block is
	// complete (including any empty tail) and no block is in progress.
	for _, c := range cursors {
		toBlock, ok := targets[c.sourceID]
		if !ok {
			continue
		}
		c.block = toBlock
		c.logIndex = -1
		if err := p.persistCursor(c); err != nil {
			return false, err
		}
	}
	return true, nil
}

// aggregateCursor is the exporter-level position: the minimum over its source
// cursors, i.e. the point up to which the *whole* pipeline has been exported.
func aggregateCursor(cursors []*sourceCursor) (uint64, int64) {
	block, logIndex := uint64(0), int64(-1)
	for i, c := range cursors {
		if i == 0 || c.block < block || (c.block == block && c.logIndex < logIndex) {
			block, logIndex = c.block, c.logIndex
		}
	}
	return block, logIndex
}

// pipelineHead is the highest block the indexer has stored for any source of the
// pipeline — the target the exporter is catching up to.
func pipelineHead(cursors []*sourceCursor) uint64 {
	var head uint64
	for _, c := range cursors {
		if c.head() > head {
			head = c.head()
		}
	}
	return head
}

// blockBefore returns b-1, guarding the genesis edge (block 0 with logs is not
// resumable mid-block; such logs are effectively never present on EVM chains).
func blockBefore(b uint64) uint64 {
	if b == 0 {
		return 0
	}
	return b - 1
}

// cursorBound converts a (completedBlock, lastLogIndex) cursor into a strict
// "after this log" bound for GetLogsAfter.
func cursorBound(completedBlock uint64, lastLogIndex int64) (uint64, uint64) {
	if lastLogIndex < 0 {
		// Nothing in progress: resume strictly after the completed block.
		return completedBlock, ^uint64(0)
	}
	// Mid-block: resume strictly after (completedBlock+1, lastLogIndex).
	return completedBlock + 1, uint64(lastLogIndex)
}

// persistCursor writes one source's cursor to its EvmiExporterSourceCursor row.
// It is called after every delivered log, so the row is always the truth about
// what that source has already handed to the plugin.
func (p *ExporterService) persistCursor(cursor *sourceCursor) error {
	result := p.db.Conn.Model(&evmi_database.EvmiExporterSourceCursor{}).
		Where("id = ?", cursor.rowID).
		Updates(map[string]interface{}{
			"sync_block":     cursor.block,
			"sync_log_index": cursor.logIndex,
		})
	if result.Error != nil {
		p.fail(result.Error)
		return result.Error
	}
	return nil
}

// persistExporterCursor writes the aggregate position back to the exporter row.
// Unlike the per-source cursors it is written once per batch, not once per log:
// nothing resumes from it, it only feeds the API/UI and the progress metrics.
func (p *ExporterService) persistExporterCursor(block uint64, logIndex int64) error {
	p.exporter.SyncBlock = block
	p.exporter.SyncLogIndex = logIndex
	result := p.db.Conn.Model(&p.exporter).Updates(map[string]interface{}{
		"sync_block":     block,
		"sync_log_index": logIndex,
	})
	if result.Error != nil {
		p.fail(result.Error)
		return result.Error
	}
	return nil
}

func (p *ExporterService) fail(err error) {
	p.logger.Error().Str("exporter", p.exporter.Name).Msg(err.Error())
	p.metrics.IncExporterErrors(p.exporterLabels())
	p.setStatus(string(evmi_database.FailedExporterStatus))
	p.emitUpdate()
}

// exporterLabels is the consistent metric label set for this exporter.
func (p *ExporterService) exporterLabels() metrics.ExporterLabels {
	return metrics.ExporterLabels{
		ChainID:  p.chain.ChainId,
		Pipeline: p.pipeline.Name,
		Exporter: p.exporter.Name,
	}
}

func toLogEvent(l types.EvmLog) pluginsdk.LogEvent {
	return pluginsdk.LogEvent{
		Id:               l.Id,
		SourceId:         l.SourceId,
		ChainId:          l.ChainId,
		Address:          l.Address,
		Topics:           l.Topics,
		Data:             l.Data,
		BlockNumber:      l.BlockNumber,
		BlockTimestamp:   l.BlockTimestamp,
		TransactionHash:  l.TransactionHash,
		TransactionFrom:  l.TransactionFrom,
		TransactionIndex: l.TransactionIndex,
		BlockHash:        l.BlockHash,
		LogIndex:         l.LogIndex,
		Removed:          l.Removed,

		ContractName: l.Metadata.ContractName,
		EventName:    l.Metadata.EventName,
		Args:         l.Metadata.Data,
	}
}
