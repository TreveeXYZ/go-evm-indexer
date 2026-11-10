// Package sql_store implements the EvmIndexerStorage backend on a relational
// database via GORM. The same implementation serves MySQL and PostgreSQL (and
// SQLite, used in tests) — only the dialect differs. Logs and transactions live
// in flat tables keyed by their stable id (inserts dedupe on conflict); complex
// fields (topics, metadata map) are JSON-encoded into text columns.
package sql_store

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/evmi-cloud/go-evm-indexer/internal/types"
	"github.com/rs/zerolog"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormlogger "gorm.io/gorm/logger"
)

type SQLStore struct {
	logger  zerolog.Logger
	dialect string
	db      *gorm.DB
}

// NewSQLStore creates a store for the given dialect: "mysql", "postgres" or
// "sqlite".
func NewSQLStore(dialect string, logger zerolog.Logger) (*SQLStore, error) {
	return &SQLStore{dialect: dialect, logger: logger}, nil
}

// intConfig reads an optional integer config key, falling back on a default
// when the key is absent or not a number.
func intConfig(config map[string]string, key string, fallback int) int {
	if v, ok := config[key]; ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func (s *SQLStore) Init(config map[string]string) error {
	dsn := config["dsn"]
	if dsn == "" {
		return fmt.Errorf("sql store: config \"dsn\" is required")
	}

	var dialector gorm.Dialector
	switch s.dialect {
	case "mysql":
		dialector = mysql.Open(dsn)
	case "postgres":
		dialector = postgres.Open(dsn)
	case "sqlite":
		dialector = sqlite.Open(dsn)
	default:
		return fmt.Errorf("sql store: unknown dialect %q", s.dialect)
	}

	db, err := gorm.Open(dialector, &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)})
	if err != nil {
		return err
	}

	// Every store instance carries its own pool, and stores are instantiated per
	// indexer source, per exporter and per API request — with database/sql's
	// unbounded default, the instances together can exhaust a managed database's
	// connection cap on startup (Cloud SQL's smallest Postgres tiers allow 25
	// total). Bound each pool; both knobs are overridable per store config.
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(intConfig(config, "maxOpenConns", 10))
		sqlDB.SetMaxIdleConns(intConfig(config, "maxIdleConns", 2))
		sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	}

	if err := db.AutoMigrate(&sqlLog{}, &sqlTx{}); err != nil {
		return err
	}
	s.db = db
	return nil
}

// Close releases the pool. Idempotent: a closed or never-opened store is a
// no-op, so a service may close on every exit path.
func (s *SQLStore) Close() error {
	if s.db == nil {
		return nil
	}
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	s.db = nil
	return sqlDB.Close()
}

// --- models ---------------------------------------------------------------

type sqlLog struct {
	Id                   string `gorm:"column:id;type:varchar(255);primaryKey"`
	SourceId             uint   `gorm:"column:source_id;index"`
	ChainId              uint64 `gorm:"column:chain_id"`
	Address              string `gorm:"column:address;type:varchar(255)"`
	Topics               string `gorm:"column:topics;type:text"`
	Data                 string `gorm:"column:data;type:text"`
	BlockNumber          uint64 `gorm:"column:block_number;index"`
	BlockTimestamp       uint64 `gorm:"column:block_timestamp"`
	TransactionFrom      string `gorm:"column:transaction_from;type:varchar(255)"`
	TransactionHash      string `gorm:"column:transaction_hash;type:varchar(255)"`
	TransactionIndex     uint64 `gorm:"column:transaction_index"`
	BlockHash            string `gorm:"column:block_hash;type:varchar(255)"`
	LogIndex             uint64 `gorm:"column:log_index"`
	Removed              bool   `gorm:"column:removed"`
	MetadataContractName string `gorm:"column:metadata_contract_name;type:varchar(255)"`
	MetadataEventName    string `gorm:"column:metadata_event_name;type:varchar(255)"`
	MetadataFunctionName string `gorm:"column:metadata_function_name;type:varchar(255)"`
	MetadataData         string `gorm:"column:metadata_data;type:text"`
}

func (sqlLog) TableName() string { return "evm_logs" }

type sqlTx struct {
	Id                   string `gorm:"column:id;type:varchar(255);primaryKey"`
	SourceId             uint   `gorm:"column:source_id;index"`
	BlockNumber          uint64 `gorm:"column:block_number;index"`
	BlockTimestamp       uint64 `gorm:"column:block_timestamp"`
	TransactionIndex     uint64 `gorm:"column:transaction_index"`
	ChainId              uint64 `gorm:"column:chain_id"`
	From                 string `gorm:"column:from_address;type:varchar(255)"`
	Data                 string `gorm:"column:data;type:text"`
	Value                string `gorm:"column:value;type:varchar(255)"`
	Nonce                uint64 `gorm:"column:nonce"`
	To                   string `gorm:"column:to_address;type:varchar(255)"`
	Hash                 string `gorm:"column:hash;type:varchar(255)"`
	MetadataContractName string `gorm:"column:metadata_contract_name;type:varchar(255)"`
	MetadataEventName    string `gorm:"column:metadata_event_name;type:varchar(255)"`
	MetadataFunctionName string `gorm:"column:metadata_function_name;type:varchar(255)"`
	MetadataData         string `gorm:"column:metadata_data;type:text"`
}

func (sqlTx) TableName() string { return "evm_transactions" }

func toSqlLog(l types.EvmLog) sqlLog {
	topics, _ := json.Marshal(l.Topics)
	data, _ := json.Marshal(l.Metadata.Data)
	return sqlLog{
		Id: l.Id, SourceId: l.SourceId, ChainId: l.ChainId, Address: l.Address, Topics: string(topics), Data: l.Data,
		BlockNumber: l.BlockNumber, BlockTimestamp: l.BlockTimestamp, TransactionFrom: l.TransactionFrom, TransactionHash: l.TransactionHash,
		TransactionIndex: l.TransactionIndex, BlockHash: l.BlockHash, LogIndex: l.LogIndex, Removed: l.Removed,
		MetadataContractName: l.Metadata.ContractName, MetadataEventName: l.Metadata.EventName,
		MetadataFunctionName: l.Metadata.FunctionName, MetadataData: string(data),
	}
}

func fromSqlLog(r sqlLog) types.EvmLog {
	var topics []string
	_ = json.Unmarshal([]byte(r.Topics), &topics)
	data := map[string]string{}
	_ = json.Unmarshal([]byte(r.MetadataData), &data)
	return types.EvmLog{
		Id: r.Id, SourceId: r.SourceId, ChainId: r.ChainId, Address: r.Address, Topics: topics, Data: r.Data,
		BlockNumber: r.BlockNumber, BlockTimestamp: r.BlockTimestamp, TransactionFrom: r.TransactionFrom, TransactionHash: r.TransactionHash,
		TransactionIndex: r.TransactionIndex, BlockHash: r.BlockHash, LogIndex: r.LogIndex, Removed: r.Removed,
		Metadata: types.EvmMetadata{ContractName: r.MetadataContractName, EventName: r.MetadataEventName, FunctionName: r.MetadataFunctionName, Data: data},
	}
}

func toSqlTx(t types.EvmTransaction) sqlTx {
	data, _ := json.Marshal(t.Metadata.Data)
	return sqlTx{
		Id: t.Id, SourceId: t.SourceId, BlockNumber: t.BlockNumber, BlockTimestamp: t.BlockTimestamp, TransactionIndex: t.TransactionIndex, ChainId: t.ChainId,
		From: t.From, Data: t.Data, Value: t.Value, Nonce: t.Nonce, To: t.To, Hash: t.Hash,
		MetadataContractName: t.Metadata.ContractName, MetadataEventName: t.Metadata.EventName,
		MetadataFunctionName: t.Metadata.FunctionName, MetadataData: string(data),
	}
}

func fromSqlTx(r sqlTx) types.EvmTransaction {
	data := map[string]string{}
	_ = json.Unmarshal([]byte(r.MetadataData), &data)
	return types.EvmTransaction{
		Id: r.Id, SourceId: r.SourceId, BlockNumber: r.BlockNumber, BlockTimestamp: r.BlockTimestamp, TransactionIndex: r.TransactionIndex, ChainId: r.ChainId,
		From: r.From, Data: r.Data, Value: r.Value, Nonce: r.Nonce, To: r.To, Hash: r.Hash,
		Metadata: types.EvmMetadata{ContractName: r.MetadataContractName, EventName: r.MetadataEventName, FunctionName: r.MetadataFunctionName, Data: data},
	}
}

// --- writes ---------------------------------------------------------------

func (s *SQLStore) InsertLogs(logs []types.EvmLog) error {
	if len(logs) == 0 {
		return nil
	}
	rows := make([]sqlLog, len(logs))
	for i, l := range logs {
		rows[i] = toSqlLog(l)
	}
	return s.db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(rows, 200).Error
}

func (s *SQLStore) InsertTransactions(txs []types.EvmTransaction) error {
	if len(txs) == 0 {
		return nil
	}
	rows := make([]sqlTx, len(txs))
	for i, t := range txs {
		rows[i] = toSqlTx(t)
	}
	return s.db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(rows, 200).Error
}

// DeleteSourceData removes every log and transaction row for the source.
func (s *SQLStore) DeleteSourceData(sourceId uint64) error {
	if err := s.db.Where("source_id = ?", sourceId).Delete(&sqlLog{}).Error; err != nil {
		return err
	}
	return s.db.Where("source_id = ?", sourceId).Delete(&sqlTx{}).Error
}

// --- reads ----------------------------------------------------------------

func (s *SQLStore) GetLogsCount() (uint64, error) {
	var count int64
	if err := s.db.Model(&sqlLog{}).Count(&count).Error; err != nil {
		return 0, err
	}
	return uint64(count), nil
}

func (s *SQLStore) GetLogs(sourceId uint64, fromBlock uint64, toBlock uint64) ([]types.EvmLog, error) {
	var rows []sqlLog
	err := s.db.
		Where("source_id = ? AND block_number >= ? AND block_number <= ?", sourceId, fromBlock, toBlock).
		Order("block_number asc, log_index asc").
		Find(&rows).Error
	return mapLogs(rows), err
}

func (s *SQLStore) GetLogsAfter(sourceIds []uint64, afterBlock uint64, afterLogIndex uint64, toBlock uint64) ([]types.EvmLog, error) {
	if len(sourceIds) == 0 {
		return []types.EvmLog{}, nil
	}
	var rows []sqlLog
	err := s.db.
		Where("source_id IN ? AND block_number <= ? AND (block_number > ? OR (block_number = ? AND log_index > ?))",
			sourceIds, toBlock, afterBlock, afterBlock, logIndexBound(afterLogIndex)).
		Order("block_number asc, log_index asc").
		Find(&rows).Error
	return mapLogs(rows), err
}

// logIndexBound converts a log-index bound to the signed value the query binds:
// a Postgres bigint cannot hold a uint64 above math.MaxInt64. The only such
// bound is ^uint64(0), sent for a cursor with no block in progress: afterBlock
// is fully delivered and nothing of it must come back. It binds as the largest
// bigint, which no log index exceeds. Bound as -1 it selected the whole block,
// and every log of the last exported block was delivered a second time.
func logIndexBound(afterLogIndex uint64) int64 {
	if afterLogIndex > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(afterLogIndex)
}

func (s *SQLStore) GetLogStream(sourceId uint64, fromBlock uint64, toBlock uint64, stream chan types.EvmLog) error {
	logs, err := s.GetLogs(sourceId, fromBlock, toBlock)
	if err != nil {
		close(stream)
		return err
	}
	for _, l := range logs {
		stream <- l
	}
	close(stream)
	return nil
}

func (s *SQLStore) GetLatestLogs(sourceId uint64, limit uint64) ([]types.EvmLog, error) {
	var rows []sqlLog
	err := s.db.
		Where("source_id = ?", sourceId).
		Order("block_number desc, log_index desc").
		Limit(int(limit)).
		Find(&rows).Error
	return mapLogs(rows), err
}

func (s *SQLStore) GetTransactions(sourceId uint64, fromBlock uint64, toBlock uint64) ([]types.EvmTransaction, error) {
	var rows []sqlTx
	err := s.db.
		Where("source_id = ? AND block_number >= ? AND block_number <= ?", sourceId, fromBlock, toBlock).
		Order("block_number desc").
		Find(&rows).Error
	out := make([]types.EvmTransaction, 0, len(rows))
	for _, r := range rows {
		out = append(out, fromSqlTx(r))
	}
	return out, err
}

func mapLogs(rows []sqlLog) []types.EvmLog {
	out := make([]types.EvmLog, 0, len(rows))
	for _, r := range rows {
		out = append(out, fromSqlLog(r))
	}
	return out
}
