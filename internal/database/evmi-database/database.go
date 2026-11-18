package evmi_database

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/evmi-cloud/go-evm-indexer/internal/logsafe"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type DatabaseType string

const (
	SqliteDatabaseType   DatabaseType = "SQLITE"
	PostgresDatabaseType DatabaseType = "POSTGRES"
	MysqlDatabaseType    DatabaseType = "MYSQL"
)

type EvmiDatabase struct {
	Conn *gorm.DB
}

func LoadDatabase(dbType DatabaseType, config map[string]string, logger zerolog.Logger) (*EvmiDatabase, error) {

	var db *gorm.DB
	var err error

	if dbType == SqliteDatabaseType {
		logger.Info().Msg("Initialize SQLite database")
		filename, ok := config["filename"]
		if !ok {
			return nil, errors.New("filename of sqlite database not provided in config")
		}

		db, err = gorm.Open(sqlite.Open(filename), &gorm.Config{Logger: safeLogger(logsafe.Stdout)})
		if err != nil {
			return nil, err
		}
	}

	if dbType == PostgresDatabaseType {
		logger.Log().Msg("Initialize Postgres database")
		dsn, ok := config["dsn"]
		if !ok {
			return nil, errors.New("dsn of sqlite database not provided in config")
		}

		db, err = gorm.Open(postgres.New(postgres.Config{
			DSN: dsn,
		}), &gorm.Config{Logger: safeLogger(logsafe.Stdout)})
		if err != nil {
			return nil, err
		}
	}

	if dbType == MysqlDatabaseType {
		logger.Log().Msg("Initialize MySQL database")
		dsn, ok := config["dsn"]
		if !ok {
			return nil, errors.New("dsn of sqlite database not provided in config")
		}

		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: safeLogger(logsafe.Stdout)})
		if err != nil {
			return nil, err
		}
	}

	if db == nil {
		return nil, errors.New("unknown database type")
	}

	// The metadata pool competes with every store pool for the database's
	// connection cap (managed Postgres tiers can allow as few as 25 total), so
	// it is bounded too. Overridable via the database config.
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(intDBConfig(config, "maxOpenConns", 10))
		sqlDB.SetMaxIdleConns(intDBConfig(config, "maxIdleConns", 2))
		sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	}

	// Migrate the schema
	err = db.AutoMigrate(&EvmiInstance{})
	if err != nil {
		return nil, err
	}

	err = db.AutoMigrate(&EvmBlockchain{})
	if err != nil {
		return nil, err
	}

	err = db.AutoMigrate(&EvmJsonAbi{})
	if err != nil {
		return nil, err
	}

	err = db.AutoMigrate(&EvmLogStore{})
	if err != nil {
		return nil, err
	}

	err = db.AutoMigrate(&EvmLogPipeline{})
	if err != nil {
		return nil, err
	}

	err = db.AutoMigrate(&EvmLogSource{})
	if err != nil {
		return nil, err
	}

	err = db.AutoMigrate(&EvmFactoryRule{}, &EvmFactoryRuleCondition{})
	if err != nil {
		return nil, err
	}

	err = db.AutoMigrate(&EvmiExporter{}, &EvmiExporterSourceCursor{})
	if err != nil {
		return nil, err
	}

	if err := backfillExporterSourceCursors(db); err != nil {
		return nil, err
	}

	err = db.AutoMigrate(&Plugin{})
	if err != nil {
		return nil, err
	}

	err = db.AutoMigrate(&User{}, &AccessToken{}, &OAuthProvider{})
	if err != nil {
		return nil, err
	}

	if err := seedDefaultAdmin(db, logger); err != nil {
		return nil, err
	}

	return &EvmiDatabase{Conn: db}, nil
}

// seedDefaultAdmin creates an initial admin user when no users exist yet. The
// password comes from EVMI_ADMIN_PASSWORD when set; the admin/admin fallback
// is meant for local setups only.
func seedDefaultAdmin(db *gorm.DB, logger zerolog.Logger) error {
	var count int64
	if err := db.Model(&User{}).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	password := os.Getenv("EVMI_ADMIN_PASSWORD")
	fromEnv := password != ""
	if !fromEnv {
		password = "admin"
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	admin := User{
		Username:     "admin",
		PasswordHash: string(hash),
		Role:         string(RoleAdmin),
	}
	if err := db.Create(&admin).Error; err != nil {
		return err
	}

	if fromEnv {
		logger.Info().Msg("created default admin user with the password from EVMI_ADMIN_PASSWORD")
	} else {
		logger.Warn().Msg("created default admin user (admin/admin) — set EVMI_ADMIN_PASSWORD or change the password immediately")
	}
	return nil
}

// backfillExporterSourceCursors migrates exporters that predate per-source export
// cursors. An exporter that has already made progress carries its position in
// EvmiExporter.SyncBlock/SyncLogIndex only; without this, its first run on the new
// schema would find no per-source rows, seed each source from its own StartBlock
// and replay the whole pipeline into the plugin.
//
// It copies that aggregate position onto one cursor row per source of the
// exporter's pipeline, and runs exactly once per exporter: an exporter that
// already has cursor rows is skipped, so a source attached *after* the migration
// is correctly treated as new and exported from its own beginning. Exporters that
// have never progressed need no backfill — seeding them from their sources'
// StartBlock yields the same range, since nothing is stored below it.
func backfillExporterSourceCursors(db *gorm.DB) error {
	var exporters []EvmiExporter
	if err := db.Where("sync_block > 0 OR sync_log_index >= 0").Find(&exporters).Error; err != nil {
		return err
	}

	for _, exp := range exporters {
		var existing int64
		if err := db.Model(&EvmiExporterSourceCursor{}).
			Where("evmi_exporter_id = ?", exp.ID).Count(&existing).Error; err != nil {
			return err
		}
		if existing > 0 {
			continue
		}

		var sources []EvmLogSource
		if err := db.Where("evm_log_pipeline_id = ?", exp.EvmLogPipelineID).Find(&sources).Error; err != nil {
			return err
		}
		if len(sources) == 0 {
			continue
		}

		rows := make([]EvmiExporterSourceCursor, 0, len(sources))
		for _, s := range sources {
			rows = append(rows, EvmiExporterSourceCursor{
				EvmiExporterID: exp.ID,
				EvmLogSourceID: s.ID,
				SyncBlock:      exp.SyncBlock,
				SyncLogIndex:   exp.SyncLogIndex,
			})
		}
		if err := db.Create(&rows).Error; err != nil {
			return err
		}
	}
	return nil
}

// intDBConfig reads an optional integer config key, falling back on a default
// when the key is absent or not a number.
func intDBConfig(config map[string]string, key string, fallback int) int {
	if v, ok := config[key]; ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
