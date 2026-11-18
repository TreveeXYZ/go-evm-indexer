package evmi_database

import (
	"io"
	"log"
	"time"

	gormlogger "gorm.io/gorm/logger"
)

// safeLogger is gorm's default logger — slow statements (200 ms and more)
// and failed ones, with their values — writing to w instead of straight to
// standard output. Given logsafe.Stdout, a statement that carries a node URL
// (a blockchain row being created or changed) is logged without its path.
func safeLogger(w io.Writer) gormlogger.Interface {
	return gormlogger.New(log.New(w, "\r\n", log.LstdFlags), gormlogger.Config{
		SlowThreshold: 200 * time.Millisecond,
		LogLevel:      gormlogger.Warn,
		Colorful:      true,
	})
}
