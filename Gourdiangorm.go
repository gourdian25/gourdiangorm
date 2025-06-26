// File: Gourdiangorm.go

package gourdiangorm

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gourdian25/gourdianlogger"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Default configuration values
const (
	DefaultMaxRetries        = 3
	DefaultMaxOpenConns      = 100
	DefaultMaxIdleConns      = 10
	DefaultRetryDelay        = 5 * time.Second
	DefaultOperationTimeout  = 30 * time.Second
	DefaultConnectTimeout    = 10 * time.Second
	DefaultStatementTimeout  = 30 * time.Second
	DefaultDisconnectTimeout = 10 * time.Second
	DefaultConnMaxLifetime   = 5 * time.Minute
	DefaultConnMaxIdleTime   = 5 * time.Minute
	DefaultHealthCheckPeriod = 1 * time.Minute
)

type GourdianGormPostgresConfig struct {
	Host              string
	Port              int
	User              string
	Password          string
	Database          string
	ApplicationName   string
	SSLMode           string
	SSLCert           string
	SSLKey            string
	SSLRootCert       string
	DefaultIsolation  string
	ReadReplicaDSNs   []string
	MaxRetries        int
	MaxOpenConns      int
	MaxIdleConns      int
	ConnMaxLifetime   time.Duration
	ConnMaxIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	StatementTimeout  time.Duration
	OperationTimeout  time.Duration
	DisconnectTimeout time.Duration
	RetryDelay        time.Duration
	TLSConfig         *tls.Config
}

func NewGourdianGormPostgresConfig(
	tlsConfig *tls.Config,
	readReplicaDSNs []string,
	port, maxRetries, maxOpenConns, maxIdleConns int,
	host, user, password, database, sslMode, sslCert, sslKey, sslRootCert, defaultIsolation string,
	connMaxLifetime, connMaxIdleTime, healthCheckPeriod, connectTimeout, readTimeout, writeTimeout, statementTimeout, operationTimeout, disconnectTimeout, retryDelay time.Duration,
) *GourdianGormPostgresConfig {
	return &GourdianGormPostgresConfig{
		Host:              host,
		Port:              port,
		User:              user,
		Password:          password,
		Database:          database,
		SSLMode:           sslMode,
		SSLCert:           sslCert,
		SSLKey:            sslKey,
		SSLRootCert:       sslRootCert,
		DefaultIsolation:  defaultIsolation,
		ReadReplicaDSNs:   readReplicaDSNs,
		MaxRetries:        maxRetries,
		MaxOpenConns:      maxOpenConns,
		MaxIdleConns:      maxIdleConns,
		ConnMaxLifetime:   connMaxLifetime,
		ConnMaxIdleTime:   connMaxIdleTime,
		HealthCheckPeriod: healthCheckPeriod,
		ConnectTimeout:    connectTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		StatementTimeout:  statementTimeout,
		OperationTimeout:  operationTimeout,
		DisconnectTimeout: disconnectTimeout,
		RetryDelay:        retryDelay,
		TLSConfig:         tlsConfig,
	}
}

func NewDefaultGourdianGormPostgresConfig(host string, port int, user, password, database string) *GourdianGormPostgresConfig {
	return &GourdianGormPostgresConfig{
		Host:              host,
		Port:              port,
		User:              user,
		Password:          password,
		Database:          database,
		MaxRetries:        DefaultMaxRetries,
		MaxOpenConns:      DefaultMaxOpenConns,
		MaxIdleConns:      DefaultMaxIdleConns,
		ConnMaxLifetime:   DefaultConnMaxLifetime,
		ConnMaxIdleTime:   DefaultConnMaxIdleTime,
		HealthCheckPeriod: DefaultHealthCheckPeriod,
		ConnectTimeout:    DefaultConnectTimeout,
		StatementTimeout:  DefaultStatementTimeout,
		OperationTimeout:  DefaultOperationTimeout,
		DisconnectTimeout: DefaultDisconnectTimeout,
		RetryDelay:        DefaultRetryDelay,
	}
}

func buildDSN(config *GourdianGormPostgresConfig) string {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		config.Host, config.Port, config.User, config.Password, config.Database)

	if config.ApplicationName != "" {
		dsn += " application_name=" + config.ApplicationName
	}
	if config.SSLMode != "" {
		dsn += " sslmode=" + config.SSLMode
	}
	if config.SSLCert != "" {
		dsn += " sslcert=" + config.SSLCert
	}
	if config.SSLKey != "" {
		dsn += " sslkey=" + config.SSLKey
	}
	if config.SSLRootCert != "" {
		dsn += " sslrootcert=" + config.SSLRootCert
	}
	if config.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(config.ConnectTimeout.Seconds()))
	}
	if config.StatementTimeout > 0 {
		dsn += fmt.Sprintf(" statement_timeout=%d", int(config.StatementTimeout.Milliseconds()))
	}

	return dsn
}

func NewGourdianPostgresGormDatabase(
	ctx context.Context,
	config *GourdianGormPostgresConfig,
	log *gourdianlogger.Logger,
) (*gorm.DB, error) {
	if config == nil {
		log.Error("Received nil PostgreSQL configuration")
		return nil, fmt.Errorf("nil PostgreSQL configuration")
	}

	if config.Host == "" || config.Database == "" {
		log.Error("Missing required PostgreSQL connection parameters")
		return nil, fmt.Errorf("missing required PostgreSQL connection parameters")
	}

	dsn := buildDSN(config)
	var db *gorm.DB
	var err error

	for attempt := 1; attempt <= config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			log.Errorf("Context cancelled while connecting to PostgreSQL: %v", ctx.Err())
			return nil, fmt.Errorf("context cancelled while connecting to PostgreSQL: %w", ctx.Err())
		default:
			log.Infof("Attempting PostgreSQL connection (attempt %d/%d)", attempt, config.MaxRetries)

			// Configure GORM
			gormConfig := &gorm.Config{
				Logger: NewGormLogger(log),
				NowFunc: func() time.Time {
					return time.Now().UTC()
				},
			}

			db, err = gorm.Open(postgres.Open(dsn), gormConfig)
			if err != nil {
				log.Warnf("Connection attempt %d failed: %v", attempt, err)
				if attempt < config.MaxRetries {
					log.Infof("Retrying in %v...", config.RetryDelay)
					time.Sleep(config.RetryDelay)
				}
				continue
			}

			// Get underlying sql.DB to configure connection pool
			sqlDB, err := db.DB()
			if err != nil {
				log.Errorf("Failed to get sql.DB from gorm.DB: %v", err)
				return nil, fmt.Errorf("failed to get sql.DB: %w", err)
			}

			// Configure connection pool
			sqlDB.SetMaxOpenConns(config.MaxOpenConns)
			sqlDB.SetMaxIdleConns(config.MaxIdleConns)
			sqlDB.SetConnMaxLifetime(config.ConnMaxLifetime)
			sqlDB.SetConnMaxIdleTime(config.ConnMaxIdleTime)

			// Verify connection with ping
			pingCtx, cancel := context.WithTimeout(ctx, config.OperationTimeout)
			defer cancel()
			if err := sqlDB.PingContext(pingCtx); err != nil {
				log.Warnf("Ping failed after connection (attempt %d): %v", attempt, err)
				_ = sqlDB.Close()
				if attempt < config.MaxRetries {
					log.Infof("Retrying in %v...", config.RetryDelay)
					time.Sleep(config.RetryDelay)
				}
				continue
			}

			// Set default transaction isolation level if specified
			if config.DefaultIsolation != "" {
				if err := setDefaultIsolation(db, config.DefaultIsolation); err != nil {
					log.Warnf("Failed to set default isolation level: %v", err)
				}
			}

			log.Info("Successfully connected to PostgreSQL")
			return db, nil
		}
	}

	log.Errorf("Failed to connect to PostgreSQL after %d attempts: %v", config.MaxRetries, err)
	return nil, fmt.Errorf("failed to connect to PostgreSQL after %d attempts: %w", config.MaxRetries, err)
}

func setDefaultIsolation(db *gorm.DB, level string) error {
	var isolation string
	switch strings.ToLower(level) {
	case "read uncommitted":
		isolation = "READ UNCOMMITTED"
	case "read committed":
		isolation = "READ COMMITTED"
	case "repeatable read":
		isolation = "REPEATABLE READ"
	case "serializable":
		isolation = "SERIALIZABLE"
	default:
		return fmt.Errorf("invalid isolation level: %s", level)
	}

	return db.Exec(fmt.Sprintf("SET default_transaction_isolation = '%s'", isolation)).Error
}

func NewDefaultGourdianPostgresGormDatabase(
	ctx context.Context,
	port int,
	host, user, password, database string,
	log *gourdianlogger.Logger,
) (*gorm.DB, error) {
	config := NewDefaultGourdianGormPostgresConfig(host, port, user, password, database)
	return NewGourdianPostgresGormDatabase(ctx, config, log)
}

func CloseDatabaseConnection(
	ctx context.Context,
	db *gorm.DB,
	disconnectTimeout time.Duration,
	log *gourdianlogger.Logger,
) error {
	if db == nil {
		log.Warn("Attempted to disconnect nil PostgreSQL connection")
		return nil
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Errorf("Failed to get sql.DB from gorm.DB: %v", err)
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}

	disconnectCtx, cancel := context.WithTimeout(ctx, disconnectTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- sqlDB.Close() }()

	select {
	case <-disconnectCtx.Done():
		log.Warn("PostgreSQL close operation cancelled by context")
		return disconnectCtx.Err()
	case err := <-done:
		if err != nil {
			log.Errorf("Error disconnecting from PostgreSQL: %v", err)
			return fmt.Errorf("error disconnecting from PostgreSQL: %w", err)
		}
		log.Info("Disconnected from PostgreSQL successfully")
		return nil
	}
}

func WithTransaction(
	ctx context.Context,
	db *gorm.DB,
	config *GourdianGormPostgresConfig,
	log *gourdianlogger.Logger,
	operations func(tx *gorm.DB) error,
) error {
	if db == nil {
		log.Error("Received nil database connection")
		return fmt.Errorf("nil database connection")
	}

	if config == nil {
		config = NewDefaultGourdianGormPostgresConfig("", 0, "", "", "")
	}

	var txErr error

	err := withRetry(ctx, config, log, func() error {
		tx := db.Begin(&sql.TxOptions{
			Isolation: getIsolationLevel(config.DefaultIsolation),
		})
		if tx.Error != nil {
			return tx.Error
		}

		if err := operations(tx); err != nil {
			if rbErr := tx.Rollback().Error; rbErr != nil {
				log.Errorf("Failed to rollback transaction: %v", rbErr)
			}
			return err
		}

		txErr = tx.Commit().Error
		return txErr
	})

	if err != nil {
		log.Errorf("Transaction failed: %v", err)
		return fmt.Errorf("transaction failed: %w", err)
	}

	return nil
}

func getIsolationLevel(level string) sql.IsolationLevel {
	switch strings.ToLower(level) {
	case "read uncommitted":
		return sql.LevelReadUncommitted
	case "read committed":
		return sql.LevelReadCommitted
	case "repeatable read":
		return sql.LevelRepeatableRead
	case "serializable":
		return sql.LevelSerializable
	default:
		return sql.LevelDefault
	}
}

func withRetry(
	ctx context.Context,
	config *GourdianGormPostgresConfig,
	log *gourdianlogger.Logger,
	op func() error,
) error {
	var lastErr error

	for attempt := 1; attempt <= config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			log.Debugf("Attempting operation (attempt %d/%d)", attempt, config.MaxRetries)
			err := op()
			if err == nil {
				return nil
			}

			lastErr = err
			if isRetryableError(err) {
				if attempt < config.MaxRetries {
					log.Warnf("Operation failed (attempt %d), retrying in %v: %v", attempt, config.RetryDelay, err)
					time.Sleep(config.RetryDelay)
					continue
				}
			}

			// For non-retryable errors or last attempt
			return err
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %w", config.MaxRetries, lastErr)
}

func isRetryableError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}

	return true
}

type GormLogger struct {
	logger *gourdianlogger.Logger
}

func NewGormLogger(log *gourdianlogger.Logger) logger.Interface {
	return &GormLogger{logger: log}
}

// LogMode implements logger.Interface
func (l *GormLogger) LogMode(level logger.LogLevel) logger.Interface {
	return l
}

// Info implements logger.Interface
func (l *GormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	l.logger.Infof(msg, data...)
}

// Warn implements logger.Interface
func (l *GormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	l.logger.Warnf(msg, data...)
}

// Error implements logger.Interface
func (l *GormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	l.logger.Errorf(msg, data...)
}

// Trace implements logger.Interface
func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, rows := fc()
	if err != nil {
		l.logger.Debugf("SQL: %s [%d rows] | Error: %v", sql, rows, err)
	} else {
		l.logger.Debugf("SQL: %s [%d rows] | %v", sql, rows, time.Since(begin))
	}
}

func CheckAndEnableUUIDExtension(ctx context.Context, db *gorm.DB, logger *gourdianlogger.Logger) error {
	sqlDB, err := db.DB()
	if err != nil {
		logger.Errorf("Failed to get database connection from GORM: %v", err)
		return fmt.Errorf("failed to get database connection from GORM: %v", err)
	}

	row := sqlDB.QueryRowContext(ctx, "SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp'")
	var exists int
	err = row.Scan(&exists)

	if err == sql.ErrNoRows {
		_, err = sqlDB.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
		if err != nil {
			logger.Errorf("Failed to create uuid-ossp extension: %v", err)
			return fmt.Errorf("failed to create uuid-ossp extension: %v", err)
		}
		logger.Info("uuid-ossp extension enabled successfully")
	} else if err != nil {
		logger.Errorf("Failed to check for uuid-ossp extension: %v", err)
		return fmt.Errorf("failed to check for uuid-ossp extension: %v", err)
	} else {
		logger.Info("uuid-ossp extension is already enabled")
	}

	return nil
}

func GormMigrator(ctx context.Context, db *gorm.DB, logger *gourdianlogger.Logger, typesToMigrate []interface{}) error {
	if db == nil {
		logger.Error("Received nil database instance for migration")
		return fmt.Errorf("nil database instance")
	}

	for _, modelType := range typesToMigrate {
		select {
		case <-ctx.Done():
			logger.Errorf("Migration cancelled: %v", ctx.Err())
			return ctx.Err()
		default:
			logger.Infof("Attempting to migrate database schema for type [%T]...", modelType)

			err := db.AutoMigrate(modelType)
			if err != nil {
				logger.Errorf("Failed to migrate database schema for type [%T]: %v", modelType, err)
				return fmt.Errorf("failed to migrate database schema for type [%T]: %w", modelType, err)
			}

			logger.Infof("Database schema migrated successfully for type [%T]", modelType)
		}
	}

	logger.Info("All migrations completed successfully")
	return nil
}
