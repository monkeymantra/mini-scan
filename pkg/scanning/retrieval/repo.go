package retrieval

import (
	"context"
	"fmt"
	"github.com/censys/scan-takehome/pkg/scanning/types"
	"log"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gocql/gocql"
	"github.com/sony/gobreaker"
)

const (
	cqlUpdateCounterQuery = `UPDATE version_counter SET version = version + 1 WHERE ip = ? AND port = ? AND service = ? AND timestamp = ?`
	cqlSelectVersionQuery = `SELECT version FROM version_counter WHERE ip = ? AND port = ? AND service = ? AND timestamp = ?`
	cqlInsertScanQuery    = `INSERT INTO scans (ip, port, service, timestamp, version, data) VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	cqlSelectLatestQuery  = `SELECT ip, port, service, timestamp, version, data FROM scans WHERE ip = ? AND port = ? AND service = ? ORDER BY version DESC LIMIT 1`
	cqlSelectQuery        = `SELECT ip, port, service, timestamp, version, data FROM scans WHERE ip = ? AND port = ? AND service = ? AND version = ?`
)

// RunWithBackoff runs the provided operation (which returns a value of type T and an error)
// using an exponential backoff that is cancellable via the provided context.
// The timeout parameter sets the maximum elapsed time for retries.
func RunWithBackoff[T any](ctx context.Context, op func() (T, error), timeout time.Duration) (T, error) {
	var zero T
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = timeout
	cancellableBo := backoff.WithContext(bo, ctx)
	// Use RetryWithData to retry the operation.
	result, err := backoff.RetryWithData(func() (interface{}, error) {
		return op()
	}, cancellableBo)
	if err != nil {
		return zero, fmt.Errorf("operation failed after retries: %w", err)
	}
	// The result is returned as interface{}, so assert it to T.
	return result.(T), nil
}

// ScanRepositoryInterface defines the methods for our repository,
// broken up into small, testable pieces.
type ScanRepositoryInterface interface {
	// EnsureVersionCounter creates a new version_counter row for the given key and timestamp
	// using an INSERT IF NOT EXISTS, and returns an error if the row already exists.
	EnsureVersionCounter(ctx context.Context, ip string, port uint32, service string, ts int64) error

	// UpdateVersionAndInsertScan increments the version for the given key and timestamp,
	// retrieves the new version number, and inserts the scan record, returning the latest version number.
	UpdateVersionAndInsertScan(ctx context.Context, ip string, port uint32, service string, ts int64, data string) (int, error)

	// InsertScan is the high-level function that ensures the version_counter row is created,
	// updates it, and then inserts the scan record.
	InsertScan(ctx context.Context, ip string, port uint32, service string, ts int64, data string) (int, error)

	GetLatestScan(ctx context.Context, ip string, port uint32, service string) (*types.Scan, error)
}

// ScanRepository is our concrete implementation that satisfies ScanRepositoryInterface.
type ScanRepository struct {
	session *gocql.Session
	breaker *gobreaker.CircuitBreaker
	timeout time.Duration
	ScanRepositoryInterface
}

// NewScanRepository creates a new ScanRepository.
func NewScanRepository(session *gocql.Session, timeout time.Duration) *ScanRepository {
	cbSettings := gobreaker.Settings{
		Name:        "CassandraCB",
		MaxRequests: 5,
		Interval:    10 * time.Second,
		Timeout:     5 * time.Second,
	}
	return &ScanRepository{
		session: session,
		breaker: gobreaker.NewCircuitBreaker(cbSettings),
		timeout: timeout,
	}
}

// CheckNoExistingVersion checks whether there is no existing version_counter row
// for the given key (ip, port, service, ts). It returns true if no row exists, and false if one exists.
func (repo *ScanRepository) CheckExistingVersion(ctx context.Context, ip string, port uint32, service string, ts int64) (bool, error) {
	log.Printf("Checking existing version for service %s and ip %s and port %d and timestamp %d", service, ip, port, ts)
	op := func() (bool, error) {
		var version int
		err := repo.session.Query(cqlSelectVersionQuery, ip, int(port), service, ts).WithContext(ctx).Scan(&version)
		if err == gocql.ErrNotFound {
			log.Printf("Version checked: %d", version)
			// No row exists
			return false, nil
		} else if err != nil {
			return false, fmt.Errorf("error checking version_counter: %v", err)
		}
		log.Printf("Version already exists")
		// Row exists
		return true, nil
	}
	return RunWithBackoff[bool](ctx, op, repo.timeout)
}

// UpdateVersionAndInsertScan updates the version_counter row (increments version) and inserts the scan record.
// Returns the new version number.
func (repo *ScanRepository) UpdateVersionAndInsertScan(ctx context.Context, ip string, port uint32, service string, ts int64, data string) (int, error) {
	operation := func() (int, error) {
		// Update the version_counter row.
		if err := repo.session.Query(cqlUpdateCounterQuery, ip, int(port), service, ts).Exec(); err != nil {
			return 0, fmt.Errorf("update version_counter error: %v", err)
		}
		// Retrieve the new version.
		var version int
		if err := repo.session.Query(cqlSelectVersionQuery, ip, int(port), service, ts).Scan(&version); err != nil {
			return 0, fmt.Errorf("select version_counter error: %v", err)
		}
		log.Printf("Version updated: %d", version)
		// Insert the scan record.
		currentTs := time.Now().UnixMilli()
		if err := repo.session.Query(cqlInsertScanQuery, ip, int(port), service, currentTs, version, data).Exec(); err != nil {
			return 0, fmt.Errorf("insert scan error: %v", err)
		} else {
			log.Printf("Inserted scan: %d", version)
		}
		return version, nil
	}
	// Use our generic RunWithBackoff helper with a timeout from repo.timeout (or any duration you prefer).
	return RunWithBackoff(ctx, operation, repo.timeout)
}

// InsertScan is the high-level function that first ensures the version_counter row is created
// and then updates it and inserts the scan record.
// One thing that's very different is that my repo stores the scan at multiple versions.
func (repo *ScanRepository) InsertScan(ctx context.Context, ip string, port uint32, service string, ts int64, data string) (int, error) {
	// In the effort to avoid duplicates, this is where we do a check for existing entry at the given timestamp, and only
	//
	exists, err := repo.CheckExistingVersion(ctx, ip, port, service, ts)
	if err != nil {
		return 0, err
	}
	if exists {
		return 0, fmt.Errorf("an entry for key (%s, %d, %s) at timestamp %d already exists - no need to update", ip, port, service, ts)
	}
	// Otherwise, continue with your update/insertion logic.
	return repo.UpdateVersionAndInsertScan(ctx, ip, port, service, ts, data)
}

// GetLatestScan retrieves the latest scan record for the given key.
func (repo *ScanRepository) GetLatestScan(ctx context.Context, ip string, port uint32, service string) (*types.ScanResponse, error) {
	op := func() (interface{}, error) {
		var scan types.ScanResponse
		err := repo.session.Query(cqlSelectLatestQuery, ip, int(port), service).Scan(
			&scan.Ip, &scan.Port, &scan.Service, &scan.Timestamp, &scan.Version, &scan.Data)
		if err != nil {
			return nil, fmt.Errorf("select latest error: %v", err)
		}
		return &scan, nil
	}
	result, err := RunWithBackoff(ctx, op, repo.timeout)
	if err != nil {
		return nil, err
	}
	return result.(*types.ScanResponse), nil
}

// GetLatestScan retrieves the latest scan record for the given key.
func (repo *ScanRepository) GetScan(ctx context.Context, ip string, port uint32, service string, version int) (*types.ScanResponse, error) {
	log.Printf("Getting scan for service %s and version %d", service, version)
	op := func() (interface{}, error) {
		var scan types.ScanResponse
		err := repo.session.Query(cqlSelectQuery, ip, int(port), service, version).Scan(
			&scan.Ip, &scan.Port, &scan.Service, &scan.Timestamp, &scan.Version, &scan.Data)
		if err != nil {
			return nil, fmt.Errorf("select latest error: %v", err)
		}
		return &scan, nil
	}
	result, err := RunWithBackoff(ctx, op, repo.timeout)
	if err != nil {
		return nil, err
	}
	return result.(*types.ScanResponse), nil
}
