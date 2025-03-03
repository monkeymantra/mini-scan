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
	// Insert a new row into version_counter if one with the given timestamp doesn't exist.
	cqlInsertVersionQuery = `INSERT INTO version_counter (ip, port, service, version, timestamp) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`

	// Select the version for a given (ip, port, service, timestamp) to check for duplicates.
	cqlSelectVersionQuery = `SELECT version FROM version_counter WHERE ip = ? AND port = ? AND service = ? AND timestamp = ? ALLOW FILTERING`

	// Select the highest version for a given (ip, port, service) (ordered by version descending).
	cqlSelectLatestVersionQuery = `SELECT version FROM version_counter WHERE ip = ? AND port = ? AND service = ? ORDER BY version DESC LIMIT 1 ALLOW FILTERING`

	// Insert a new scan record into scans if one with the same (ip, port, service, version) doesn't exist.
	cqlInsertScanQuery = `INSERT INTO scans (ip, port, service, timestamp, version, data) VALUES (?, ?, ?, ?, ?, ?)`

	// Retrieve the latest scan record.
	cqlSelectLatestQuery = `SELECT ip, port, service, timestamp, version, data FROM scans WHERE ip = ? AND port = ? AND service = ? ORDER BY version DESC LIMIT 1 ALLOW FILTERING`

	// Retrieve a specific scan by version.
	cqlSelectScanQuery = `SELECT ip, port, service, timestamp, version, data FROM scans WHERE ip = ? AND port = ? AND service = ? AND version = ? ALLOW FILTERING`
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

	GetLatestScan(ctx context.Context, ip string, port uint32, service string) (*types.ScanResponse, error)
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

// CheckForDuplicateTimestamp checks whether there is already a version_counter row for the given (ip, port, service, ts).
func (repo *ScanRepository) CheckForDuplicateTimestamp(ctx context.Context, ip string, port uint32, service string, ts int64) (bool, error) {
	op := func() (bool, error) {
		var version int
		err := repo.session.Query(cqlSelectVersionQuery, ip, int(port), service, ts).
			WithContext(ctx).Consistency(gocql.Quorum).Scan(&version)
		if err == gocql.ErrNotFound {
			return false, nil // No duplicate.
		} else if err != nil {
			return false, fmt.Errorf("error checking for duplicate timestamp: %v", err)
		}
		return true, nil // Duplicate exists.
	}
	return RunWithBackoff(ctx, op, repo.timeout)
}

// GetCurrentVersion retrieves the highest version for (ip, port, service) by ordering on version descending.
func (repo *ScanRepository) GetCurrentVersion(ctx context.Context, ip string, port uint32, service string) (int, error) {
	op := func() (int, error) {
		var version int
		err := repo.session.Query(cqlSelectLatestVersionQuery, ip, int(port), service).
			WithContext(ctx).Consistency(gocql.Quorum).Scan(&version)
		if err == gocql.ErrNotFound {
			return 0, nil // No version exists yet.
		} else if err != nil {
			return 0, fmt.Errorf("error selecting current version: %v", err)
		}
		return version, nil
	}
	return RunWithBackoff(ctx, op, repo.timeout)
}

// EnsureVersionCounter ensures that there is no existing version_counter row for the given (ip, port, service, ts).
// It uses an INSERT IF NOT EXISTS query to create a new row with the provided timestamp and version.
// If a row with the same timestamp exists, it returns an error.
func (repo *ScanRepository) EnsureVersionCounter(ctx context.Context, ip string, port uint32, service string, ts int64, version int) error {
	op := func() (bool, error) {
		m := make(map[string]interface{})
		// Use INSERT ... IF NOT EXISTS with the provided version.
		applied, err := repo.session.Query(cqlInsertVersionQuery, ip, int(port), service, version, ts).
			WithContext(ctx).Consistency(gocql.Quorum).MapScanCAS(m)
		if err != nil {
			return false, fmt.Errorf("version_counter insert error: %v", err)
		}
		if !applied {
			return false, fmt.Errorf("version_counter row already exists for key (%s, %d, %s) at timestamp %d; existing data: %v", ip, port, service, ts, m)
		}
		return true, nil
	}
	_, err := RunWithBackoff(ctx, op, repo.timeout)
	return err
}

// UpdateVersionAndInsertScan retrieves the current highest version, then uses that to compute a new version.
// It then ensures no duplicate timestamp exists, inserts a new version_counter row with the new version and provided timestamp,
// and finally inserts a new scan record into the scans table.
func (repo *ScanRepository) UpdateVersionAndInsertScan(ctx context.Context, ip string, port uint32, service string, ts int64, data string) (int, error) {
	op := func() (int, error) {
		// Step 1: Get current highest version.
		currentVersion, err := repo.GetCurrentVersion(ctx, ip, port, service)
		if err != nil {
			return 0, fmt.Errorf("error retrieving current version: %v", err)
		}
		newVersion := currentVersion + 1

		// Step 2: Ensure that no row exists with the same timestamp.
		dup, err := repo.CheckForDuplicateTimestamp(ctx, ip, port, service, ts)
		if err != nil {
			return 0, err
		}
		if dup {
			return 0, fmt.Errorf("a version_counter entry for key (%s, %d, %s) at timestamp %d already exists", ip, port, service, ts)
		}

		// Step 3: Insert the new version_counter row for this timestamp.
		if err := repo.EnsureVersionCounter(ctx, ip, port, service, ts, newVersion); err != nil {
			return 0, err
		}

		// Step 4: Insert the scan record into scans.
		if err := repo.session.Query(cqlInsertScanQuery, ip, int(port), service, ts, newVersion, data).
			WithContext(ctx).Consistency(gocql.Quorum).Exec(); err != nil {
			return 0, fmt.Errorf("error inserting scan record: %v", err)
		}
		log.Printf("Inserted scan for key (%s, %d, %s) with timestamp %d and version %d", ip, port, service, ts, newVersion)
		return newVersion, nil
	}
	return RunWithBackoff(ctx, op, repo.timeout)
}

// InsertScan is the high-level method that inserts a new scan record if no duplicate exists for the given (ip, port, service, ts).
// It computes the new version (current highest + 1) and then calls UpdateVersionAndInsertScan.
func (repo *ScanRepository) InsertScan(ctx context.Context, ip string, port uint32, service string, ts int64, data string) (int, error) {
	// Check for duplicate timestamp in version_counter.
	var dummy int
	err := repo.session.Query(cqlSelectVersionQuery, ip, int(port), service, ts).
		WithContext(ctx).Consistency(gocql.Quorum).Scan(&dummy)
	if err != nil && err != gocql.ErrNotFound {
		return 0, fmt.Errorf("error checking for existing version row: %v", err)
	}
	if err == nil {
		return 0, fmt.Errorf("an entry for key (%s, %d, %s) at timestamp %d already exists", ip, port, service, ts)
	}

	// Otherwise, update version and insert the scan record.
	return repo.UpdateVersionAndInsertScan(ctx, ip, port, service, ts, data)
}

// GetLatestScan retrieves the latest scan record for the given (ip, port, service).
func (repo *ScanRepository) GetLatestScan(ctx context.Context, ip string, port uint32, service string) (*types.ScanResponse, error) {
	op := func() (interface{}, error) {
		var scan types.ScanResponse
		err := repo.session.Query(cqlSelectLatestQuery, ip, int(port), service).
			WithContext(ctx).Consistency(gocql.Quorum).Scan(
			&scan.Ip, &scan.Port, &scan.Service, &scan.Timestamp, &scan.Version, &scan.Data)
		if err != nil {
			return nil, fmt.Errorf("error selecting latest scan: %v", err)
		}
		return &scan, nil
	}
	result, err := RunWithBackoff(ctx, op, repo.timeout)
	if err != nil {
		return nil, err
	}
	return result.(*types.ScanResponse), nil
}
