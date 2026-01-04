//go:build integration

package gollback

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		panic(err)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(err)
	}

	testPool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		panic(err)
	}

	// table for tests
	_, err = testPool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_txn (
			id SERIAL PRIMARY KEY,
			value TEXT
		)
	`)
	if err != nil {
		panic(err)
	}

	code := m.Run()

	testPool.Close()
	_ = container.Terminate(ctx)

	os.Exit(code)
}

func truncateTable(t *testing.T) {
	_, err := testPool.Exec(context.Background(), "TRUNCATE test_txn")
	if err != nil {
		t.Fatalf("failed to truncate table: %v", err)
	}
}

func getRowCount(t *testing.T) int {
	var count int
	err := testPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM test_txn").Scan(&count)
	if err != nil {
		t.Fatalf("failed to get row count: %v", err)
	}
	return count
}

func TestRunInTxn_Commit(t *testing.T) {
	truncateTable(t)
	txnManager, getConn := NewTxnProvider(testPool)
	ctx := context.Background()

	err := txnManager.RunInTxn(ctx, func(ctx context.Context) error {
		conn := getConn(ctx)
		_, err := conn.Exec(ctx, "INSERT INTO test_txn (value) VALUES ($1)", "test1")
		return err
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if count := getRowCount(t); count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

func TestRunInTxn_Rollback(t *testing.T) {
	truncateTable(t)
	txnManager, getConn := NewTxnProvider(testPool)
	ctx := context.Background()

	err := txnManager.RunInTxn(ctx, func(ctx context.Context) error {
		conn := getConn(ctx)
		_, err := conn.Exec(ctx, "INSERT INTO test_txn (value) VALUES ($1)", "should-rollback")
		if err != nil {
			return err
		}
		return errors.New("intentional error")
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if count := getRowCount(t); count != 0 {
		t.Errorf("expected 0 rows (rolled back), got %d", count)
	}
}

func TestRunInTxn_NestedReuse(t *testing.T) {
	truncateTable(t)
	txnManager, getConn := NewTxnProvider(testPool)
	ctx := context.Background()

	err := txnManager.RunInTxn(ctx, func(ctx context.Context) error {
		conn := getConn(ctx)
		_, err := conn.Exec(ctx, "INSERT INTO test_txn (value) VALUES ($1)", "outer")
		if err != nil {
			return err
		}

		// Nested call - should reuse transaction
		return txnManager.RunInTxn(ctx, func(ctx context.Context) error {
			conn := getConn(ctx)
			_, err := conn.Exec(ctx, "INSERT INTO test_txn (value) VALUES ($1)", "inner")
			return err
		})
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if count := getRowCount(t); count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestRunInTxn_Timeout_ImmediateRollback(t *testing.T) {
	truncateTable(t)
	txnManager, getConn := NewTxnProvider(testPool)
	ctx := context.Background()

	start := time.Now()
	err := txnManager.RunInTxn(ctx, func(ctx context.Context) error {
		conn := getConn(ctx)
		_, err := conn.Exec(ctx, "INSERT INTO test_txn (value) VALUES ($1)", "timeout-test")
		if err != nil {
			return err
		}
		time.Sleep(5 * time.Second) // exceed timeout (100ms), but we should return immediately
		return nil
	}, WithTimeout(100*time.Millisecond))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("expected immediate return on timeout, but took %v", elapsed)
	}
	time.Sleep(50 * time.Millisecond)
	if count := getRowCount(t); count != 0 {
		t.Errorf("expected 0 rows (rolled back), got %d", count)
	}
}

func TestRunInTxn_Timeout_DuringDbOperation(t *testing.T) {
	truncateTable(t)
	txnManager, getConn := NewTxnProvider(testPool)
	ctx := context.Background()

	err := txnManager.RunInTxn(ctx, func(ctx context.Context) error {
		conn := getConn(ctx)
		// pg_sleep(1) takes 1 second, but timeout is 100ms
		// This will cause the Exec to fail with context.DeadlineExceeded
		_, err := conn.Exec(ctx, "SELECT pg_sleep(1)")
		return err
	}, WithTimeout(100*time.Millisecond))

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestRunInTxn_ReadOnly_Read(t *testing.T) {
	truncateTable(t)
	// Insert a row first
	_, _ = testPool.Exec(context.Background(), "INSERT INTO test_txn (value) VALUES ($1)", "existing")

	txnManager, getConn := NewTxnProvider(testPool)
	ctx := context.Background()

	var count int
	err := txnManager.RunInTxn(ctx, func(ctx context.Context) error {
		conn := getConn(ctx)
		return conn.QueryRow(ctx, "SELECT COUNT(*) FROM test_txn").Scan(&count)
	}, ReadOnly())

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if count != 1 {
		t.Errorf("expected count=1, got %d", count)
	}
}

func TestRunInTxn_ReadOnly_WriteFails(t *testing.T) {
	truncateTable(t)
	txnManager, getConn := NewTxnProvider(testPool)
	ctx := context.Background()

	err := txnManager.RunInTxn(ctx, func(ctx context.Context) error {
		conn := getConn(ctx)
		_, err := conn.Exec(ctx, "INSERT INTO test_txn (value) VALUES ($1)", "should-fail")
		return err
	}, ReadOnly())

	if err == nil {
		t.Fatal("expected error for write in read-only transaction")
	}
}

func TestConnGetter_ReturnsTxn_InsideRunInTxn(t *testing.T) {
	txnManager, getConn := NewTxnProvider(testPool)
	ctx := context.Background()

	var connInsideTxn Conn
	var connOutsideTxn Conn

	connOutsideTxn = getConn(ctx)

	_ = txnManager.RunInTxn(ctx, func(ctx context.Context) error {
		connInsideTxn = getConn(ctx)
		return nil
	})

	if connInsideTxn == connOutsideTxn {
		t.Error("expected different Conn inside transaction vs outside")
	}
	if connOutsideTxn != testPool {
		t.Error("expected connOutsideTxn to be the pool")
	}
}

func TestTxnBeginError_PoolClosed(t *testing.T) {
	ctx := context.Background()

	// creating a separate pool
	pool, err := pgxpool.New(ctx, testPool.Config().ConnString())
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	txnManager, _ := NewTxnProvider(pool)
	pool.Close() // close it

	err = txnManager.RunInTxn(ctx, func(ctx context.Context) error {
		return nil
	})

	var beginErr TxnBeginError
	if !errors.As(err, &beginErr) {
		t.Errorf("expected TxnBeginError, got %T: %v", err, err)
	}
}
