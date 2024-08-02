package sharding

import (
	"context"
	"database/sql"
	"strings"

	"gorm.io/gorm"
)

// ConnPool Implement a ConnPool for replace db.Statement.ConnPool in Gorm
type ConnPool struct {
	// db, This is global db instance
	sharding *Sharding
	gorm.ConnPool
}

func (pool *ConnPool) String() string {
	return "gorm:sharding:conn_pool"
}

func (pool ConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return pool.ConnPool.PrepareContext(ctx, query)
}

func (pool ConnPool) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	ftQuery, stQuery, table, _, needFullScan, err := pool.sharding.resolve(query, args...)
	if err != nil {
		return nil, err
	}

	pool.sharding.querys.Store("last_query", stQuery)

	if needFullScan {
		var results []sql.Result
		for _, q := range strings.Split(stQuery, " UNION ALL ") {
			result, err := pool.ConnPool.ExecContext(ctx, q, args...)
			if err != nil {
				return nil, err
			}
			results = append(results, result)
		}
		// 合并结果
		return mergeResults(results), nil
	}

	if table != "" {
		if r, ok := pool.sharding.configs[table]; ok {
			if r.DoubleWrite {
				pool.ConnPool.ExecContext(ctx, ftQuery, args...)
			}
		}
	}

	return pool.ConnPool.ExecContext(ctx, stQuery, args...)
}

// https://github.com/go-gorm/gorm/blob/v1.21.11/callbacks/query.go#L18
func (pool ConnPool) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	ftQuery, stQuery, table, newArgs, needFullScan, err := pool.sharding.resolve(query, args...)
	if err != nil {
		return nil, err
	}

	pool.sharding.querys.Store("last_query", stQuery)

	if needFullScan {
		return pool.ConnPool.QueryContext(ctx, stQuery, newArgs...)
	}

	if table != "" {
		if r, ok := pool.sharding.configs[table]; ok {
			if r.DoubleWrite {
				pool.ConnPool.ExecContext(ctx, ftQuery, args...)
			}
		}
	}

	return pool.ConnPool.QueryContext(ctx, stQuery, args...)
}

func (pool ConnPool) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	_, query, _, newArgs, needFullScan, _ := pool.sharding.resolve(query, args...)
	pool.sharding.querys.Store("last_query", query)

	if needFullScan {
		return pool.ConnPool.QueryRowContext(ctx, query, newArgs...)
	}

	return pool.ConnPool.QueryRowContext(ctx, query, args...)
}

// mergeResults 合并多个 sql.Result
func mergeResults(results []sql.Result) sql.Result {
	// 实现合并逻辑，例如累加 RowsAffected
	// 这里只是一个简单的示例，可能需要根据实际需求进行调整
	var totalAffected int64
	for _, r := range results {
		if a, err := r.RowsAffected(); err == nil {
			totalAffected += a
		}
	}
	return &mergedResult{rowsAffected: totalAffected}
}

type mergedResult struct {
	rowsAffected int64
}

func (r *mergedResult) LastInsertId() (int64, error) {
	// 对于合并结果，LastInsertId 可能没有意义
	// 返回一个错误或者默认值，具体取决于你的需求
	return 0, sql.ErrNoRows
}

func (r *mergedResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// BeginTx Implement ConnPoolBeginner.BeginTx
func (pool *ConnPool) BeginTx(ctx context.Context, opt *sql.TxOptions) (gorm.ConnPool, error) {
	if basePool, ok := pool.ConnPool.(gorm.ConnPoolBeginner); ok {
		return basePool.BeginTx(ctx, opt)
	}

	return pool, nil
}

// Implement TxCommitter.Commit
func (pool *ConnPool) Commit() error {
	if _, ok := pool.ConnPool.(*sql.Tx); ok {
		return nil
	}

	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Commit()
	}

	return nil
}

// Implement TxCommitter.Rollback
func (pool *ConnPool) Rollback() error {
	if _, ok := pool.ConnPool.(*sql.Tx); ok {
		return nil
	}

	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Rollback()
	}

	return nil
}

func (pool *ConnPool) Ping() error {
	return nil
}
