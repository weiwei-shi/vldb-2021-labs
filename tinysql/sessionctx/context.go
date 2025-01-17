// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sessionctx

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// Context is an interface for transaction and executive args environment.
type Context interface {
	// NewTxn creates a new transaction for further execution.
	// If old transaction is valid, it is committed first.
	// It's used in BEGIN statement and DDL statements to commit old transaction.
	NewTxn(context.Context) error

	// Txn returns the current transaction which is created before executing a statement.
	// The returned kv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	// 如果active参数为true，则调用此函数将等待pending的txn变为有效。
	Txn(active bool) (kv.Transaction, error)

	// GetClient gets a kv.Client.
	GetClient() kv.Client

	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value interface{})

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}

	// ClearValue clears the value associated with this context for key.
	ClearValue(key fmt.Stringer)

	GetSessionVars() *variable.SessionVars

	// RefreshTxnCtx commits old transaction without retry,
	// and creates a new transaction.
	// now just for load data and batch insert.
	RefreshTxnCtx(context.Context) error

	// InitTxnWithStartTS initializes a transaction with startTS.
	// It should be called right before we builds an executor.
	InitTxnWithStartTS(startTS uint64) error

	// GetStore returns the store of session.
	GetStore() kv.Storage

	// StmtCommit flush all changes by the statement to the underlying transaction.
	StmtCommit() error
	// StmtRollback provides statement level rollback.
	StmtRollback()
	// StmtAddDirtyTableOP adds the dirty table operation for current statement.
	StmtAddDirtyTableOP(op int, physicalID int64, handle int64)
	// DDLOwnerChecker returns owner.DDLOwnerChecker.
	DDLOwnerChecker() owner.DDLOwnerChecker
	// PrepareTxnFuture uses to prepare txn by future.
	PrepareTxnFuture(ctx context.Context)
}

type basicCtxType int

func (t basicCtxType) String() string {
	switch t {
	case QueryString:
		return "query_string"
	case Initing:
		return "initing"
	case LastExecuteDDL:
		return "last_execute_ddl"
	}
	return "unknown"
}

// Context keys.
const (
	// QueryString is the key for original query string.
	QueryString basicCtxType = 1
	// Initing is the key for indicating if the server is running bootstrap or upgrade job.
	Initing basicCtxType = 2
	// LastExecuteDDL is the key for whether the session execute a ddl command last time.
	LastExecuteDDL basicCtxType = 3
)

type connIDCtxKeyType struct{}

// ConnID is the key in context.
var ConnID = connIDCtxKeyType{}

// SetCommitCtx sets the variables for context before commit a transaction.
func SetCommitCtx(ctx context.Context, sessCtx Context) context.Context {
	return context.WithValue(ctx, ConnID, sessCtx.GetSessionVars().ConnectionID)
}
