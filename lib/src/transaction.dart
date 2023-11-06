// Copyright 2023 Janez Stupar
// This code is based on Daniel Cachapa's work in sql_crdt:
// https://github.com/cachapa/sql_crdt
// SPDX-License-Identifier: Apache-2.0
part of 'package:synchroflite/synchroflite.dart';

class TransactionSynchroflite extends TransactionCrdt with SqfliteCrdtImplMixin {
  final SqfliteApi _txn;

  TransactionSynchroflite(this._txn, canonicalTime) : super(_txn, canonicalTime);

  @override
  Future<R> _rawInsert<T, R>(T db, InsertStatement statement,
      [List<Object?>? args, Hlc? hlc]) {
    affectedTables.add(statement.table.tableName);
    hlc ??= canonicalTime;
    return super._rawInsert(db, statement, args, hlc);
  }

  @override
  Future<R> _rawUpdate<T, R>(T db, UpdateStatement statement,
      [List<Object?>? args, Hlc? hlc]) {
    affectedTables.add(statement.table.tableName);
    hlc ??= canonicalTime;
    return super._rawUpdate(db, statement, args, hlc);
  }

  @override
  Future<R> _rawDelete<T, R>(T db, DeleteStatement statement,
      [List<Object?>? args, Hlc? hlc]) {
    affectedTables.add(statement.table.tableName);
    hlc ??= canonicalTime;
    return super._rawDelete(db, statement, args, hlc);
  }

  Future<List<Map<String, Object?>>> rawQuery(String sql,
      [List<Object?>? args]) {
    return _innerRawQuery(_txn, sql, args);
  }

  Future<int> rawUpdate(String sql, [List<Object?>? args]) {
    return _innerRawUpdate(_txn, sql, args, canonicalTime);
  }

  Future<int> rawInsert(String sql, [List<Object?>? args]) {
    return _innerRawInsert(_txn, sql, args, canonicalTime);
  }

  Future<int> rawDelete(String sql, [List<Object?>? args]) {
    return _innerRawDelete(_txn, sql, args, canonicalTime);
  }

  Batch batch() => BatchSynchroflite(_txn.batch(), canonicalTime, (tables, hlc) async {
        affectedTables.addAll(tables);
      }, inTransaction: true);
}
