// Copyright 2023 Janez Stupar
// This code is based on Daniel Cachapa's work in sql_crdt:
// https://github.com/cachapa/sql_crdt
// SPDX-License-Identifier: Apache-2.0
part of 'package:synchroflite/synchroflite.dart';

class TransactionSqfliteCrdt extends TransactionCrdt with SqfliteCrdtImplMixin {
  final SqfliteApi _txn;

  TransactionSqfliteCrdt(this._txn, canonicalTime) : super(_txn, canonicalTime);

  @override
  Future<int> _rawInsert(SqfliteApi db, InsertStatement statement,
      [List<Object?>? args, Hlc? hlc]) {
    hlc ??= canonicalTime.increment();
    affectedTables.add(statement.table.tableName);
    return super._rawInsert(db, statement, args, hlc);
  }

  @override
  Future<int> _rawUpdate(SqfliteApi db, UpdateStatement statement,
      [List<Object?>? args, Hlc? hlc]) {
    hlc ??= canonicalTime.increment();
    affectedTables.add(statement.table.tableName);
    return super._rawUpdate(db, statement, args, hlc);
  }

  @override
  Future<int> _rawDelete(SqfliteApi db, DeleteStatement statement,
      [List<Object?>? args, Hlc? hlc]) {
    hlc ??= canonicalTime.increment();
    affectedTables.add(statement.table.tableName);
    return super._rawDelete(db, statement, args, hlc);
  }

  Future<List<Map<String, Object?>>> rawQuery(String sql,
      [List<Object?>? args]) {
    return _innerRawQuery(_txn, sql, args);
  }

  Future<int> rawUpdate(String sql, [List<Object?>? args]) {
    return _innerRawUpdate(_txn, sql, args);
  }

  Future<int> rawInsert(String sql, [List<Object?>? args]) {
    return _innerRawInsert(_txn, sql, args);
  }

  Future<int> rawDelete(String sql, [List<Object?>? args]) {
    return _innerRawDelete(_txn, sql, args);
  }

  batch() => super._innerBatch(_txn);
}
