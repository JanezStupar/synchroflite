// Copyright 2023 Janez Stupar
// SPDX-License-Identifier: Apache-2.0
import 'package:sqflite_common/sqlite_api.dart';
import 'package:synchroflite/src/sqlite_api.dart';

class SqfliteApi extends SqliteApi {
  final DatabaseExecutor _db;

  SqfliteApi(this._db) : super(_db);

  Batch batch() => _db.batch();

  Future<List<Map<String, Object?>>> rawQuery(String sql,
      [List<Object?>? arguments]) {
    return _db.rawQuery(sql, arguments);
  }

  Future<int> rawUpdate(String sql, [List<Object?>? arguments]) {
    return _db.rawUpdate(sql, arguments);
  }

  Future<int> rawInsert(String sql, [List<Object?>? arguments]) {
    return _db.rawInsert(sql, arguments);
  }

  Future<int> rawDelete(String sql, [List<Object?>? arguments]) {
    return _db.rawDelete(sql, arguments);
  }

  Future<void> close() {
    return (_db as Database).close();
  }

  @override
  Future<void> transaction(Future<void> Function(SqfliteApi txn) action) async {
    assert(_db is Database, 'Cannot start a transaction within a transaction');
    return (_db as Database).transaction((t) => action(SqfliteApi(t)));
  }
}
