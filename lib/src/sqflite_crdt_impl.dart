
part of 'package:sqlite_crdt/sqflite_crdt.dart';

// Queries that don't need to be intercepted and transformed
const specialQueries = <String> {
  'SELECT 1',
};

mixin class SqfliteCrdtImplMixin {

  Object? _convert(Object? value) => (value is Hlc) ? value.toString() : value;

  Future<List<Map<String, Object?>>> _baseRawQuery(SqfliteApi db, SelectStatement statement,
      [List<Object?>? args]) {
    return db.rawQuery(statement.toSql(), args);
  }

  Future<int> _baseRawUpdate(SqfliteApi db, UpdateStatement statement, [List<Object?>? args]) async {
    return await db.rawUpdate(statement.toSql(), args?.map(_convert).toList());
  }

  Future<int> _baseRawInsert(SqfliteApi db, InsertStatement statement, [List<Object?>? args]) {
    return db.rawInsert(statement.toSql(), args?.map(_convert).toList());
  }

  Future<List<Map<String, Object?>>> queryFunc(SqfliteApi db, SelectStatement statement,
      [List<Object?>? args]) {
    SelectStatement newStatement = CrdtUtil.prepareSelect(statement, args);
    return _baseRawQuery(db, newStatement, args);
  }

  Future<int> _rawInsert(SqfliteApi db, InsertStatement statement, List<Object?>? args, [Hlc? hlc]) async {
    InsertStatement newStatement = CrdtUtil.prepareInsert(statement, args);
    return _baseRawInsert(db, newStatement, [...args ?? [], hlc, hlc?.nodeId, hlc]);
  }

  Future<int> _rawUpdate(SqfliteApi db, UpdateStatement statement, List<Object?>? args, [Hlc? hlc]) async {
    UpdateStatement newStatement = CrdtUtil.prepareUpdate(statement, args);
    return _baseRawUpdate(db, newStatement, [...args ?? [], hlc, hlc?.nodeId, hlc]);
  }

  Future<int> _rawDelete(SqfliteApi db, DeleteStatement statement, List<Object?>? args, [Hlc? hlc]) async {
    UpdateStatement newStatement = CrdtUtil.prepareDelete(statement, args);
    return _baseRawUpdate(db, newStatement, [...args ?? [], 1, hlc, hlc?.nodeId, hlc]);
  }

  Future<List<Map<String, Object?>>> _innerRawQuery(SqfliteApi db, String sql,
      [List<Object?>? arguments]) {

    // There are some queries where it doesn't make sense to add CRDT columns
    final isSpecial = specialQueries.contains(sql.toUpperCase());
    if (isSpecial) {
      return db.rawQuery(sql, arguments);
    }

    final result = CrdtUtil.parseSql(sql);
    if (result.rootNode is SelectStatement) {
      return queryFunc(db, result.rootNode as SelectStatement, arguments);
    } else {
      return db.rawQuery(sql, arguments);
    }
  }

  Future<int> _innerRawUpdate(SqfliteApi db, String sql, List<Object?>? arguments, [Hlc? hlc] ) async {
    final result = CrdtUtil.parseSql(sql);
    if (result.rootNode is UpdateStatement) {
      return _rawUpdate(db, result.rootNode as UpdateStatement, arguments, hlc);
    } else if (result.rootNode is DeleteStatement) {
      return _rawDelete(db, result.rootNode as DeleteStatement, arguments, hlc);
    } else {
      throw 'Unsupported statement: $sql';
    }
  }

  Future<int> _innerRawInsert(SqfliteApi db, String sql, List<Object?>? arguments, [Hlc? hlc]) async {
    final result = CrdtUtil.parseSql(sql);
    return  _rawInsert(db, result.rootNode as InsertStatement, arguments, hlc);
  }

  Future<int> _innerRawDelete(SqfliteApi db, String sql, List<Object?>? arguments, [Hlc? hlc]) {
    final result = CrdtUtil.parseSql(sql);
    return _rawDelete(db, result.rootNode as DeleteStatement, arguments, hlc);
  }

  Batch _innerBatch(SqfliteApi db) => (db as Database).batch();
}

