// Copyright 2023 Janez Stupar
// SPDX-License-Identifier: Apache-2.0
part of 'package:synchroflite/synchroflite.dart';

// Queries that don't need to be intercepted and transformed
const specialQueries = <String> {
  'SELECT 1',
};

typedef Executor<T, R> = Future<R> Function(T db, String sql, List<Object?>? arguments);
Object? _convert(Object? value) => (value is Hlc) ? value.toString() : value;

Future<R> performAction<T, R>(T db, String sql, List<Object?>? arguments, Executor<T, R> executor) {
  return executor(db, sql, arguments);
}

Future<int> rawInsert(SqfliteApi db, String sql, List<Object?> arguments) async {
  return await performAction<SqfliteApi, int>(db, sql, arguments, (SqfliteApi db, String sql, List<Object?>? args) {
    return db.rawInsert(sql, args?.map(_convert).toList()); // assuming rawInsert returns Future<int>
  });
}

Future<void> batchRawInsert(Batch batch, String sql, List<Object?> arguments) async {
  await performAction<Batch, void>(batch, sql, arguments, (Batch batch, String sql, List<Object?>? args) {
    batch.rawInsert(sql, args?.map(_convert).toList());
    return Future.value(); // return a void Future
  });
}

Future<int> rawUpdate(SqfliteApi db, String sql, List<Object?> arguments) async {
  return await performAction<SqfliteApi, int>(db, sql, arguments, (SqfliteApi db, String sql, List<Object?>? args) {
    return db.rawUpdate(sql, args?.map(_convert).toList()); // assuming rawUpdate returns Future<int>
  });
}

Future<void> batchRawUpdate(Batch batch, String sql, List<Object?> arguments) async {
  await performAction<Batch, void>(batch, sql, arguments, (Batch batch, String sql, List<Object?>? args) {
    batch.rawUpdate(sql, args?.map(_convert).toList());
    return Future.value(); // return a void Future
  });
}

Future<List<Map<String, Object?>>> rawQuery(SqfliteApi db, String sql, List<Object?>? arguments) async {
  return await performAction<SqfliteApi, List<Map<String, Object?>>>(db, sql, arguments, (SqfliteApi db, String sql, List<Object?>? args) {
    return db.rawQuery(sql, args?.map(_convert).toList()); // assuming rawQuery returns Future<List<Map<String, Object?>>>
  });
}

Future<void> batchRawQuery(Batch batch, String sql, List<Object?>? arguments) async {
  await performAction<Batch, void>(batch, sql, arguments, (Batch batch, String sql, List<Object?>? args) {
    batch.rawQuery(sql, args?.map(_convert).toList());
    return Future.value(); // return a void Future
  });
}

// This mixin is used to override the default implementation of rawQuery, rawUpdate, rawInsert, and rawDelete
mixin class SqfliteCrdtImplMixin {

  Future<R> queryFunc<T, R>(T db, SelectStatement statement,
      [List<Object?>? args]) {
    SelectStatement newStatement = CrdtUtil.prepareSelect(statement, args);
    if (db is SqfliteApi) {
      return rawQuery(db, newStatement.toSql(), args) as Future<R>;
    } else if (db is Batch) {
      return batchRawQuery(db, newStatement.toSql(), args) as Future<R>;
    } else {
      throw 'Unsupported database type: ${db.runtimeType}';
    }
  }

  Future<R> _rawInsert<T, R>(T db, InsertStatement statement, List<Object?>? args, [Hlc? hlc]) async {
    InsertStatement newStatement = CrdtUtil.prepareInsert(statement, args);
    args = [...args ?? [], hlc, hlc?.nodeId, hlc];
    if (db is SqfliteApi) {
      return rawInsert(db, newStatement.toSql(), args) as FutureOr<R>;
    } else if (db is Batch) {
      return batchRawInsert(db, newStatement.toSql(), args) as Future<R>;
    } else {
      throw 'Unsupported database type: ${db.runtimeType}';
    }
  }

  Future<R> _rawUpdate<T,R>(T db, UpdateStatement statement, List<Object?>? args, [Hlc? hlc]) async {
    UpdateStatement newStatement = CrdtUtil.prepareUpdate(statement, args);
    args = [...args ?? [], hlc, hlc?.nodeId, hlc];
    if (db is SqfliteApi) {
      return rawUpdate(db, newStatement.toSql(), args) as FutureOr<R>;
    } else if (db is Batch) {
      return batchRawUpdate(db, newStatement.toSql(), args) as Future<R>;
    } else {
      throw 'Unsupported database type: ${db.runtimeType}';
    }
  }

  Future<R> _rawDelete<T, R>(T db, DeleteStatement statement, List<Object?>? args, [Hlc? hlc]) async {
    UpdateStatement newStatement = CrdtUtil.prepareDelete(statement, args);
    args = [...args ?? [], 1, hlc, hlc?.nodeId, hlc];
    if (db is SqfliteApi) {
      return rawUpdate(db, newStatement.toSql(), args) as FutureOr<R>;
    } else if (db is Batch) {
      return batchRawUpdate(db, newStatement.toSql(), args) as Future<R>;
    } else {
      throw 'Unsupported database type: ${db.runtimeType}';
    }
  }

  Future<R> _innerRawQuery<T, R>(T db, String sql,
      [List<Object?>? arguments]) {

    // There are some queries where it doesn't make sense to add CRDT columns
    final isSpecial = specialQueries.contains(sql.toUpperCase());
    if (isSpecial) {
      if (db is SqfliteApi) {
        return rawQuery(db, sql, arguments) as Future<R>;
      } else if (db is Batch) {
        return batchRawQuery(db, sql, arguments) as Future<R>;
      } else {
        throw 'Unsupported database type: ${db.runtimeType}';
      }
    }

    final result = CrdtUtil.parseSql(sql);
    if (result.rootNode is SelectStatement) {
      return queryFunc(db, result.rootNode as SelectStatement, arguments);
    } else {
      if (db is SqfliteApi) {
        return rawQuery(db, sql, arguments) as Future<R>;
      } else if (db is Batch) {
        return batchRawQuery(db, sql, arguments) as Future<R>;
      } else {
        throw 'Unsupported database type: ${db.runtimeType}';
      }
    }
  }

  Future<R> _innerRawUpdate<T,R>(T db, String sql, List<Object?>? arguments, [Hlc? hlc] ) async {
    final result = CrdtUtil.parseSql(sql);
    if (result.rootNode is UpdateStatement) {
      return _rawUpdate(db, result.rootNode as UpdateStatement, arguments, hlc);
    } else if (result.rootNode is DeleteStatement) {
      return _rawDelete(db, result.rootNode as DeleteStatement, arguments, hlc);
    } else {
      throw 'Unsupported statement: $sql';
    }
  }

  Future<R> _innerRawInsert<T,R>(T db, String sql, List<Object?>? arguments, [Hlc? hlc]) async {
    final result = CrdtUtil.parseSql(sql);
    return  _rawInsert(db, result.rootNode as InsertStatement, arguments, hlc);
  }

  Future<R> _innerRawDelete<T,R>(T db, String sql, List<Object?>? arguments, [Hlc? hlc]) {
    final result = CrdtUtil.parseSql(sql);
    return _rawDelete(db, result.rootNode as DeleteStatement, arguments, hlc);
  }
}

