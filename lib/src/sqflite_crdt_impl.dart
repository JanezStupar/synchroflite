// Copyright 2023 Janez Stupar
// SPDX-License-Identifier: Apache-2.0
part of 'package:synchroflite/synchroflite.dart';

// Queries that don't need to be intercepted and transformed
const specialQueries = <String>{
  'SELECT 1',
};

// There are some queries where it doesn't make sense to add CRDT columns
bool isSpecialQuery(ParseResult result) {
  // Pragma queries don't need to be intercepted and transformed
  if (result.sql.toUpperCase().startsWith('PRAGMA')) {
    return true;
  }

  //  IF the query is on the lookup table, we don't need to add CRDT columns
  if (specialQueries.contains(result.sql.toUpperCase())) {
    return true;
  }
  ;

  final statement = result.rootNode;
  if (statement is SelectStatement) {
    //     If the query is accessing the schema table, we don't need to add CRDT columns
    if (statement.from != null) {
      if (statement.from is TableReference) {
        final table = statement.from as TableReference;
        if ([
          'sqlite_schema',
          'sqlite_master',
          'sqlite_temp_schema',
          'sqlite_temp_master'
        ].contains(table.tableName)) {
          return true;
        }
      }
    }
  }
  return false;
}

typedef Executor<T, R> = Future<R> Function(
    T db, String sql, List<Object?>? arguments);

Object? _convert(Object? value) => (value is Hlc) ? value.toString() : value;

typedef HlcGenerator = Hlc Function();

Future<R> performAction<T, R>(
    T db, String sql, List<Object?>? arguments, Executor<T, R> executor) {
  return executor(db, sql, arguments);
}

Future<int> rawInsert(
    SqfliteApi db, String sql, List<Object?> arguments) async {
  return await performAction<SqfliteApi, int>(db, sql, arguments,
      (SqfliteApi db, String sql, List<Object?>? args) {
    return db.rawInsert(sql,
        args?.map(_convert).toList()); // assuming rawInsert returns Future<int>
  });
}

Future<void> batchRawInsert(
    Batch batch, String sql, List<Object?> arguments) async {
  await performAction<Batch, void>(batch, sql, arguments,
      (Batch batch, String sql, List<Object?>? args) {
    batch.rawInsert(sql, args?.map(_convert).toList());
    return Future.value(); // return a void Future
  });
}

Future<int> rawUpdate(
    SqfliteApi db, String sql, List<Object?> arguments) async {
  return await performAction<SqfliteApi, int>(db, sql, arguments,
      (SqfliteApi db, String sql, List<Object?>? args) {
    return db.rawUpdate(sql,
        args?.map(_convert).toList()); // assuming rawUpdate returns Future<int>
  });
}

Future<void> batchRawUpdate(
    Batch batch, String sql, List<Object?> arguments) async {
  await performAction<Batch, void>(batch, sql, arguments,
      (Batch batch, String sql, List<Object?>? args) {
    batch.rawUpdate(sql, args?.map(_convert).toList());
    return Future.value(); // return a void Future
  });
}

Future<List<Map<String, Object?>>> rawQuery(
    SqfliteApi db, String sql, List<Object?>? arguments) async {
  return await performAction<SqfliteApi, List<Map<String, Object?>>>(
      db, sql, arguments, (SqfliteApi db, String sql, List<Object?>? args) {
    return db.rawQuery(
        sql,
        args
            ?.map(_convert)
            .toList()); // assuming rawQuery returns Future<List<Map<String, Object?>>>
  });
}

Future<void> batchRawQuery(
    Batch batch, String sql, List<Object?>? arguments) async {
  await performAction<Batch, void>(batch, sql, arguments,
      (Batch batch, String sql, List<Object?>? args) {
    batch.rawQuery(sql, args?.map(_convert).toList());
    return Future.value(); // return a void Future
  });
}

// This mixin is used to override the default implementation of rawQuery, rawUpdate, rawInsert, and rawDelete
mixin class SqfliteCrdtImplMixin {
  Future<R> queryFunc<T, R>(T db, SelectStatement statement,
      [List<Object?>? args]) {
    var newStatement = CrdtUtil.prepareSelect(statement, args);
    if (db is SqfliteApi) {
      return rawQuery(db, newStatement.toSql(), args) as Future<R>;
    } else if (db is Batch) {
      return batchRawQuery(db, newStatement.toSql(), args) as Future<R>;
    } else {
      throw 'Unsupported database type: ${db.runtimeType}';
    }
  }

  Future<R> _rawInsert<T, R>(
      T db, InsertStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    var (newStatement, newArgs) = CrdtUtil.prepareInsert(statement, args, hlc!);
    if (db is SqfliteApi) {
      return rawInsert(db, newStatement.toSql(), newArgs!) as Future<R>;
    } else if (db is Batch) {
      return batchRawInsert(db, newStatement.toSql(), newArgs!) as Future<R>;
    } else {
      throw 'Unsupported database type: ${db.runtimeType}';
    }
  }

  Future<R> _rawUpdate<T, R>(
      T db, UpdateStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    var (newStatement, newArgs) = CrdtUtil.prepareUpdate(statement, args, hlc!);
    if (db is SqfliteApi) {
      return rawUpdate(db, newStatement.toSql(), newArgs!) as Future<R>;
    } else if (db is Batch) {
      return batchRawUpdate(db, newStatement.toSql(), newArgs!) as Future<R>;
    } else {
      throw 'Unsupported database type: ${db.runtimeType}';
    }
  }

  Future<R> _rawDelete<T, R>(
      T db, DeleteStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    var (newStatement, newArgs) = CrdtUtil.prepareDelete(statement, args, hlc!);
    if (db is SqfliteApi) {
      return rawUpdate(db, newStatement.toSql(), newArgs!) as Future<R>;
    } else if (db is Batch) {
      return batchRawUpdate(db, newStatement.toSql(), newArgs!) as Future<R>;
    } else {
      throw 'Unsupported database type: ${db.runtimeType}';
    }
  }

  Future<R> _innerRawQuery<T, R>(T db, String sql, [List<Object?>? arguments]) {
    // There are some queries where it doesn't make sense to add CRDT columns
    final result = CrdtUtil.parseSql(sql);
    final isSpecial = isSpecialQuery(result);

    if (result.rootNode is SelectStatement && !isSpecial) {
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

  Future<R> _innerRawUpdate<T, R>(T db, String sql, List<Object?>? arguments,
      [Hlc? hlc]) async {
    final result = CrdtUtil.parseSql(sql);
    if (result.rootNode is UpdateStatement) {
      return _rawUpdate(db, result.rootNode as UpdateStatement, arguments, hlc);
    } else if (result.rootNode is DeleteStatement) {
      return _rawDelete(db, result.rootNode as DeleteStatement, arguments, hlc);
    } else {
      throw 'Unsupported statement: $sql';
    }
  }

  Future<R> _innerRawInsert<T, R>(T db, String sql, List<Object?>? arguments,
      [Hlc? hlc]) async {
    final result = CrdtUtil.parseSql(sql);
    return _rawInsert(db, result.rootNode as InsertStatement, arguments, hlc);
  }

  Future<R> _innerRawDelete<T, R>(T db, String sql, List<Object?>? arguments,
      [Hlc? hlc]) {
    final result = CrdtUtil.parseSql(sql);
    return _rawDelete(db, result.rootNode as DeleteStatement, arguments, hlc);
  }

  Future<void> _baseExecute<T>(T db, String sql, List<Object?>? args) async {
    // Run the query unchanged
    if (db is SqfliteApi) {
      await db.execute(sql, args?.map(_convert).toList());
    } else if (db is Batch) {
      db.execute(sql, args?.map(_convert).toList());
      return Future.value(); // return a void Future
    } else {
      throw 'Unsupported database type: ${db.runtimeType}';
    }
  }

  Future<void> _createTable<T>(
      T db, CreateTableStatement statement, List<Object?>? columns) async {
    statement = CrdtUtil.prepareCreate(statement);
    return _baseExecute(db, statement.toSql(), columns);
  }

  Future<void> _innerExecute<T>(
      T db, String sql, HlcGenerator hlc, List<Object?>? args) async {
    final result = CrdtUtil.parseSql(sql);

    // Warn if the query can't be parsed
    if (result.rootNode is InvalidStatement) {
      print('Warning: unable to parse SQL statement.');
      if (sql.contains(';')) {
        print('The parser can only interpret single statements.');
      }
      print(sql);
    }

    // Bail on "manual" transaction statements
    if (result.rootNode is BeginTransactionStatement ||
        result.rootNode is CommitStatement) {
      throw 'Unsupported statement: $sql.\nUse SqliteCrdt.transaction() instead.';
    }

    if (result.rootNode is CreateTableStatement) {
      await _createTable(db, result.rootNode as CreateTableStatement, args);
    } else if (result.rootNode is InsertStatement) {
      await _rawInsert(db, result.rootNode as InsertStatement, args, hlc());
    } else if (result.rootNode is UpdateStatement) {
      await _rawUpdate(db, result.rootNode as UpdateStatement, args, hlc());
    } else if (result.rootNode is DeleteStatement) {
      await _rawDelete(db, result.rootNode as DeleteStatement, args, hlc());
    } else {
      return _baseExecute(db, sql, args);
    }
  }
}
