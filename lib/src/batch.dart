part of 'package:synchroflite/synchroflite.dart';

typedef WithHlc = void Function(Hlc hlc);
typedef OnChangeFunction = Future<void> Function(Iterable<String> tables, Hlc hlc);

// Override of Sqflite's Batch class to allow for injection of CRDT logic
class BatchCrdt with SqfliteCrdtImplMixin implements BatchApi {
  final Batch _db;
  final bool inTransaction;
  int _length = 0;
  // We enqueue statements to be executed later when we know whether the
  // batch is being committed or applied so we can add the appropriate HLC's
  final List<Function> _statementQueue = [];

  @override
  int get length => _length;

  final Hlc canonicalTime;
  final OnChangeFunction onDatasetChanged;
  String get nodeId => canonicalTime.nodeId;

  final affectedTables = <String>{};

  BatchCrdt(this._db, this.canonicalTime, this.onDatasetChanged, {this.inTransaction=false});

  void _enqueue(Function statement) {
    _statementQueue.add(statement);
  }

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


  @override
  void rawQuery(String sql, [List<Object?>? args]) {
    _length++;
    _enqueue(() => _innerRawQuery(_db, sql, args));
  }

  @override
  void rawUpdate(String sql, [List<Object?>? args]) {
    _length++;
    _enqueue((hlc) => _innerRawUpdate(_db, sql, args, hlc));
  }

  @override
  void rawInsert(String sql, [List<Object?>? args]) {
    _length++;
    _enqueue((hlc) => _innerRawInsert(_db, sql, args, hlc));
  }

  @override
  void rawDelete(String sql, [List<Object?>? args]) {
    _length++;
    _enqueue((hlc) => _innerRawDelete(_db, sql, args, hlc));
  }

  void _createTable(Batch batch, CreateTableStatement statement, args) {
    affectedTables.add(statement.tableName);
    batch.execute(CrdtUtil.prepareCreate(statement).toSql(), args);
  }

  void _batchExecute(Batch batch, String sql, List<Object?>? args, [Hlc? hlc]) {
    final result = CrdtUtil.parseSql(sql);

    // Warn if the query can't be parsed
    if (result.rootNode is InvalidStatement) {
      print('Warning: unable to parse SQL statement.');
      if (sql.contains(';')) {
        print('The parser can only interpret single statements.');
      }
      print(sql);
    }

    if (result.rootNode is CreateTableStatement) {
      _createTable(batch, result.rootNode as CreateTableStatement, args);
    } else if (result.rootNode is InsertStatement) {
      _innerRawInsert(batch, sql, args, hlc);
    } else if (result.rootNode is UpdateStatement) {
      _innerRawUpdate(batch, sql, args, hlc);
    } else if (result.rootNode is DeleteStatement) {
      _innerRawDelete(batch, sql, args, hlc);
    } else {
      // Run the query unchanged
      batch.execute(sql, args?.map(_convert).toList());
    }
  }

  @override
  void execute(String sql, [List<Object?>? arguments]) {
    _length++;
    _enqueue((hlc) => _batchExecute(_db, sql, arguments, hlc));
  }


  // When applying a batch, we increment the HLC's of all statements
  @override
  Future<List<Object?>> apply({bool? noResult, bool? continueOnError}) {
    for (var statement in _statementQueue) {
      if (statement is WithHlc) {
        if (inTransaction) {
          statement(canonicalTime);
        } else {
          statement(canonicalTime.increment());
        }
      } else {
        statement();
      }
    }
    return _db.apply(noResult: noResult, continueOnError: continueOnError).whenComplete(() async {
      if (affectedTables.isNotEmpty) {
        await onDatasetChanged(affectedTables, canonicalTime);
      }
    });
  }

  // When committing a batch, all statements have same timestamp as batch
  @override
  Future<List<Object?>> commit(
      {bool? exclusive, bool? noResult, bool? continueOnError}) {
    for (var statement in _statementQueue) {
      if (statement is WithHlc) {
        statement(canonicalTime);
      } else {
        statement();
      }
    }
    return _db.commit(exclusive: exclusive,
        noResult: noResult,
        continueOnError: continueOnError).whenComplete(() {
      if (affectedTables.isNotEmpty) {
        onDatasetChanged(affectedTables, canonicalTime);
      }
    });
  }

  @override //overriding noSuchMethod
  void noSuchMethod(Invocation invocation) =>
      'Got the ${invocation.memberName} with arguments ${invocation
          .positionalArguments}';
}
