part of 'package:synchroflite/synchroflite.dart';

class BatchCrdt with SqfliteCrdtImplMixin implements BatchApi {
  final Batch _db;
  int _length = 0;

  @override
  int get length => _length;

  final Hlc canonicalTime;
  String get nodeId => canonicalTime.nodeId;

  final affectedTables = <String>{};

  BatchCrdt(this._db, this.canonicalTime);

  @override
  Future<R> _rawInsert<T, R>(T db, InsertStatement statement,
      [List<Object?>? args, Hlc? hlc]) {
    affectedTables.add(statement.table.tableName);
    return super._rawInsert(db, statement, args, hlc);
  }

  @override
  Future<R> _rawUpdate<T, R>(T db, UpdateStatement statement,
      [List<Object?>? args, Hlc? hlc]) {
    affectedTables.add(statement.table.tableName);
    return super._rawUpdate(db, statement, args, hlc);
  }

  @override
  Future<R> _rawDelete<T, R>(T db, DeleteStatement statement,
      [List<Object?>? args, Hlc? hlc]) {
    affectedTables.add(statement.table.tableName);
    return super._rawDelete(db, statement, args, hlc);
  }


  @override
  void rawQuery(String sql,
      [List<Object?>? args]) {
    _length++;
    _innerRawQuery(_db, sql, args);
  }

  @override
  void rawUpdate(String sql, [List<Object?>? args]) {
    _length++;
    _innerRawUpdate(_db, sql, args, canonicalTime);
  }

  @override
  void rawInsert(String sql, [List<Object?>? args]) {
    _length++;
    _innerRawInsert(_db, sql, args, canonicalTime);
  }

  @override
  void rawDelete(String sql, [List<Object?>? args]) {
    _length++;
     _innerRawDelete(_db, sql, args, canonicalTime);
  }

  @override
  void execute(String sql, [List<Object?>? arguments]) {
    _length++;
    _db.execute(sql, arguments);
  }


  @override
  Future<List<Object?>> apply({bool? noResult, bool? continueOnError}) {
    return _db.apply(noResult: noResult, continueOnError: continueOnError);
  }

  @override
  Future<List<Object?>> commit({bool? exclusive, bool? noResult, bool? continueOnError}) {
    return _db.commit(exclusive: exclusive, noResult: noResult, continueOnError: continueOnError);
  }

  @override //overriding noSuchMethod
    noSuchMethod(Invocation invocation) => 'Got the ${invocation.memberName} with arguments ${invocation.positionalArguments}';
}
