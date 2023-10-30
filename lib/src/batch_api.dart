import 'package:sqflite_common/sqlite_api.dart' show Batch;
import 'package:synchroflite/src/sqflite_api.dart' show SqfliteApi;

abstract class BatchApi extends Batch {
  /// See [Batch.commit]
  @override
  Future<List<Object?>> commit({
    bool? exclusive,
    bool? noResult,
    bool? continueOnError,
  });

  /// See [Batch.apply]
  @override
  Future<List<Object?>> apply({bool? noResult, bool? continueOnError});

  /// See [SqfliteApi.rawInsert]
  @override
  void rawInsert(String sql, [List<Object?>? arguments]);

  /// See [SqfliteApi.rawUpdate]
  @override
  void rawUpdate(String sql, [List<Object?>? arguments]);

  /// See [SqfliteApi.rawDelete]
  @override
  void rawDelete(String sql, [List<Object?>? arguments]);

  /// See [SqfliteApi.execute];
  @override
  void execute(String sql, [List<Object?>? arguments]);

  /// See [SqfliteApi.rawQuery];
  @override
  void rawQuery(String sql, [List<Object?>? arguments]);

  /// Current batch size
  @override
  int get length;
}
