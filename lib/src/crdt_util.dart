import 'package:source_span/source_span.dart';
import 'package:collection/collection.dart';
import 'package:sqlparser/sqlparser.dart';

class CrdtUtil {
  static final _sqlEngine = SqlEngine();

  /// function takes a SQL statement and a list of arguments
  /// transforms the SQL statement to change parameters with automatic index
  /// into parameters with explicit index
  static transformAutomaticExplicit(Statement statement) {
    statement.allDescendants.whereType<NumberedVariable>().forEachIndexed((i, ref) {
      ref.explicitIndex ??= i + 1;
    });
  }

  static _listToBinaryExpression (List<Expression> expressions, Token token) {
    if (expressions.length == 1) {
      return expressions.first;
    }
    return BinaryExpression(expressions.first, token, _listToBinaryExpression(expressions.sublist(1), token));
  }

  static UpdateStatement prepareUpdate(UpdateStatement statement, List<Object?>? args) {
    final argCount = args?.length ?? 0;
    transformAutomaticExplicit(statement);
    final newStatement = UpdateStatement(
      withClause: statement.withClause,
      returning: statement.returning,
      from: statement.from,
      or: statement.or,
      table: statement.table,
      set: [
        ...statement.set,
        SetComponent(
          column: Reference(columnName: 'hlc'),
          expression: NumberedVariable(argCount + 1),
        ),
        SetComponent(
          column: Reference(columnName: 'node_id'),
          expression: NumberedVariable(argCount + 2),
        ),
        SetComponent(
          column: Reference(columnName: 'modified'),
          expression: NumberedVariable(argCount + 3),
        ),
      ],
      where: statement.where,
    );

    return newStatement;
  }

  static SelectStatement prepareSelect(SelectStatement statement, List<Object?>? args) {
    transformAutomaticExplicit(statement);
    var fakeSpan = SourceFile.fromString('fakeSpan').span(0);
    var andToken = Token(TokenType.and, fakeSpan);
    var equalToken = Token(TokenType.equal, fakeSpan);

    List<Expression> deletedExpr = [];
    statement.from?.allDescendants.whereType<TableReference>().forEachIndexed((index, reference) {
      if (reference.as != null) {
        deletedExpr.add(BinaryExpression(
            Reference(columnName: 'is_deleted', entityName: reference.as, schemaName: reference.schemaName),
            equalToken,
            NumericLiteral(0)
        ));
        print(reference.tableName);
      }
    });
    if (deletedExpr.isEmpty) {
      deletedExpr.add(BinaryExpression(
          Reference(columnName: 'is_deleted'),
          equalToken,
          NumericLiteral(0)
      ));
    }


    if (statement.where != null) {
      statement.where = BinaryExpression(
          statement.where!,
          Token(TokenType.and, fakeSpan),
          _listToBinaryExpression(deletedExpr, andToken)
      );
    } else {
      statement.where = _listToBinaryExpression(deletedExpr, andToken);
    }

    return statement;
  }

  static UpdateStatement prepareDelete(DeleteStatement statement, List<Object?>? args) {
    final argCount = args?.length ?? 0;
    transformAutomaticExplicit(statement);
    final newStatement = UpdateStatement(
      returning: statement.returning,
      withClause: statement.withClause,
      table: statement.table,
      set: [
        SetComponent(
          column: Reference(columnName: 'is_deleted'),
          expression: NumberedVariable(argCount + 1),
        ),
        SetComponent(
          column: Reference(columnName: 'hlc'),
          expression: NumberedVariable(argCount + 2),
        ),
        SetComponent(
          column: Reference(columnName: 'node_id'),
          expression: NumberedVariable(argCount + 3),
        ),
        SetComponent(
          column: Reference(columnName: 'modified'),
          expression: NumberedVariable(argCount + 4),
        ),
      ],
      where: statement.where,
    );

    // args = [...args ?? [], 1, hlc, hlc.nodeId, hlc];
    return newStatement;
  }


  static InsertStatement prepareInsert(InsertStatement statement, List<Object?>? args) {
    final argCount = args?.length ?? 0;
    transformAutomaticExplicit(statement);
    final newStatement = InsertStatement(
      mode: statement.mode,
      upsert: statement.upsert,
      returning: statement.returning,
      withClause: statement.withClause,
      table: statement.table,
      targetColumns: [
        ...statement.targetColumns,
        Reference(columnName: 'hlc'),
        Reference(columnName: 'node_id'),
        Reference(columnName: 'modified'),
      ],
      source: ValuesSource([
        Tuple(expressions: [
          ...(statement.source as ValuesSource).values.first.expressions,
          NumberedVariable(argCount + 1),
          NumberedVariable(argCount + 2),
          NumberedVariable(argCount + 3),
        ])
      ]),
    );

    // Touch
    if (statement.upsert is UpsertClause) {
      final action = statement.upsert!.entries.first.action;
      if (action is DoUpdate) {
        action.set.addAll([
          SetComponent(
            column: Reference(columnName: 'hlc'),
            expression: NumberedVariable(argCount + 1),
          ),
          SetComponent(
            column: Reference(columnName: 'node_id'),
            expression: NumberedVariable(argCount + 2),
          ),
          SetComponent(
            column: Reference(columnName: 'modified'),
            expression: NumberedVariable(argCount + 3),
          ),
        ]);
      }
    }

    return newStatement;
  }

  static ParseResult parseSql(String sql) {
    final result = _sqlEngine.parse(sql);

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

    return result;
  }
}
