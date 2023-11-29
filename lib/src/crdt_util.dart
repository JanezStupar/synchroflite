// Copyright 2023 Janez Stupar
// This code is based on Daniel Cachapa's work in sql_crdt:
// https://github.com/cachapa/sql_crdt
// SPDX-License-Identifier: Apache-2.0

import 'package:source_span/source_span.dart';
import 'package:collection/collection.dart';
import 'package:sql_crdt/sql_crdt.dart';
import 'package:sqlparser/sqlparser.dart';

typedef TestFunc<E> = bool Function(E element);
typedef CreatorFunc<E> = E Function(int listLength);
typedef NumberedExtractor<E> = int Function(E element);

Iterable<AstNode> filterNodes(Iterable<AstNode> nodes, Type type) {
  final filtered = <String>{};
  if (type == Reference) {
    return nodes.whereType<Reference>().where((element) {
      if (filtered.contains(element.columnName)) {
        return false;
      }
      filtered.add(element.columnName);
      return true;
    });
  } else if (type == NumberedVariable) {
    return nodes.whereType<NumberedVariable>().where((element) {
      if (filtered.contains(element.explicitIndex.toString())) {
        return false;
      }
      if (element.explicitIndex != null) {
        filtered.add(element.explicitIndex.toString());
      }
      return true;
    });
  } else {
    throw 'Unsupported type';
  }
}

class CrdtArgParser {
  final Map<String, int> _argMap = {};
  final List<Object?> _args;
  final Hlc _hlc;
  final List<AstNode> _nodes;
  final List<AstNode> _numberedNodes;

  /// [args] is the list of arguments
  /// [hlc] is the HLC timestamp
  /// [argumentCount] is the number of named parameters in the statement
  CrdtArgParser(this._args, this._hlc, List<AstNode> nodes)
      : _nodes = filterNodes(nodes, Reference).toList(),
        _numberedNodes = filterNodes(nodes, NumberedVariable).toList();

  void init() {
    _argMap.clear();
    var argumentCount = _nodes.length + _argMap.length + 1;
    ['hlc', 'node_id', 'modified'].forEach((element) {
      var index = extractFromNodes(_nodes, element);
      if (index != -1) {
        if (_argMap[element] == null) {
          _argMap[element] = index;
        } else {
          _argMap[element] = argumentCount;
        }
      }
    });
  }

  /// Find out whether the argument is already present in the list
  /// Store the index of the argument in the map
  /// the index is inferred from the explicit index of the argument
  /// [name] is the name of the named parameter
  /// [iterable] is the list of arguments
  /// [test] is the function that tests whether the named parameter is already present
  /// [creator] is the function that creates the named parameter
  List<E> fromIterable<E>(String name, Iterable<E> iterable, TestFunc<E> test,
      CreatorFunc<E> creator) {
    var list = iterable.toList(growable: true);
    var argumentCount = _numberedNodes.length + _argMap.length + 1;
    var index = extractFromNodes(_nodes, name);
    var indexList = extractFromNodes(list as List<AstNode>, name);
    if (index != -1 && _argMap[name] == null) {
      _argMap[name] = index;
    }
    if (indexList == -1) {
      if (_argMap[name] != null) {
        index = _argMap[name]!;
        list.add(creator(index));
      } else {
        index = argumentCount;
        list.add(creator(argumentCount));
        _argMap[name] = index;
      }
    }
    return list;
  }

  int extractFromNodes(Iterable<AstNode> expressions, String name) {
    var explicitIndex = -1;
    var index = -1;

    // Write an optimized version of the above code
    var expList = expressions.toList();
    for (var element in expList) {
      final i = expList.indexOf(element);
      if (explicitIndex != -1 && index != -1) {
        break;
      }
      if (element is Reference || element is ExpressionResultColumn) {
        var ref;
        if (element is ExpressionResultColumn &&
            element.expression is Reference) {
          ref = element.expression as Reference;
        } else if (element is ExpressionResultColumn &&
            element.expression is CastExpression) {
          ref = (element.expression as CastExpression).operand as Reference;
        } else {
          ref = element;
        }
        if (ref.columnName == name) {
          if (ref.parent is SetComponent) {
            final parent = ref.parent as SetComponent;
            if (parent.expression is NumberedVariable) {
              final numbered = parent.expression as NumberedVariable;
              if (numbered.explicitIndex != null) {
                if (explicitIndex == -1) {
                  explicitIndex = numbered.explicitIndex!;
                }
              }
            }
          }
          if (index == -1) {
            index = i + 1;
          }
        }
      }
    }

    return explicitIndex == -1 ? index : explicitIndex;
  }

  /// Dump the arguments in the order of the named parameters
  List<Object?> dumpArgs() {
    var args = [..._args];

    // We don't append delete unless it is explicitly mentioned in some argument
    if (_argMap['is_deleted'] != null && _argMap['is_deleted']! == -1) {
      args[_argMap['is_deleted']!.toInt()] = _args[_argMap['is_deleted']!] ?? 0;
    }

    // If there are no arguments, then there it makes no sense to append hlc
    if (args.isEmpty) {
      return [];
    }

    // Other columns should always be appended
    var argIndex;
    argIndex = _argMap['hlc']?.toInt() ?? -1;
    if (argIndex != -1 && argIndex <= args.length) {
      args[argIndex - 1] = _hlc;
    } else {
      args.add(_hlc);
    }

    argIndex = _argMap['node_id']?.toInt() ?? -1;
    if (argIndex != -1 && argIndex <= args.length) {
      args[argIndex - 1] = _hlc.nodeId;
    } else {
      args.add(_hlc.nodeId);
    }

    argIndex = _argMap['modified']?.toInt() ?? -1;
    if (argIndex != -1 && argIndex <= args.length) {
      args[argIndex - 1] = _hlc;
    } else {
      args.add(_hlc);
    }
    return args;
  }
}

// This class contains utility functions for transforming SQL statements that has been extracted from `sql_crdt` package.
class CrdtUtil {
  static final _sqlEngine = SqlEngine();

  /// function takes a SQL statement and a list of arguments
  /// transforms the SQL statement to change parameters with automatic index
  /// into parameters with explicit index
  static void transformAutomaticExplicit(Statement statement) {
    statement.allDescendants
        .whereType<NumberedVariable>()
        .forEachIndexed((i, ref) {
      ref.explicitIndex ??= i + 1;
    });
  }

  static Expression _listToBinaryExpression(
      List<Expression> expressions, Token token) {
    if (expressions.length == 1) {
      return expressions.first;
    }
    return BinaryExpression(expressions.first, token,
        _listToBinaryExpression(expressions.sublist(1), token));
  }

  static (UpdateStatement, List<Object?>?) prepareUpdate(
      UpdateStatement statement, List<Object?>? args, hlc) {
    transformAutomaticExplicit(statement);
    final argParser =
        CrdtArgParser(args ?? [], hlc, statement.allDescendants.toList());
    argParser.init();

    var set = statement.set;
    set = argParser.fromIterable(
        'hlc',
        set,
        (SetComponent element) => element.column.columnName == 'hlc',
        (listLength) => SetComponent(
            column: Reference(columnName: 'hlc'),
            expression: NumberedVariable(listLength)));
    set = argParser.fromIterable(
        'node_id',
        set,
        (SetComponent element) => element.column.columnName == 'node_id',
        (listLength) => SetComponent(
            column: Reference(columnName: 'node_id'),
            expression: NumberedVariable(listLength)));
    set = argParser.fromIterable(
        'modified',
        set,
        (SetComponent element) => element.column.columnName == 'modified',
        (listLength) => SetComponent(
            column: Reference(columnName: 'modified'),
            expression: NumberedVariable(listLength)));

    final newStatement = UpdateStatement(
      withClause: statement.withClause,
      returning: statement.returning,
      from: statement.from,
      or: statement.or,
      table: statement.table,
      set: set,
      where: statement.where,
    );

    final newArgs = argParser.dumpArgs();
    return (newStatement, newArgs);
  }

  static SelectStatement prepareSelect(
      SelectStatement statement, List<Object?>? args) {
    transformAutomaticExplicit(statement);
    var fakeSpan = SourceFile.fromString('fakeSpan').span(0);
    var andToken = Token(TokenType.and, fakeSpan);
    var equalToken = Token(TokenType.equal, fakeSpan);
    var deletedExpr = <Expression>[];

    statement.from?.allDescendants
        .whereType<TableReference>()
        .forEachIndexed((index, reference) {
      if (reference.as != null) {
        deletedExpr.add(BinaryExpression(
            Reference(
                columnName: 'is_deleted',
                entityName: reference.as,
                schemaName: reference.schemaName),
            equalToken,
            NumericLiteral(0)));
        print(reference.tableName);
      }
    });
    if (deletedExpr.isEmpty) {
      deletedExpr.add(BinaryExpression(
          Reference(columnName: 'is_deleted'), equalToken, NumericLiteral(0)));
    }

    if (statement.where != null) {
      statement.where = BinaryExpression(
          statement.where!,
          Token(TokenType.and, fakeSpan),
          _listToBinaryExpression(deletedExpr, andToken));
    } else {
      statement.where = _listToBinaryExpression(deletedExpr, andToken);
    }

    return statement;
  }

  static (UpdateStatement, List<Object?>?) prepareDelete(
      DeleteStatement statement, List<Object?>? args, Hlc hlc) {
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

    final newArgs = [...args ?? [], 1, hlc, hlc.nodeId, hlc];
    return (newStatement, newArgs);
  }

  static (InsertStatement, List<Object?>?) prepareInsert(
      InsertStatement statement, List<Object?>? args, Hlc hlc) {
    transformAutomaticExplicit(statement);
    final argCount = args?.length ?? 0;
    final argParser =
        CrdtArgParser(args ?? [], hlc, statement.allDescendants.toList());
    argParser.init();
    var source;

    // targetColumns
    var targetColumns = [...statement.targetColumns];

    targetColumns = argParser.fromIterable(
        'hlc',
        targetColumns,
        (Reference element) => element.columnName == 'hlc',
        (_) => Reference(columnName: 'hlc'));

    targetColumns = argParser.fromIterable(
        'node_id',
        targetColumns,
        (Reference element) => element.columnName == 'node_id',
        (_) => Reference(columnName: 'node_id'));

    targetColumns = argParser.fromIterable(
        'modified',
        targetColumns,
        (Reference element) => element.columnName == 'modified',
        (_) => Reference(columnName: 'modified'));

    //  Sort out the source portion of the statement
    if (statement.source is SelectInsertSource) {
      source =
          ((statement.source as SelectInsertSource).stmt as SelectStatement)
              .columns
              .map((e) => e as ExpressionResultColumn);

      source = argParser.fromIterable(
          'hlc',
          source,
          (ExpressionResultColumn element) =>
              (element.expression as Reference).columnName == 'hlc',
          (_) =>
              ExpressionResultColumn(expression: Reference(columnName: 'hlc')));

      source = argParser.fromIterable(
          'node_id',
          source,
          (ExpressionResultColumn element) =>
              (element.expression as Reference).columnName == 'node_id',
          (_) => ExpressionResultColumn(
              expression: Reference(columnName: 'node_id')));

      source = argParser.fromIterable(
          'modified',
          source,
          (ExpressionResultColumn element) =>
              (element.expression as Reference).columnName == 'modified',
          (_) => ExpressionResultColumn(
              expression: Reference(columnName: 'modified')));

      source = SelectInsertSource(SelectStatement(
          columns: source,
          from:
              ((statement.source as SelectInsertSource).stmt as SelectStatement)
                  .from));
    } else {
      final expressions =
          (statement.source as ValuesSource).values.first.expressions;
      final valueSources = [];
      for (var i = 1; expressions.length + i <= targetColumns.length; i++) {
        valueSources.add(NumberedVariable(argCount + i));
      }
      source = ValuesSource([
        Tuple(expressions: [
          ...expressions,
          ...valueSources,
        ])
      ]);
    }

    final newStatement = InsertStatement(
      mode: statement.mode,
      upsert: statement.upsert,
      returning: statement.returning,
      withClause: statement.withClause,
      table: statement.table,
      targetColumns: targetColumns,
      source: source,
    );

    // Touch
    if (newStatement.upsert is UpsertClause) {
      final action = newStatement.upsert!.entries.first.action;
      if (action is DoUpdate) {
        action.set = argParser.fromIterable(
            'hlc',
            action.set,
            (SetComponent element) => element.column.columnName == 'hlc',
            (listLength) => SetComponent(
                column: Reference(columnName: 'hlc'),
                expression: NumberedVariable(listLength)));
        action.set = argParser.fromIterable(
            'node_id',
            action.set,
            (SetComponent element) => element.column.columnName == 'node_id',
            (listLength) => SetComponent(
                column: Reference(columnName: 'node_id'),
                expression: NumberedVariable(listLength)));
        action.set = argParser.fromIterable(
            'modified',
            action.set,
            (SetComponent element) => element.column.columnName == 'modified',
            (listLength) => SetComponent(
                column: Reference(columnName: 'modified'),
                expression: NumberedVariable(listLength)));
      }
    }

    return (newStatement, argParser.dumpArgs());
  }

  static CreateTableStatement prepareCreate(CreateTableStatement statement) {
    final columns = statement.columns;
    final columnNames = columns.map((e) => e.columnName).toList();

    // is deleted
    final isDeleted = ColumnDefinition(
      columnName: 'is_deleted',
      typeName: 'INTEGER',
      constraints: [Default(null, NumericLiteral(0))],
    );
    if (!columnNames.contains('is_deleted')) {
      columns.add(isDeleted);
    } else {
      columns[columnNames.indexOf('is_deleted')] = isDeleted;
    }

    // hlc
    final hlc = ColumnDefinition(
      columnName: 'hlc',
      typeName: 'TEXT',
      constraints: [NotNull(null)],
    );
    if (!columnNames.contains('hlc')) {
      columns.add(hlc);
    } else {
      columns[columnNames.indexOf('hlc')] = hlc;
    }

    // node_id
    final nodeId = ColumnDefinition(
      columnName: 'node_id',
      typeName: 'TEXT',
      constraints: [NotNull(null)],
    );
    if (!columnNames.contains('node_id')) {
      columns.add(nodeId);
    } else {
      columns[columnNames.indexOf('node_id')] = nodeId;
    }

    // modified
    final modified = ColumnDefinition(
      columnName: 'modified',
      typeName: 'TEXT',
      constraints: [NotNull(null)],
    );
    if (!columnNames.contains('modified')) {
      columns.add(modified);
    } else {
      columns[columnNames.indexOf('modified')] = modified;
    }

    final newStatement = CreateTableStatement(
      tableName: statement.tableName,
      columns: columns,
      tableConstraints: statement.tableConstraints,
      ifNotExists: statement.ifNotExists,
      isStrict: statement.isStrict,
      withoutRowId: statement.withoutRowId,
    );
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
