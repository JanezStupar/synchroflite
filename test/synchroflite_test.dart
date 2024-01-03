// Copyright 2023 Daniel Cachapa
// Copyright 2023 Janez Stupar
// This file is copied from sqlite_crdt package:
// https://github.com/cachapa/sqlite_crdt
// SPDX-License-Identifier: Apache-2.0
import 'package:synchroflite/synchroflite.dart';
import 'package:test/test.dart';

void main() {
  group('Basic', () {
    late SqlCrdt crdt;

    setUp(() async {
      crdt = await Synchroflite.openInMemory(
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
            )
          ''');
        },
      );
    });

    test('Node ID', () {
      expect(crdt.nodeId.isEmpty, false);
    });

    test('Canonical time', () async {
      expect(crdt.canonicalTime.dateTime,
          DateTime.fromMillisecondsSinceEpoch(0).toUtc());

      await _insertUser(crdt, 1, 'John Doe');
      final result = await crdt.query('SELECT * FROM users');
      final hlc = (result.first['hlc'] as String).toHlc;
      expect(crdt.canonicalTime, hlc);

      await _insertUser(crdt, 2, 'Jane Doe');
      final newResult = await crdt.query('SELECT * FROM users');
      final newHlc = (newResult.last['hlc'] as String).toHlc;
      expect(newHlc > hlc, isTrue);
      expect(crdt.canonicalTime, newHlc);
    });

    test('Create table', () async {
      await crdt.execute('''
        CREATE TABLE test (
          id INTEGER NOT NULL,
          name TEXT,
          PRIMARY KEY (id)
        )
      ''');
      final result = await crdt.query('SELECT * FROM test');
      expect(result, []);
    });

    test('Insert', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'John Doe');
    });

    test('Replace', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final insertHlc =
          (await crdt.query('SELECT hlc FROM users')).first['hlc'] as String;
      await crdt.execute(
          'REPLACE INTO users (id, name) VALUES (?1, ?2)', [1, 'Jane Doe']);
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'Jane Doe');
      expect((result.first['hlc'] as String).compareTo(insertHlc), 1);
    });

    test('Upsert', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final insertHlc =
          (await crdt.query('SELECT hlc FROM users')).first['hlc'] as String;
      await crdt.execute('''
        INSERT INTO users (id, name) VALUES (?1, ?2)
        ON CONFLICT (id) DO UPDATE SET name = ?2
      ''', [1, 'Jane Doe']);
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'Jane Doe');
      expect((result.first['hlc'] as String).compareTo(insertHlc), 1);
    });

    test('Update', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final insertHlc =
          (await crdt.query('SELECT hlc FROM users')).first['hlc'] as String;
      await _updateUser(crdt, 1, 'Jane Doe');
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'Jane Doe');
      expect((result.first['hlc'] as String).compareTo(insertHlc), 1);
    });

    test('Delete', () async {
      await _insertUser(crdt, 1, 'John Doe');
      await crdt.execute('''
        DELETE FROM users
        WHERE id = ?1
      ''', [1]);
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'John Doe');
      expect(result.first['is_deleted'], 1);
    });

    test('Transaction', () async {
      await crdt.transaction((txn) async {
        await _insertUser(txn, 1, 'John Doe');
        await _insertUser(txn, 2, 'Jane Doe');
      });
      final result = await crdt.query('SELECT * FROM users');
      expect(result.length, 2);
      expect(result.first['hlc'], result.last['hlc']);
    });

    test('Changeset', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final result = await crdt.getChangeset();
      expect(result['users']!.first['name'], 'John Doe');
    });

    test('Merge', () async {
      final hlc = Hlc.now('test_node_id');
      await crdt.merge({
        'users': [
          {
            'id': 1,
            'name': 'John Doe',
            'hlc': hlc,
          },
        ],
      });
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'John Doe');
      expect(result.first['hlc'] as String, hlc.toString());
    });

    /// This is current behavior, but it is not ideal.
    /// So the client library should handle this case.
    test('getLastModified for unknown node', () async {
      final result = await crdt.getLastModified(onlyNodeId: 'unknown_node');
      expect(result.nodeId, isNot(equals('unknown_node')));
    });
  });

  group('Watch', () {
    late SqlCrdt crdt;

    setUp(() async {
      crdt = await Synchroflite.openInMemory(
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
            )
          ''');
          await db.execute('''
            CREATE TABLE purchases (
              id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              price REAL NOT NULL,
              PRIMARY KEY (id)
            )
          ''');
        },
      );
    });

    test('Emit on watch', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emits((List<Map<String, Object?>> e) => e.first['name'] == 'John Doe'),
      );
      await streamTest;
    });

    test('Emit on insert', () async {
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emitsInOrder([
          [],
          (List<Map<String, Object?>> e) => e.first['name'] == 'John Doe',
        ]),
      );
      await _insertUser(crdt, 1, 'John Doe');
      await streamTest;
    });

    test('Emit on update', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emitsInOrder([
          (List<Map<String, Object?>> e) => e.first['name'] == 'John Doe',
          (List<Map<String, Object?>> e) => e.first['name'] == 'Jane Doe',
        ]),
      );
      await _updateUser(crdt, 1, 'Jane Doe');
      await streamTest;
    });

    test('Emit on delete', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users WHERE is_deleted = 0'),
        emitsInOrder([
          (List<Map<String, Object?>> e) => e.first['name'] == 'John Doe',
          [],
        ]),
      );
      await _deleteUser(crdt, 1);
      await streamTest;
    });

    test('Emit on transaction', () async {
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emitsInOrder([
          [],
          (List<Map<String, Object?>> e) =>
              e.first['name'] == 'John Doe' && e.last['name'] == 'Jane Doe',
        ]),
      );
      await crdt.transaction((txn) async {
        await _insertUser(txn, 1, 'John Doe');
        await _insertUser(txn, 2, 'Jane Doe');
      });
      await streamTest;
    });

    test('Emit on merge', () async {
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emitsInOrder([
          [],
          (List<Map<String, Object?>> e) => e.first['name'] == 'John Doe',
        ]),
      );
      await crdt.merge({
        'users': [
          {
            'id': 1,
            'name': 'John Doe',
            'hlc': Hlc.now('test_node_id'),
          },
        ],
      });
      await streamTest;
    });

    test('Emit only on selected table', () async {
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emitsInOrder([
          [],
          (List<Map<String, Object?>> e) => e.first['name'] == 'John Doe',
        ]),
      );
      await _insertPurchase(crdt, 1, 1, 12.3);
      await _insertUser(crdt, 1, 'John Doe');
      await streamTest;
    });

    test('Emit on all selected tables', () async {
      final streamTest = expectLater(
        crdt.watch(
            'SELECT users.name, price FROM users LEFT JOIN purchases ON users.id = user_id'),
        emitsInOrder([
          [],
          (List<Map<String, Object?>> e) =>
              e.first['name'] == 'John Doe' && e.first['price'] == null,
          (List<Map<String, Object?>> e) =>
              e.first['name'] == 'John Doe' && e.first['price'] == 12.3,
        ]),
      );
      await _insertUser(crdt, 1, 'John Doe');
      await _insertPurchase(crdt, 1, 1, 12.3);
      await streamTest;
    });
  });

  group('Synchroflite', () {
    late Synchroflite crdt;

    setUp(() async {
      crdt = await Synchroflite.openInMemory(
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
            )
          ''');
        },
      );
    });

    test('rawInsert', () async {
      final id = await crdt.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      expect(id, 1);
    });

    test('rawUpdate', () async {
      await crdt.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1,  ?2)
      ''', [1, 'John Doe']);
      final rowsAffected = await crdt.rawUpdate('''
        UPDATE users SET name = ?2
        WHERE id = ?1
      ''', [1, 'Jane Doe']);
      expect(rowsAffected, 1);
    });

    test('rawDelete', () async {
      await crdt.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      final rowsAffected = await crdt.rawDelete('''
        DELETE FROM users WHERE id = ?1
      ''', [1]);
      expect(rowsAffected, 1);
    });

    test('update multiple', () async {
      await crdt.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      await crdt.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [2, 'Jane Doe']);
      final rowsAffected = await crdt.rawUpdate('''
        UPDATE users SET name = ?1
      ''', ['Bobby Doe']);
      expect(rowsAffected, 2);
    });
  });

  group('Synchroflite batch', () {
    late Synchroflite crdt;

    setUp(() async {
      crdt = await Synchroflite.openInMemory(
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
            )
          ''');
        },
      );
    });

    test('batch commit', () async {
      final batch = crdt.batch();
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [2, 'Jane Doe']);
      final result = await batch.commit();
      expect(result.length, 2);
    });

    test('batch apply', () async {
      final batch = crdt.batch();
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [2, 'Jane Doe']);
      final result = await batch.apply();
      expect(result.length, 2);
    });

    test('batch insert, query, apply', () async {
      final batch = crdt.batch();
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [2, 'Jane Doe']);
      batch.rawQuery('SELECT * FROM users');
      expect(batch.length, 3);
      final result = await batch.apply();
      expect(result.length, 3);
    });

    test('batch insert, query, commit', () async {
      final batch = crdt.batch();
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [2, 'Jane Doe']);
      batch.rawQuery('SELECT * FROM users');
      expect(batch.length, 3);
      final result = await batch.commit();
      expect(result.length, 3);
    });

    // When batch.apply is called very operation should have its own timestamp
    test('batch apply timestamps', () async {
      final batch = crdt.batch();
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [2, 'Jane Doe']);
      await batch.apply();
      final changeset = await crdt.getChangeset();
      expect(changeset['users']!.first['hlc'],
          isNot(equals(changeset['users']!.last['hlc'])));
    });

    // When batch.commit is called all operations should have the same timestamp
    test('batch commit timestamps', () async {
      final batch = crdt.batch();
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      batch.rawInsert('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [2, 'Jane Doe']);
      await batch.commit();
      final changeset = await crdt.getChangeset();
      expect(changeset['users']!.first['hlc'],
          equals(changeset['users']!.last['hlc']));
    });

    // when batch calls execute the timestamp should be correct
    test('batch execute', () async {
      final batch = crdt.batch();
      batch.execute('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      await batch.apply();
      final changeset = await crdt.getChangeset();
      expect(changeset['users']!.first['hlc'], isNotNull);
    });

    // when batch calls execute the timestamp should be correct
    test('batch execute multiple', () async {
      final batch = crdt.batch();
      batch.execute('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      batch.execute('SELECT 1');
      batch.execute('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [2, 'Jane Doe']);
      batch.execute('''
      UPDATE users SET name = ?2
      WHERE id = ?1
      ''', [1, 'Josepth Doe']);
      batch.execute('''
        SELECT * FROM users
       ''');
      final result = await batch.apply();
      final changeset = await crdt.getChangeset();
      expect(batch.length, equals(5));
      expect(result.length, equals(5));
      expect(changeset['users']!.length, equals(2));
      expect(changeset['users']!.first['hlc'], isNotNull);
    });

    // test the batch within a transaction
    test('batch transaction', () async {
      await crdt.transaction((txn) async {
        await _insertUser(txn, 1, 'John Doe');
        await _insertUser(txn, 2, 'Jane Doe');

        final batch = txn.batch();
        batch.execute('''
      UPDATE users SET name = ?2
      WHERE id = ?1
      ''', [1, 'Josepth Doe']);
        batch.execute('''
        SELECT * FROM users
       ''');
        final resultBatch = await batch.apply();
        expect(batch.length, equals(2));
        expect(resultBatch.length, equals(2));
        expect(resultBatch, equals([1, null]));
      });
      final result = await crdt.query('SELECT * FROM users');
      expect(result.length, equals(2));
      expect(result.first['hlc'], equals(result.last['hlc']));
      expect(result.first['name'], equals('Josepth Doe'));
    });
  });

  group('Synchroflite batch watch', () {
    late Synchroflite crdt;

    setUp(() async {
      crdt = await Synchroflite.openInMemory(
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
              )
              ''');
          await db.execute('''
            CREATE TABLE purchases (
              id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              price REAL NOT NULL,
              PRIMARY KEY (id)
            )
          ''');
        },
      );
    });

    // test the onChange callback for batch commit
    test('batch commit onChange', () async {
      final stream = crdt.watch('SELECT * FROM users');
      final streamTest = expectLater(
          stream,
          emitsInOrder([
            [],
            (List<Map<String, Object?>> e) =>
                e.first['name'] == 'Joseph Doe' && e.last['name'] == 'Jane Doe',
          ]));
      final batch = crdt.batch();
      batch.execute('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [1, 'John Doe']);
      batch.execute('SELECT 1');
      batch.execute('''
        INSERT INTO users (id, name)
        VALUES (?1, ?2)
      ''', [2, 'Jane Doe']);
      batch.execute('''
      UPDATE users SET name = ?2
      WHERE id = ?1
      ''', [1, 'Joseph Doe']);
      batch.rawInsert('''
        INSERT INTO purchases (id, user_id, price)
        VALUES (?1, ?2, ?3)
      ''', [1, 1, 12.3]);
      batch.execute('''
        SELECT * FROM users
       ''');
      await batch.commit();
      final changeset = await crdt.getChangeset();
      await streamTest;

      expect(batch.length, equals(6));
      expect(changeset['users']!.length, equals(2));
      expect(changeset['purchases']!.length, equals(1));
      expect(changeset['users']!.first['hlc'],
          equals(changeset['purchases']!.last['hlc']));
      expect(changeset['users']!.first['hlc'], isNotNull);
    });

    test('batch within transaction onChange', () async {
      final stream = crdt.watch('SELECT * FROM users');
      final streamTest = expectLater(
          stream,
          emitsInOrder([
            [],
            (List<Map<String, Object?>> e) =>
                e.first['name'] == 'Josepth Doe' &&
                e.last['name'] == 'Jane Doe',
          ]));

      await crdt.transaction((txn) async {
        await _insertUser(txn, 1, 'John Doe');
        await _insertUser(txn, 2, 'Jane Doe');

        final batch = txn.batch();
        batch.execute('''
      UPDATE users SET name = ?2
      WHERE id = ?1
      ''', [1, 'Josepth Doe']);
        batch.execute('''
        SELECT * FROM users
       ''');
        final resultBatch = await batch.apply();
        expect(batch.length, equals(2));
        expect(resultBatch.length, equals(2));
        expect(resultBatch, equals([1, null]));
      });
      final result = await crdt.query('SELECT * FROM users');
      expect(result.length, equals(2));
      expect(result.first['hlc'], equals(result.last['hlc']));
      expect(result.first['name'], equals('Josepth Doe'));
      await streamTest;
    });
  });

  group('Special queries', () {
    late Synchroflite crdt;

    setUp(() async {
      crdt = await Synchroflite.openInMemory(
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
              )
              ''');
        },
      );
    });

    test('SELECT 1 case', () async {
      final result = await crdt.query('SELECT 1');
      expect(result.first['1'], 1);
    });

    test('PRAGMA queries', () async {
      final result = await crdt.query('PRAGMA table_info(users)');
      expect(result.first['name'], 'id');
    });

    test('sqlite_schema queries', () async {
      final result = await crdt.query('''
        SELECT name FROM sqlite_schema
        WHERE type ='table' AND name NOT LIKE 'sqlite_%'
      ''');
      expect(result.first['name'], 'users');
    });
  });

  /// Note these tests here relate to a functionality that was implemented
  /// so that the Drift migrations could be used with Synchroflite
  /// 99.9% of the time it is an incredibly bad idea to fiddle with CRDT columns yourself
  /// Do not trust yourself to not break the CRDT.
  /// But just in case you really want to blow your foot off, here is the sawed off shotgun.
  group('statements with CRDT columns  provided instead of appended at the end',
      () {
    late Synchroflite crdt;

    setUp(() async {
      crdt = await Synchroflite.openInMemory(
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
              )
              ''');
        },
      );
    });

    test('create table with CRDT columns specified', () async {
      await crdt.execute('''
            CREATE TABLE relations (
              id INTEGER NOT NULL,
              user TEXT,
              hlc TEXT NOT NULL,
              node_id TEXT NOT NULL,
              modified TEXT NOT NULL,
              PRIMARY KEY (id)
              )
              ''');
    });

    test('select with CRDT columns specified', () async {
      // insert user
      await crdt.execute('''
            INSERT INTO users (id, name)
            VALUES (?1, ?2)
          ''', [1, 'John Doe']);
      final result = await crdt.query('''
            SELECT id, name, hlc, node_id, modified FROM users
              ''');
      expect(result.first['name'], equals('John Doe'));
    });

    test('insert with CRDT columns specified', () async {
      final hlc = Hlc.now('test_node_id');
      await crdt.execute('''
            INSERT INTO users (id, name, hlc, node_id, modified)
            VALUES (?1, ?2, ?3, ?4, ?5)
          ''', [1, 'John Doe', hlc.toString(), hlc.nodeId, hlc.toString()]);
      final result = await crdt.query('''
            SELECT id, name, hlc, node_id, modified FROM users
              ''');
      expect(result.first['name'], equals('John Doe'));
    });

    test('upsert with CRDT columns specified', () async {
      var hlc = Hlc.now('test_node_id');
      await crdt.execute('''
            INSERT INTO users (id, name, hlc, node_id, modified)
            VALUES (?1, ?2, ?3, ?4, ?5)
          ''', [1, 'John Doe', hlc.toString(), hlc.nodeId, hlc.toString()]);

      // upsert
      hlc.increment();
      await crdt.execute('''
            INSERT INTO users (id, name, hlc, node_id, modified)
              VALUES (?1, ?2, ?3, ?4, ?5)
              ON CONFLICT (id) DO UPDATE SET 
                name = excluded.name, 
                hlc = excluded.hlc, 
                node_id = excluded.node_id, 
                modified = excluded.modified
          ''', [1, 'Jane Doe', hlc.toString(), hlc.nodeId, hlc.toString()]);

      final result = await crdt.query('''
            SELECT id, name, hlc, node_id, modified FROM users
              ''');
      expect(result.first['name'], equals('Jane Doe'));
    });

    test('update with CRDT columns specified', () async {
      var hlc = Hlc.now('test_node_id');
      await crdt.execute('''
                INSERT INTO users (id, name, hlc, node_id, modified)
                VALUES (?1, ?2, ?3, ?4, ?5)
              ''', [1, 'John Doe', hlc.toString(), hlc.nodeId, hlc.toString()]);

      hlc = hlc.increment();
      await crdt.execute('''
                UPDATE users SET name = ?2, hlc = ?3, node_id = ?4, modified = ?5
                WHERE id = ?1
              ''', [1, 'Jane Doe', hlc.toString(), hlc.nodeId, hlc.toString()]);
      final result = await crdt.query('''
                SELECT id, name, hlc, node_id, modified FROM users
                  ''');
      expect(result.first['name'], equals('Jane Doe'));
    });

    test('update with CRDT columns specified and multiple where terms',
        () async {
      var hlc = Hlc.now('test_node_id');
      await crdt.execute('''
                INSERT INTO users (id, name, hlc, node_id, modified)
                VALUES (?1, ?2, ?3, ?4, ?5)
              ''', [1, 'John Doe', hlc.toString(), hlc.nodeId, hlc.toString()]);
      hlc = hlc.increment();
      await crdt.execute('''
                INSERT INTO users (id, name, hlc, node_id, modified)
                VALUES (?1, ?2, ?3, ?4, ?5)
              ''',
          [2, 'James Roe', hlc.toString(), hlc.nodeId, hlc.toString()]);
      hlc = hlc.increment();
      await crdt.execute('''
                UPDATE users SET name = ?2, hlc = ?3, node_id = ?4, modified = ?5
                WHERE id = ?1 AND name = ?6
              ''', [
        1,
        'Jane Doe',
        hlc.toString(),
        hlc.nodeId,
        hlc.toString(),
        'John Doe'
      ]);
      final result = await crdt.query('''
                SELECT id, name, hlc, node_id, modified FROM users
                  ''');
      expect(result.first['name'], equals('Jane Doe'));
    });
  });

  group('Special cases', () {
    late Synchroflite crdt;

    setUp(() async {
      crdt = await Synchroflite.openInMemory(
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
              )
              ''');
        },
      );
    });

    test('insert select', () async {
      await crdt.execute('''
            INSERT INTO users (id, name)
            VALUES (?1, ?2)
          ''', [1, 'John Doe']);
      await crdt.execute('''
            CREATE TABLE users_tmp_copy (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
              )
              ''');
      await crdt.execute('''
            INSERT INTO users_tmp_copy (id, name, hlc, node_id, modified)
            SELECT id, name, hlc, node_id, modified FROM users
              ''');
      final result = await crdt.query('''
          SELECT id, name FROM users_tmp_copy
            ''');
      expect(result.first['name'], equals('John Doe'));
    }, timeout: Timeout(Duration(minutes: 10)));

    test('insert select without CRDT columns', () async {
      await crdt.execute('''
            INSERT INTO users (id, name)
            VALUES (?1, ?2)
          ''', [1, 'John Doe']);
      await crdt.execute('''
            CREATE TABLE users_tmp_copy (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
              )
              ''');
      await crdt.execute('''
            INSERT INTO users_tmp_copy (id, name, hlc, node_id, modified)
            SELECT id, name  FROM users
              ''');
      final result = await crdt.query('''
          SELECT id, name FROM users_tmp_copy
            ''');
      expect(result.first['name'], equals('John Doe'));
    });
  });

  group('regressions', () {
    late Synchroflite crdt;

    setUp(() async {
      crdt = await Synchroflite.openInMemory(
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
              )
              ''');
        },
      );
    });

    test('upsert with duplicate parameters', () async {
      // insert user
      await crdt.execute('''
            INSERT INTO "users" ("id", "name") 
            VALUES (?, ?) 
              ON CONFLICT("id") 
            DO UPDATE 
              SET "id" = ?, "name" = ?
          ''', [1, 'John Doe', 1, 'John Doe']);
      final result = await crdt.query('''
            SELECT id, name, hlc, node_id, modified FROM users
              ''');
      expect(result.first['name'], equals('John Doe'));
    });

    test('do not delete previously deleted records', () async {
      // Previously deleted records should not get modified
      // when a delete is called with the same criteria
      // the modified column should not be updated
      await crdt.execute('''
            INSERT INTO "users" ("id", "name") 
            VALUES (?, ?) 
              ON CONFLICT("id") 
            DO UPDATE 
              SET "id" = ?, "name" = ?
          ''', [1, 'John Doe', 1, 'John Doe']);
      await crdt.execute('''
            DELETE FROM "users" 
            WHERE "id" = ?
          ''', [1]);
      final changeset_2 = await crdt.getChangeset();
      await crdt.execute('''
            DELETE FROM "users" 
            WHERE "id" = ?
          ''', [1]);
      final changeset_3 = await crdt.getChangeset();
      expect(changeset_2['users']!.first, equals(changeset_3['users']!.first));
    });
  });
}

Future<void> _insertUser(TimestampedCrdt crdt, int id, String name) =>
    crdt.execute('''
      INSERT INTO users (id, name)
      VALUES (?1, ?2)
    ''', [id, name]);

Future<void> _updateUser(TimestampedCrdt crdt, int id, String name) =>
    crdt.execute('''
      UPDATE users SET name = ?2
      WHERE id = ?1
    ''', [id, name]);

Future<void> _deleteUser(TimestampedCrdt crdt, int id) =>
    crdt.execute('DELETE FROM users WHERE id = ?1', [id]);

Future<void> _insertPurchase(
        TimestampedCrdt crdt, int id, int userId, double price) =>
    crdt.execute('''
      INSERT INTO purchases (id, user_id, price)
      VALUES (?1, ?2, ?3)
    ''', [id, userId, price]);
