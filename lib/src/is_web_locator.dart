// Copyright 2023 Daniel Cachapa
// This file is copied from sqlite_crdt package:
// https://github.com/cachapa/sqlite_crdt
// SPDX-License-Identifier: Apache-2.0
import 'is_web_io.dart' if (dart.library.html) 'is_web_web.dart' as test;

bool get sqliteCrdtIsWeb => test.sqliteCrdtIsWeb;

bool get sqliteCrdtIsLinux => test.sqliteCrdtIsLinux;
