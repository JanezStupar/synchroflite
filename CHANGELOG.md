## 0.1.2
Records flagged as deleted are not modified again when a delete query targeting them is executed.

## 0.1.1
Improve handling of special queries that should not get transformed by CRDT.
Run Execute through standalone implementation.
Statement transformation methods account for potentially existing CRDT arguments in queries.
Implement support for INSERT .. SELECT FROM .. statements.
Fix a regression where the user provides duplicated SQL params in a valid fashion.

## 0.1.0

Forked from `sqlite_crdt`. Implements some sqflite specific features for the purpose of using it in a Drift plugin.

