#!/bin/bash
#
# $Id: test_collision.sh 1473 2013-03-07 20:41:38Z coelho $
#

auth=$1 base=$2

psql $base <<EOF
DROP TABLE test1, test2;
CREATE TABLE test1 (id integer PRIMARY KEY, content text);
CREATE TABLE test2 (id integer PRIMARY KEY, content text);
INSERT INTO test1 (id, content) VALUES
  (536636, '74e09d3896d47b6dcc83e2d27484d50e'),
  (838939, '9f279730ed8658f642597f922064402c'),
  (4672156, 'e37dc45df4d341c15c4f709666107466');
INSERT INTO test2 (id, content) VALUES
  (536636, '74e09d3896d47b6dcc83e2d27484d50e'),
  (838939, '9f279730ed8658f642597f922064402c'),
  (4700943, 'acc5bfbbf8e12b402e0672391d76e1ef');
EOF

./pg_comparator --expect=2 --checksum-function=md5 \
  "$auth/$base/test1?id:content" \
  "$auth/$base/test2" || exit 1

./pg_comparator --expect=2 --checksum-function=md5 \
  "$auth/$base/test2?id:content" \
  "$auth/$base/test1" || exit 2

psql test <<EOF
DROP TABLE test1, test2;
EOF
