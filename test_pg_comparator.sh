#! /bin/bash
#
# $Id: test_pg_comparator.sh 187 2004-08-25 09:41:15Z coelho $
#
# test pg_comparator
#

rows=1000001
name=foo
seed=1

while getopts "s:r:n:" opt
do
  case $opt in
      s) seed=$OPTARG ;;
      n) name=$OPTARG ;;
      r) rows=$OPTARG ;;
      *) echo "usage: $0 [-s seed] [-n name] [-r rows] args..."; exit 1;;
  esac
done

shift $(( $OPTIND - 1 ))

echo "BUILD size=$rows name=$name seed=$seed $(date)"

{ 
    # create and fill tables
    echo "DROP TABLE ${name}1,${name}2;"
    rand_table.pl --table ${name}1 --seed $seed --rows $rows --columns=c1,c2
    rand_table.pl --table ${name}2 --seed $seed --rows $rows --columns=c3,c4

    # simple update
    echo "UPDATE ${name}1 SET c1='bouh' WHERE id=$(($rows/8));"

    # null different update
    echo "UPDATE ${name}1 SET c2=NULL WHERE id=$(($rows*2/3));"
    echo "UPDATE ${name}2 SET c3=NULL WHERE id=$(($rows*2/3));"

    # delete
    echo "DELETE FROM ${name}1 WHERE id=$(($rows/3));"

    # insert
    echo "DELETE FROM ${name}2 WHERE id=$(($rows*9/11));"
} | psql

[ "$1" = 'load' ] && exit

echo "COMPARE $(date)"
time pg_comparator "$@" /${name}1?id:c1,c2 /${name}2?id:c3,c4

echo "CLEAN $(date)"
echo "DROP TABLE ${name}1, ${name}2;" | psql

echo "DONE $(date)"
