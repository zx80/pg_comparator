#! /bin/bash
# $Id: test_pg_comparator.sh 429 2008-05-23 15:27:45Z fabien $

rows=1000001
name=foo
seed=1
width=5
base=$USER

while getopts "s:r:n:w:" opt
do
  case $opt in
      s) seed=$OPTARG ;;
      n) name=$OPTARG ;;
      r) rows=$OPTARG ;;
      w) width=$OPTARG ;; # about 17 chars per w
      *) 
	  echo "usage: $0 [-s seed] [-n name] [-r rows] [-w width] args..."; 
	  exit 1
	  ;;
  esac
done

shift $(( $OPTIND - 1 ))

echo "BUILD size=$rows name=$name seed=$seed width=$width $(date)"

{ 
    # create and fill tables
    echo "DROP TABLE ${name}1,${name}2;"
    rand_table.pl --table ${name}1 --seed $seed --rows $rows \
	--columns=c1,c2 --width $width
    rand_table.pl --table ${name}2 --seed $seed --rows $rows \
	--columns=c3,c4 --width $width

    echo "BEGIN;"

    # simple update
    echo "UPDATE ${name}1 SET c1='bouh' WHERE id=$(($rows/8));"

    # null different update...
    echo "UPDATE ${name}1 SET c2=NULL WHERE id=$(($rows*2/3));"
    echo "UPDATE ${name}2 SET c3=NULL WHERE id=$(($rows*2/3));"

    # exchange values in same idc? uneasy to test...
    idx=$(($rows/5)) idy=$(($idx+1))
    echo "UPDATE ${name}2 SET id=0 WHERE id=$idx;"
    echo "UPDATE ${name}2 SET id=$idx WHERE id=$idy;"
    echo "UPDATE ${name}2 SET id=$idy WHERE id=0;"

    # delete
    echo "DELETE FROM ${name}1 WHERE id=$(($rows/3));"

    # insert
    echo "DELETE FROM ${name}2 WHERE id=$(($rows*9/11));"

    echo "COMMIT;"
    echo "ANALYZE ${name}1;"
    echo "ANALYZE ${name}2;"
} | psql $base

[ "$1" = 'load' ] && exit

echo "COMPARE $(date)"
echo pg_comparator "$@" /${base}/${name}1?id:c1,c2 /${base}/${name}2?id:c3,c4
time pg_comparator "$@" /${base}/${name}1?id:c1,c2 /${base}/${name}2?id:c3,c4

echo "EXPECTING 4 updates, 1 insert, 1 delete"
echo "CLEAN $(date)"
echo "DROP TABLE ${name}1, ${name}2;" | psql

echo "DONE $(date)"
