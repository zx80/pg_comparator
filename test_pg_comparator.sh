#! /bin/bash
#
# $Id: test_pg_comparator.sh 549 2010-03-21 18:11:51Z fabien $
#
# ./test_pg_comparator.sh -r 100 \
#    -a fabien:mypassword@localhost -- \
#    --synchronize --stats --do-it

PATH=$PATH:.

# default values
rows=1001 name=foo seed=1 width=5 nkey=0 ncol=2 base=$USER auth= keep= cmp=1

# default diffs
upt=2 ins=2 del=2 nul=2 rev=1 total=

while getopts "s:r:n:w:a:b:k:c:u:i:d:l:v:t:KNh" opt
do
  case $opt in
    # connection
    a) auth=$OPTARG ;;
    b) base=$OPTARG ;;
    n) name=$OPTARG ;;
    # data generation
    s) seed=$OPTARG ;;
    r) rows=$OPTARG ;;
    k) nkey=$OPTARG ;;
    c) ncol=$OPTARG ;;
    w) width=$OPTARG ;; # about 17 chars per w
    # diffs
    t) total=$OPTARG ;;
    u) upt=$OPTARG ;;
    i) ins=$OPTARG ;;
    d) del=$OPTARG ;;
    l) nul=$OPTARG ;;
    v) rev=$OPTARG ;;
    # cleanup
    K) keep=1 ;;
    N) cmp= ;;
    # help
    h|*)
      echo -e \
	"usage: $0 [options] [-- pg-comparator-options...]\n" \
	" -a auth: authority string for connection\n" \
	" -b base: database name\n" \
	" -n name: table prefix\n" \
	" -s seed: random generator seed\n" \
	" -r rows: vertical size\n" \
	" -k keys: number of additionnal key columns\n" \
	" -c cols: number of data columns\n" \
	" -w width: horizontal size (17 chars per unit)\n" \
	" -t total: number of differences to generate\n" \
        " -K: keep resulting table\n" \
        " -N: do not compare"
      exit 1
      ;;
  esac
done

shift $(( $OPTIND - 1 ))

# if total is set, share upt/ins/del/nul/rev to match it
if [ "$total" ] ; then
  ins=$(($total/4))
  nul=$ins
  upt=$ins
  rev=0
  # fix if key only, no updates
  [ $ncol -eq 0 ] && upt=0 rev=0 nul=0
  del=$(($total - $ins - $upt - $rev - $nul))
else
  # fix if key only, no updates
  [ $ncol -eq 0 ] && upt=0 rev=0 nul=0
  # compute total
  total=$(($upt+$ins+$del+$nul+$rev))
fi

expect=$(($upt+$ins+$del+$nul+2*$rev))

# result
msg="u=$upt i=$ins d=$del n=$nul r=$rev: "
msg+="$ins INSERT, $del DELETE, $(($upt+$nul+2*$rev)) UPDATE"

# build additionnal key names
key1= key2=
if [ $nkey -gt 0 ] ; then
  let i=$nkey
  while [ $i -gt 0 ] ; do
    let i--
    key1+=",k$i"
    key2+=",l$i"
  done
fi
key1=${key1#,}
key2=${key2#,}

# build column names
col1= col2=
if [ $ncol -gt 0 ] ; then
  let i=$ncol
  while [ $i -gt 0 ] ; do
    let i--
    col1+=",a$i"
    col2+=",b$i"
  done
fi
col1=${col1#,}
col2=${col2#,}

echo "BUILD size=$rows name=$name seed=$seed width=$width $(date)"

{
    # create and fill tables
    echo "DROP TABLE ${name}1,${name}2;"
    rand_table.pl --table ${name}1 --seed $seed --rows $rows \
	--keys=$key1 --columns=$col1 --width $width
    rand_table.pl --table ${name}2 --seed $seed --rows $rows \
	--keys=$key2 --columns=$col2 --width $width

    echo "BEGIN;"

    i=1 c=0 div=$(($total+1))
    while [ $i -le $total ]
    do
      id=$(($rows*$i/$div))
      if [[ $upt -ne 0 ]] ; then
	let upt-- c=++$c%$ncol
	echo "UPDATE ${name}1 SET a$c='bouh' WHERE id=$id;"
      elif [[ $ins -ne 0 ]] ; then
	let ins--
	echo "DELETE FROM ${name}2 WHERE id=$id;"
      elif [[ $del -ne 0 ]] ; then
	let del--
	echo "DELETE FROM ${name}1 WHERE id=$id;"
      elif [[ $nul -ne 0 ]] ; then
	let nul-- c=++$c%$ncol
	echo "UPDATE ${name}1 SET a$c=NULL WHERE id=$id;"
      elif [[ $rev -ne 0 ]] ; then
	let rev--
	id2=$(($id + 1))
	echo "UPDATE ${name}1 SET id=0 WHERE id=$id;"
	echo "UPDATE ${name}1 SET id=$id WHERE id=$id2;"
	echo "UPDATE ${name}1 SET id=$id2 WHERE id=0;"
      fi
      let i++
    done

    echo "COMMIT;"
    echo "ANALYZE ${name}1;"
    echo "ANALYZE ${name}2;"
} | psql $base

[ "$1" = 'load' ] && exit

if [ $nkey -gt 0 ] ; then
  key1="id,$key1" key2="id,$key2"
else
  key1='id' key2='id'
fi

if [ "$cmp" ] ; then
  echo "COMPARE $(date)"
  # use implicit authority in second connection
  echo pg_comparator -e $expect "$@" \
    $auth/${base}/${name}1?$key1:$col1 /${base}/${name}2?$key2:$col2
  time pg_comparator -e $expect "$@" \
    $auth/${base}/${name}1?$key1:$col1 /${base}/${name}2?$key2:$col2
fi

echo "EXPECTING $msg"

[ "$keep" ] ||
{
  echo "CLEAN $(date)"
  echo "DROP TABLE ${name}1, ${name}2;" | psql
}

echo "DONE $(date)"
