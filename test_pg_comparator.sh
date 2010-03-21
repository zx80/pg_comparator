#! /bin/bash
#
# $Id: test_pg_comparator.sh 518 2010-03-21 10:39:01Z fabien $
#
# ./test_pg_comparator.sh -r 100 \
#    -a fabien:mypassword@localhost -- \
#    --synchronize --stats --do-it

PATH=$PATH:.

# default values
rows=1001 name=foo seed=1 width=5 ncol=2 base=$USER auth= keep=

# default diffs
upt=2 ins=2 del=2 nul=2 rev=1 total=

while getopts "s:r:n:w:a:b:c:u:i:d:l:v:t:kh" opt
do
  case $opt in
    # connection
    a) auth=$OPTARG ;;
    b) base=$OPTARG ;;
    n) name=$OPTARG ;;
    # data generation
    s) seed=$OPTARG ;;
    r) rows=$OPTARG ;;
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
    k) keep=1 ;;
    # help
    h|*)
      echo -e \
	"usage: $0 [options] [-- pg-comparator-options...]\n" \
	" -s seed: random generator seed\n" \
	" -n name: table prefix\n" \
	" -r rows: vertical size\n" \
	" -w width: horizontal size (17 chars per unit)\n" \
	" -a auth: authority part in the test url\n" \
        " -k: keep resulting table"
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
	--columns=$col1 --width $width
    rand_table.pl --table ${name}2 --seed $seed --rows $rows \
	--columns=$col2 --width $width

    echo "BEGIN;"

    i=1 div=$(($total+1))
    while [ $i -le $total ]
    do
      id=$(($rows*$i/$div))
      if [[ $upt -ne 0 ]] ; then
	let upt--
	echo "UPDATE ${name}1 SET a0='bouh' WHERE id=$id;"
      elif [[ $ins -ne 0 ]] ; then
	let ins--
	echo "DELETE FROM ${name}2 WHERE id=$id;"
      elif [[ $del -ne 0 ]] ; then
	let del--
	echo "DELETE FROM ${name}1 WHERE id=$id;"
      elif [[ $nul -ne 0 ]] ; then
	let nul--
	echo "UPDATE ${name}1 SET a0=NULL WHERE id=$id;"
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

echo "COMPARE $(date)"
echo pg_comparator -e $expect "$@" \
  $auth/${base}/${name}1?id:$col1 /${base}/${name}2?id:$col2
time pg_comparator -e $expect "$@" \
  $auth/${base}/${name}1?id:$col1 /${base}/${name}2?id:$col2

echo "EXPECTING $msg"

[ "$keep" ] ||
{
  echo "CLEAN $(date)"
  echo "DROP TABLE ${name}1, ${name}2;" | psql
}

echo "DONE $(date)"
