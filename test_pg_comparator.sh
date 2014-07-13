#! /bin/bash
#
# $Id: test_pg_comparator.sh 1499 2014-07-12 18:03:30Z coelho $
#
# ./test_pg_comparator.sh -r 100 \
#    -a fabien:mypassword@localhost:port -- \
#    --synchronize --stats --do-it

PATH=$PATH:.

# detect pipeline errors
set -o pipefail

# default values
rows=1000 name=foo width=5 nkey=0 ncol=2 base1=$USER base2=$USER
seed=$(date +%s)

# operations to perform
create= modify= cmp=

# misc
auth= keep= eng= debug= trigger= empty1= empty2= nullkey=

# default diffs
upt=2 ins=2 del=2 nul=2 rev=1 notnull= total=

# process options
while [[ "$1" && "$1" != '--' ]] ; do
  opt="$1"
  shift
  case $opt in
    # connection
    --auth|-a) auth1="$1" ; auth2="$1" ; shift ;;
    --auth1|-1) auth1="$1" ; shift ;;
    --auth2|-2) auth2="$1" ; shift ;;
    --base|-b) base1="$1"; base2="$1" ; shift ;;
    --base1|-b1) base1="$1" ; shift ;;
    --base2|-b2) base2="$1" ; shift ;;
    --name|-n) name="$1" ; shift ;;
    # data generation
    --seed|-s) seed=$1 ; shift ;;
    --rows|-r) rows=$1 ; shift ;;
    --key|-k) nkey=$1 ; shift ;;
    --col|-c) ncol=$1 ; shift ;;
    --width|-w) width=$1 ; shift ;; # about 17 (up to 18) chars per w
    --engine|-e) eng=$1 ; shift ;; # for mysql only
    --null-key|-nk) nullkey=' --null-key' ;;
    --tuple-trigger|--tt|-T) trigger+=' --tc=tup_cs --no-null' ;;
    --key-trigger|--kt) trigger+=' --kc=key_cs --no-null' ;;
    --empty-1|--e1) empty1=1 ;;
    --empty-2|--e2) empty2=1 ;;
    # diffs
    --total|-t) total=$1 ins=0 upt=0 del=0 ; shift ;;
    --update|--upd|-u) upt=$1 ; shift ;;
    --insert|--ins|-i) ins=$1 ; shift ;;
    --delete|--del|-d) del=$1 ; shift ;;
    --null|-l) nul=$1 ; shift ;;
    --reverse|-v) rev=$1 ; shift ;;
    --not-null|--notnull|--nn|-N) notnull=1 ;;
    # cleanup
    --keep|-K) keep=1 ;;
    --debug|-D) debug=1 ;;
    # ops
    --create|-C) create=1 ;;
    --modify|-M) modify=1 ;;
    --run|-R) cmp=1 ;;
    # help
    --help|-h|*)
      echo -e \
	"usage: $0 [options] [-- pg-comparator-options...]\n" \
	" -a auth: authority string for connection\n" \
	" -b[12] base: database name\n" \
	" -n name: table prefix\n" \
	" -s seed: random generator seed, default is seconds since 1970\n" \
	" -r rows: vertical size\n" \
	" -k keys: number of additionnal key columns\n" \
	" -c cols: number of data columns\n" \
	" -w width: horizontal size (17 chars per unit)\n" \
	" -t total: number of differences to generate\n" \
        " -K: keep resulting table\n" \
        " -C: create\n" \
	" -M: modify\n" \
	" -R: run"
      exit 1
      ;;
  esac
done

[ "$1" = '--' ] && shift

# unset total specification if empty1 or empty2
[ "$empty1$empty2" ] && total=0 ins=0 upt=0 del=0 rev=0 nul=0

# if total is set, share upt/ins/del/nul/rev to match it
if [ "$total" ] ; then
  if [ $ncol -eq 0 ] ; then
    # fix if key only, no updates!
    [ $ins -eq 0 ] && ins=$(($total/2))
    nul=0 upt=0 rev=0
    del=$(($total-$ins))
  else
    [ $ins -eq 0 ] && ins=$(($total/4))
    rem=$(($total-$ins))
    del=$(($rem/2)) nul=$del rev=0
    [ "$notnull" ] && nul=0
    upt=$(($total - $ins - $del - 2 * $rev - $nul))
  fi
else
  # fix if key only, no updates
  [ $ncol -eq 0 ] && upt=0 rev=0 nul=0
  [ "$notnull" ] && nul=0
  # compute total
  total=$(($upt+$ins+$del+$nul+$rev))
fi

if [ $nkey -gt 0 -a $rev -gt 0 ] ; then
  echo "FIX BUG in reverse generation for non simple keys!" >&2
  exit 1
fi

expect=$(($upt+$ins+$del+$nul+2*$rev))

if [ $expect -ge $rows -a ! "$empty1" -a ! "$empty2" ] ; then
  echo "FIX BUG too many differences to generate $expect/$rows" >&2
  exit 2
fi

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

# generate table creation
function create_table()
{
  #echo "$@" >&2
  local db=$1 name=$2 seed=$3 rows=$4 keys=$5 cols=$6 width=$7 eng=$8
  shift 8

  local engine=
  [ $db = 'mysql' -a "$eng" ] && engine="--engine $eng"

  # create and fill tables
  rand_table.pl --$db --table ${name} --seed $seed --rows $rows \
	--keys=$keys --columns=$cols --width $width --transaction \
        $engine $trigger $nullkey

  return 0;
}

# generate specified table modifications
function change_table()
{
  local db=$1 name=$2 ncol=$3 rows=$4 keys=$5 cols=$6 \
      upt=$7 ins=$8 del=$9 nul=${10} rev=${11}
  shift 11

  local total=$(($upt + $ins + $del + $nul + $rev))
  local i=1 c=0 div=$(($total+1))

  [ "$del" = 0 ] || { echo "expecting del=0" >&1 ; exit 1 ; }

  # build key,col,attribute,list
  local kc=''
  [ "$keys" ] && kc=",$keys"
  if [ "$cols" ] ; then
    if [ "$kc" ] ; then
      kc+=",$cols";
    else
      kc=",$cols";
    fi
  fi

  # change table in a transaction
  if [ $db = 'firebird' ] ; then
    echo "COMMIT;"
  else
    echo "BEGIN;"
  fi

  while [ $i -le $total ]
  do
    # id about proportional to i-th fraction
    local id=$(($rows*$i/$div))
    #echo "id=$id" >&2
    # special case for --null-key
    if [ $i -eq 1 -a "$nullkey" ] ; then
      let upt--
      [ "$debug" ] && { echo "# update id IS NULL" >&2 ; }
      echo "UPDATE $name SET b0='id is null' WHERE id IS NULL;"
    elif [[ $upt -ne 0 ]] ; then
      let upt-- c=++$c%$ncol
      [ "$debug" ] && { echo "# update id=$id" >&2 ; }
      echo "UPDATE $name SET b$c='bouh' WHERE id=$id;"
    #elif [[ $del -ne 0 ]] ; then
    #  # new tuples are inserted at the end, by copying some contents...
    #  local nid=$(($rows+$del))
    #  let del--
    #  [ "$debug" ] && { echo "# delete id=$nid" >&2 ; }
    #  echo "INSERT INTO $name(id$kc)"
    #  echo "SELECT $nid$kc FROM $name WHERE id=$id;"
    elif [[ $ins -ne 0 ]] ; then
      # inserts are just done by deleting some rows
      let ins--
      [ "$debug" ] && { echo "# insert id=$id" >&2 ; }
      echo "DELETE FROM $name WHERE id=$id;"
    elif [[ $nul -ne 0 ]] ; then
      # set a NULL value in a column
      let nul-- c=++$c%$ncol
      [ "$debug" ] && { echo "# null id=$id" >&2 ; }
      echo "UPDATE $name SET b$c=NULL WHERE id=$id;"
    elif [[ $rev -ne 0 ]] ; then
      # exchange two tuples
      # BUG: IT GENERATES INSERT/DELETE INSTEAD OF UPDATE FOR MULTIPLE KEYS
      let rev--
      id2=$(($id + 1))
      [ "$debug" ] && { echo "# reverse id=$id id2=$id2" >&2 ; }
      echo "UPDATE $name SET id=0 WHERE id=$id;"
      echo "UPDATE $name SET id=$id WHERE id=$id2;"
      echo "UPDATE $name SET id=$id2 WHERE id=0;"
    fi
    let i++
  done

  echo "COMMIT;"

  [ $db = 'pgsql' ] && echo "VACUUM FULL ANALYZE ${name};"

  return 0
}

# pgsql://calvin:hobbes@[host[:port]]
function parse_conn()
{
  local auth=$1 base=$2
  local port=5432
  local host=${1//*@/}
  if [[ $host == *:* ]] ; then
    port=${host//*:}
    host=${host//:*}
  fi
  local user=${1//*:\/\//}
  user=${user//:*/}
  local pass=${1//@*/}
  pass=${pass//*:/}
  if [[ $auth == pgsql://* ]]
  then
    if [ "$host" ] ; then
      echo "psql 'host=$host port=$port user=$user password=$pass dbname=$base'"
    else
      echo "psql 'user=$user password=$pass dbname=$base'"
    fi
  elif [[ $auth == mysql://* ]]
  then
    if [ "$host" ] ; then
      echo "mysql --host=$host --port=$port --user=$user --pass=$pass --database=$base"
    else
      echo "mysql --user=$user --pass=$pass --database=$base"
    fi
  elif [[ $auth == sqlite://* ]] ; then
      echo "sqlite3 $base"
  elif [[ $auth == firebird://* ]] ; then
    # debian/ubuntu use 'isql-fb' instead of 'isql'
    echo "isql-fb -user $user -password $pass"
  else
    echo "invalid authority $auth" >&2
    exit 1
  fi
}

function database_name()
{
  echo ${1//:*/}
}

sql1=$(parse_conn $auth1 $base1)
db1=$(database_name $auth1)
#echo "sql1=$sql1 db1=$db1"

sql2=$(parse_conn $auth2 $base2)
db2=$(database_name $auth2)
#echo "sql2=$sql2 db2=$db2"

[ "$create" ] &&
{
  if [ "$empty1" ] ; then rows1=0 ; else rows1=$rows ; fi
  # create and fill tables which will be identical, because same seed
  {
    [ $db1 = 'firebird' ] && echo "CONNECT '$base1';"
    create_table $db1 ${name}1 $seed $rows1 "$key1" "$col1" $width $eng
  } | eval $sql1 &
  wait=$!
  # we wait for sqlite, because the same database cannot be locked twice
  if [ $db1 = 'sqlite' ] ; then
    wait $wait
    wait=
  fi
  if [ "$empty2" ] ; then rows2=0 ; else rows2=$rows ; fi
  # however some more fill-in is done to make space for delete
  {
    [ $db2 = 'firebird' ] && echo "CONNECT '$base2';"
    create_table $db2 ${name}2 $seed $(($rows2+$del)) \
      "$key2" "$col2" $width $eng
  } | eval $sql2
  status=$?
  if [ $status -ne 0 ] ; then
    echo "table 2 generation failed: $status" >&2
    exit 1
  fi
}

[ "$modify" -a ! "$empty1$empty2" ] &&
{
  # modify the second table
  {
    [ $db2 = 'firebird' ] && echo "CONNECT '$base2';"
    change_table $db2 ${name}2 $ncol $rows "$key2" "$col2" \
      $upt $ins 0 $nul $rev
  } | tee /dev/tty | eval $sql2
  status=$?
  if [ $status -ne 0 ] ; then
    echo "table 2 changes failed: $status" >&2
    exit 2
  fi
}

echo "EXPECTING $msg"

if [ "$wait" ] ; then
  wait $wait
  status=$?
  if [ $status -ne 0 ] ; then
    echo "table 1 generation failed: $status" >&2
    exit 3
  fi
fi

status=0
if [ "$cmp" ] ; then
  if [ $nkey -gt 0 ] ; then
    key1="id,$key1" key2="id,$key2"
  else
    key1='id' key2='id'
  fi
  echo "COMPARE $(date)"
  # use implicit authority in second connection
  echo pg_comparator -e $expect "$@" \
    $auth1/${base1}/${name}1?$key1:$col1 $auth2/${base2}/${name}2?$key2:$col2
  time pg_comparator -e $expect "$@" \
    $auth1/${base1}/${name}1?$key1:$col1 $auth2/${base2}/${name}2?$key2:$col2
  status=$?
fi

# hmmm... pgsql specific
[ "$keep" ] ||
{
  echo "CLEAN $(date)"
  echo "DROP TABLE ${name}1, ${name}2;" | psql
}

echo "DONE $(date)"
exit $status
