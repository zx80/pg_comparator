#! /bin/bash
#
# $Id: test_pg_comparator.sh 1142 2012-08-09 12:23:32Z fabien $
#
# ./test_pg_comparator.sh -r 100 \
#    -a fabien:mypassword@localhost -- \
#    --synchronize --stats --do-it

PATH=$PATH:.

# default values
rows=1000 name=foo seed=1 width=5 nkey=0 ncol=2 base=$USER

# operations to perform
create= modify= cmp=

# misc
auth= keep= eng= debug=

# default diffs
upt=2 ins=2 del=2 nul=2 rev=1 total=

# handle options
while getopts "1:2:s:r:n:w:a:b:k:c:u:i:d:l:e:v:t:DKCMRh" opt
do
  case $opt in
    # connection
    a) auth1=$OPTARG ; auth2=$OPTARG ;;
    1) auth1=$OPTARG ;;
    2) auth2=$OPTARG ;;
    b) base=$OPTARG ;;
    n) name=$OPTARG ;;
    # data generation
    s) seed=$OPTARG ;;
    r) rows=$OPTARG ;;
    k) nkey=$OPTARG ;;
    c) ncol=$OPTARG ;;
    w) width=$OPTARG ;; # about 17 chars per w
    e) eng=$OPTARG ;;
    # diffs
    t) total=$OPTARG ;;
    u) upt=$OPTARG ;;
    i) ins=$OPTARG ;;
    d) del=$OPTARG ;;
    l) nul=$OPTARG ;;
    v) rev=$OPTARG ;;
    # cleanup
    K) keep=1 ;;
    D) debug=1 ;;
    # ops
    C) create=1 ;;
    M) modify=1 ;;
    R) cmp=1 ;;
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
        " -C: create\n" \
	" -M: modify\n" \
	" -R: run"
      exit 1
      ;;
  esac
done

shift $(( $OPTIND - 1 ))

# if total is set, share upt/ins/del/nul/rev to match it
if [ "$total" ] ; then
  if [ $ncol -eq 0 ] ; then
    # fix if key only, no updates!
    ins=$(($total/2))
    nul=0 upt=0 rev=0
    del=$(($total-$ins))
  else
    ins=$(($total/4))
    del=$ins nul=$ins rev=0
    upt=$(($total - $ins - $del - $rev - $nul))
  fi
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

# generate table creation
function create_table()
{
  #echo "$@" >&2
  local db=$1 name=$2 seed=$3 rows=$4 keys=$5 cols=$6 width=$7 eng=$8
  shift 8

  local engine=
  [ $db = 'mysql' -a "$eng" ] && engine="--engine $eng"

  echo "BEGIN;"

  # create and fill tables
  echo "DROP TABLE IF EXISTS ${name};"

  rand_table.pl --$db --table ${name} --seed $seed --rows $rows \
	--keys=$keys --columns=$cols --width $width $engine

  echo "COMMIT;"

  [ $db = 'pgsql' ] && echo "VACUUM FULL ANALYZE ${name};"
}

# generate specified table modifications
function change_table()
{
  local db=$1 name=$2 ncol=$3 rows=$4 keys=$5 cols=$6 \
      upt=$7 ins=$8 del=$9 nul=${10} rev=${11}
  shift 11

  local total=$(($upt + $ins + $del + $nul + $rev))
  local i=1 c=0 div=$(($total+1))

  local kc=''
  [ "$keys" ] && kc=",$keys"
  if [ "$cols" ] ; then
    if [ "$kc" ] ; then
      kc+=",$cols";
    else
      kc=",$cols";
    fi
  fi

  echo "BEGIN;"

  while [ $i -le $total ]
  do
    local id=$(($rows*$i/$div))
    #echo "id=$id" >&2
    if [[ $upt -ne 0 ]] ; then
      let upt-- c=++$c%$ncol
      [ "$debug" ] && { echo "# update id=$id" >> debug.out ; }
      echo "UPDATE $name SET b$c='bouh' WHERE id=$id;"
    elif [[ $del -ne 0 ]] ; then
      local nid=$(($rows+$del))
      let del--
      [ "$debug" ] && { echo "# delete id=$nid" >> debug.out ; }
      echo "INSERT INTO $name(id$kc)"
      echo "SELECT $nid$kc FROM $name WHERE id=$id;"
    elif [[ $ins -ne 0 ]] ; then
      let ins--
      [ "$debug" ] && { echo "# insert id=$id" >> debug.out ; }
      echo "DELETE FROM $name WHERE id=$id;"
    elif [[ $nul -ne 0 ]] ; then
      let nul-- c=++$c%$ncol
      [ "$debug" ] && { echo "# null id=$id" >> debug.out ; }
      echo "UPDATE $name SET b$c=NULL WHERE id=$id;"
    elif [[ $rev -ne 0 ]] ; then
      let rev--
      id2=$(($id + 1))
      [ "$debug" ] && { echo "# reverse id=$id id2=$id2" >> debug.out ; }
      echo "UPDATE $name SET id=0 WHERE id=$id;"
      echo "UPDATE $name SET id=$id WHERE id=$id2;"
      echo "UPDATE $name SET id=$id2 WHERE id=0;"
    fi
    let i++
  done

  echo "COMMIT;"

  [ $db = 'pgsql' ] && echo "VACUUM FULL ANALYZE ${name};"
}

# pgsql://calvin:hobbes@[host]
function parse_conn()
{
  local auth=$1 base=$2
  local host=${1//*@/}
  local user=${1//*:\/\//}
  user=${user//:*/}
  local pass=${1//@*/}
  pass=${pass//*:/}
  if [[ $auth == pgsql://* ]]
  then
    if [ "$host" ] ; then
      echo "psql 'host=$host user=$user password=$pass dbname=$base'"
    else
      echo "psql 'user=$user password=$pass dbname=$base'"
    fi
  elif [[ $auth == mysql://* ]]
  then
    if [ "$host" ] ; then
      echo "mysql --host=$host --user=$user --pass=$pass --database=$base"
    else
      echo "mysql --user=$user --pass=$pass --database=$base"
    fi
  else
    echo "invalid authority $auth" >&2
    exit 1
  fi
}

sql1=$(parse_conn $auth1 $base)
[[ "$sql1" == psql* ]] && db1=pgsql || db1=mysql
#echo "sql1=$sql1 db1=$db1"

sql2=$(parse_conn $auth2 $base)
[[ "$sql2" == psql* ]] && db2=pgsql || db2=mysql
#echo "sql2=$sql2 db2=$db2"

[ "$create" ] &&
{
  # create and fill tables
  create_table $db1 ${name}1 $seed $rows "$key1" "$col1" $width $eng | \
      eval $sql1 &
  wait=$!
  create_table $db2 ${name}2 $seed $rows "$key2" "$col2" $width $eng | \
      eval $sql2
}

[ "$modify" ] &&
{
  # modify the second table
  change_table $db2 ${name}2 $ncol $rows "$key2" "$col2" \
      $upt $ins $del $nul $rev | eval $sql2
}

echo "EXPECTING $msg"

[ "$wait" ] && wait $wait

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
    $auth1/${base}/${name}1?$key1:$col1 $auth2/${base}/${name}2?$key2:$col2
  time pg_comparator -e $expect "$@" \
    $auth1/${base}/${name}1?$key1:$col1 $auth2/${base}/${name}2?$key2:$col2
  status=$?
fi

[ "$keep" ] ||
{
  echo "CLEAN $(date)"
  echo "DROP TABLE ${name}1, ${name}2;" | psql
}

echo "DONE $(date)"
exit $status
