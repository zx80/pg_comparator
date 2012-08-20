#! /usr/bin/perl
#
# $Id: rand_table.pl 1367 2012-08-20 07:38:04Z fabien $
#
# generates a sample table for pg_comparator tests
#

use strict;
use warnings;
use Getopt::Long qw(:config no_ignore_case);

my $table = 'foo';   # default table name
my $rows = 1000;     # number of rows
my @keys = ();       # list of supplemental key attributes
my @columns = ();    # list of value attributes
my $width = 1;       # add 18 chars per width
my $key = 0;         # starting id
my $null = 1;        # declare elements as nullable
my $nullkey = 0;     # allow null keys
my $create = 1;      # whether to create the table
my $db = 'pgsql';    # target database, 'pgsql' or 'mysql'
my $transaction = 0; # whether to wrap in a transaction
my ($engine, $key_cs, $tup_cs); # mysql engine, key/tuple checksum attributes

GetOptions(
  "table|t=s" => \$table,
  "seed|s=i" => sub { srand($_[1]); },
  "rows|r=i" => \$rows,
  "create|e!" => \$create,
  "keys|k:s" => \@keys,
  "columns|c:s" => \@columns,
  "width|w=i" => \$width,
  "null|n!" => \$null,
  "null-key|nk!" => \$nullkey,
  "start-key|start|sk|K=i" => \$key,
  "transaction|T!" => \$transaction,
  # target database
  "mysql" => sub { $db = 'mysql'; },
  "pgsql" => sub { $db = 'pgsql'; },
  "engine=s" => \$engine,
  # auto maintained checksums
  "key-checksum|kc=s" => \$key_cs,
  "tuple-checksum|tc=s" => \$tup_cs,
  # help
  "help|h" => sub {
    print "$0 -t tab -s seed -r rows -c col,names -w width -k i -e\n";
    exit 0;
  })
or die $!;

# fix columns options
@keys = split ',', join ',', @keys;
@columns = split ',', join ',', @columns;

die "use --no-null with --*-checksum options"
  if $null and ($tup_cs or $key_cs);

die "--*-checksum options work only for pgsql"
  if $db ne 'pgsql' and ($tup_cs or $key_cs);

die "engine option only valid under mysql"
  if defined $engine and $db ne 'mysql';

# generate an auto-maintained checksum on some not null cols
sub column_checksum($$$@)
{
  my ($name, $att, $size, @cols) = @_;
  my $cat = 'NEW.id::TEXT';
  $cat .= "||'|'||NEW." . (join "::TEXT||'|'||NEW.", @cols) . '::TEXT' if @cols;
  print
    "CREATE OR REPLACE\n",
    "FUNCTION ${table}_${name}_checksum() RETURNS TRIGGER AS\n",
    "\$\$\n",
    "BEGIN\n",
    # "  RAISE NOTICE 'compute checksum for id=%', NEW.id;\n",
    "  NEW.${att} = cksum$size($cat);\n",
    "  RETURN NEW;\n",
    "END;\n",
    "\$\$ LANGUAGE plpgsql;\n",
    "CREATE TRIGGER ${table}_${name}_checksum_trigger\n",
    "BEFORE UPDATE OR INSERT ON ${table}\n",
    "FOR EACH ROW EXECUTE PROCEDURE ${table}_${name}_checksum();\n";
}

# declare table
if ($create)
{
  print "DROP TABLE IF EXISTS $table;\n";
  print "CREATE TABLE $table(\n  id INTEGER";
  # key columns
  for my $k (@keys) {
    # note: mysql does not like BLOB/CLOB types in keys...
    # NOT NULL: implied by PRIMARY KEY if used
    print ",\n  $k ", ($db eq 'pgsql'? 'TEXT': 'VARCHAR(64)');
  }
  # other columns
  for my $c (@columns) {
    # note: mysql: max len for text is 65535
    print ",\n  $c TEXT", ($null? '': ' NOT NULL');
  }

  # checksums attributes
  print ",\n  $key_cs " . ($db eq 'pgsql'? 'INT4': 'INTEGER') . ' NOT NULL'
    if $key_cs;
  print ",\n  $tup_cs " . ($db eq 'pgsql'? 'INT8': 'BIGINT') . ' NOT NULL'
    if $tup_cs;

  # PRIMARY KEY implies NOT NULL, but UNIQUE does not
  print
      ",\n  ", $nullkey? 'UNIQUE': 'PRIMARY KEY', " (",
      join(',', 'id', @keys), ")\n)", $engine? "ENGINE $engine": '', ";\n";

  # checksum triggers, should be a single trigger.
  column_checksum('key', $key_cs, 4, @keys) if $key_cs;
  column_checksum('tup', $tup_cs, 8, (@keys, @columns)) if $tup_cs;
}
  # generate a pseudo random string
sub ran($)
{
  my ($n) = @_;
  my $ran = 'data-';
  while ($n--) {
    $ran .= rand();
  }
  return $ran;
}

if ($rows)
{
  # start transaction
  print "BEGIN;\n" if $transaction;

  # fill table
  print "COPY $table(", join(',', 'id', @keys, @columns), ") FROM STDIN;\n"
      if $db eq 'pgsql';
  print "INSERT INTO $table VALUES\n" if $db eq 'mysql';

  # generate speudo random table contents
  my $i = 0;
  while ($i<$rows)
  {
    $i++;
    if ($db eq 'pgsql') {
      print (($nullkey and $i==1)? '\N': ($key+$i));
      for my $c (@keys, @columns) {
	print "\t", ran($width);
      }
      print "\n";
    }
    elsif ($db eq 'mysql') {
      print '(';
      print (($nullkey and $i==1)? 'NULL': ($key+$i));
      for my $c (@keys, @columns) {
	print ",'", ran($width), "'";
      }
      print ")", ($i!=$rows? ',': ''), "\n";
    }
    else {
      die "unexpected db=$db";
    }
  }

  print "\\.\n" if $db eq 'pgsql';
  print ";\n" if $db eq 'mysql';

  # end transaction
  print "COMMIT;\n" if $transaction;

  # end fill table
  print "ANALYZE $table;\n" if $db eq 'pgsql';
}
