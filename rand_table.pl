#! /usr/bin/perl
#
# $Id: rand_table.pl 1298 2012-08-17 07:14:22Z fabien $
#
# generates a sample table for pg_comparator tests
#

use strict;
use warnings;
use Getopt::Long qw(:config no_ignore_case);

my $table = 'foo';
my $rows = 1000;
my @keys = ();
my @columns = ();
my $width = 1;
my $key = 0;
my $null = 1;
my $create = 1;
my $db = 'pgsql';
my $transaction = 0;
my ($engine, $key_cs, $tup_cs);

GetOptions(
  "table|t=s" => \$table,
  "seed|s=i" => sub { srand($_[1]); },
  "rows|r=i" => \$rows,
  "create|e!" => \$create,
  "keys|k:s" => \@keys,
  "columns|c:s" => \@columns,
  "width|w=i" => \$width,
  "null|n!" => \$null,
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

# generate an auto-maintained checksum on some cols
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
  for my $c (@keys, @columns) {
    print ",\n  $c ",
      ($db eq 'pgsql'? 'TEXT': 'VARCHAR(64)'),
      ($null? '': ' NOT NULL');
  }

  # checksums attributes
  print ",\n  $key_cs " . ($db eq 'pgsql'? 'INT4': 'INTEGER') . ' NOT NULL'
    if $key_cs;
  print ",\n  $tup_cs " . ($db eq 'pgsql'? 'INT8': 'BIGINT') . ' NOT NULL'
    if $tup_cs;

  # primary key
  print
      ",\n  PRIMARY KEY (", join(',','id',@keys), ")\n)",
      $engine? "ENGINE $engine": '', ";\n";

  # checksum triggers, should be a single trigger.
  column_checksum('key', $key_cs, 4, @keys) if $key_cs;
  column_checksum('tup', $tup_cs, 8, (@keys, @columns)) if $tup_cs;
}

# start transaction
print "BEGIN;\n" if $transaction;

# fill table
print "COPY $table(", join(',', 'id', @keys, @columns), ") FROM STDIN;\n"
    if $db eq 'pgsql';

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

my $i = 0;
while ($i++<$rows) {
  if ($db eq 'pgsql') {
    print $key+$i;
    for my $c (@keys, @columns) {
      print "\t", ran($width);
    }
    print "\n";
  }
  elsif ($db eq 'mysql') {
    print "INSERT INTO $table VALUES(", $key+$i;
    for my $c (@keys, @columns) {
      print ",'", ran($width), "'";
    }
    print ");\n";
  }
}

# end transaction
print "COMMIT;\n" if $transaction;

# end fill table
print "\\.\nANALYZE $table;\n" if $db eq 'pgsql';
