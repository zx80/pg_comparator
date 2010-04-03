#! /usr/bin/perl -w
#
# $Id: rand_table.pl 647 2010-03-31 15:28:06Z fabien $
#
# generates a sample table
#

use strict;
use Getopt::Long qw(:config no_ignore_case);

my $table = 'foo';
my $rows = 1000;
my @keys = ();
my @columns = ();
my $width = 5;
my $key = 0;
my $create = 1;
my $db = 'pgsql';
my $engine;

GetOptions
  ("table|t=s" => \$table,
   "seed|s=i" => sub { srand($_[1]); },
   "rows|r=i" => \$rows,
   "create|e!" => \$create,
   "keys|k:s" => \@keys,
   "columns|c:s" => \@columns,
   "width|w=i" => \$width,
   "start-key|start|sk|K=i" => \$key,
   # target database
   "mysql" => sub { $db = 'mysql'; },
   "pgsql" => sub { $db = 'pgsql'; },
   "engine=s" => \$engine,
   # help
   "help|h" => sub {
       print "$0 -t tab -s seed -r rows -c col,names -w width -k i -e\n";
       exit 0;
   })
    or die $!;

# fix columns options
@keys = split ',', join ',', @keys;
@columns = split ',', join ',', @columns;

die "engine option only valid under mysql"
    if defined $engine and $db ne 'mysql';

# declare table
if ($create)
{
  print "CREATE TABLE $table(\n  id INTEGER";
  for my $c (@keys, @columns) {
    print ",\n  $c ", $db eq 'pgsql'? 'TEXT': 'VARCHAR(64)';
  }
  print
      ",\n  PRIMARY KEY (", join(',','id',@keys), ")\n)",
      $engine? "ENGINE $engine": '', ";\n";
}

# fill table
print "COPY $table(", join(',', 'id', @keys, @columns), ") FROM STDIN;\n"
    if $db eq 'pgsql';

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

# end fill table
print "\\.\nANALYZE $table;\n" if $db eq 'pgsql';
