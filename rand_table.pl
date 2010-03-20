#! /usr/bin/perl -w
#
# $Id: rand_table.pl 502 2010-03-20 21:28:51Z fabien $
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

GetOptions
  ("table|t=s" => \$table,
   "seed|s=i" => sub { srand($_[1]); },
   "rows|r=i" => \$rows,
   "create|e!" => \$create,
   "key|K=s" => \@keys,
   "columns|c:s" => \@columns,
   "width|w=i" => \$width,
   "start-key|start|sk|k=i" => \$key,
   "help|h" => sub {
       print "$0 -t tab -s seed -r rows -c col,names -w width -k i -e\n";
       exit 0;
   })
    or die $!;

# fix columns options
@keys = split ',', join ',', @keys;
@columns = split ',', join ',', @columns;

# declare table
if ($create)
{
  print "CREATE TABLE $table(\n  id INTEGER";
  for my $c (@keys, @columns) {
    print ",\n  $c TEXT";
  }
  print ",\n  PRIMARY KEY (", join(',','id',@keys),  ")\n);\n";
}

# fill table
print "COPY $table(", join(',', 'id', @keys, @columns), ") FROM STDIN;\n";

sub ran($)
{
    my ($n) = @_;
    my $ran = 'data:';
    while ($n--) {
	$ran .= rand();
    }
    return $ran;
}

my $i = 0;
while ($i++<$rows) {
    print $key+$i;
    for my $c (@keys, @columns) {
	print "\t", ran($width);
    }
    print "\n";
}

# end fill table
print "\\.\nANALYZE $table;\n";
