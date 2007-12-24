#! /usr/bin/perl -w
# 
# $Id: rand_table.pl 383 2007-05-23 19:47:26Z fabien $
#
# generates a sample table
#

use strict;
use Getopt::Long;

my $table = 'foo';
my $seed = '';
my $rows = 1000;
my @columns = ();
my $width = 5;
my $key = 0;
my $create = 1;

GetOptions
  ("table|t=s" => \$table,
   "seed|s=i" => \$seed,
   "rows|r=i" => \$rows,
   "create|e!" => \$create,
   "columns|c=s" => \@columns,
   "width|w=i" => \$width,
   "start-key|start|sk|k=i" => \$key,
   "help|h" => sub {
       print "$0 -t tab -s seed -r rows -c col,names -w width -k i -e\n";
       exit 0;
   })
    or die $!;

# fix options
@columns = split /,/, join ',', @columns;
@columns = ('data') unless @columns;
srand($seed) if $seed;

# declare table
if ($create)
{
    print "CREATE TABLE $table (id INTEGER PRIMARY KEY";
    for my $c (@columns) {
	print ", $c TEXT";
    }
    print ");\n";
}

# fill table
print "COPY $table(id,", join(',', @columns), ") FROM STDIN;\n";

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
    for my $c (@columns) {
	print "\t", ran($width);
    }
    print "\n";
}

# end fill table
print "\\.\nANALYZE $table;\n";
