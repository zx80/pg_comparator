#! /usr/local/bin/perl -w
# 
# $Id: rand_table.pl 147 2004-08-24 08:21:12Z coelho $
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

GetOptions("table|t=s" => \$table,
	   "seed|s=i" => \$seed,
	   "rows|r=i" => \$rows,
	   "columns|c=s" => \@columns,
	   "width|w=i" => \$width)
    or die $!;

# fix options
@columns = split /,/, join ',', @columns;
@columns = ('data') unless @columns;
srand($seed) if $seed;

# declare table
print "CREATE TABLE $table (id INTEGER PRIMARY KEY";
for my $c (@columns) {
    print ", $c TEXT";
}
print ");\n";

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
    print $i;
    for my $c (@columns) {
	print "\t", ran($width);
    }
    print "\n";
}

# end fill table
print "\\.\n";
