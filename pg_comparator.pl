#! /usr/bin/perl -w
#
# $Id: pg_comparator.pl 421 2008-02-17 09:48:09Z fabien $
#
# HELP 1: pg_comparator --man
# HELP 2: pod2text pg_comparator
# HELP 3: read pod stuff bellow with your favorite viewer
#

=head1 NAME

B<pg_comparator> - network and time efficient table content comparison.

=head1 SYNOPSIS

B<pg_comparator> [options as B<--help> B<--option> B<--man>] conn1 conn2

=head1 DESCRIPTION

This script performs a network and time efficient comparison of two
possibly large tables on two servers. It makes only sense to use it
if the expected differences are small.

The implementation is quite generic: multi-column keys, no assumption
of data types other that they can be cast to text, subset of columns
can be used for the comparison, handling of null values...

=head1 OPTIONS

Options allow to request help or to adjust some internal parameters.
Short one-letter options are also available, usually with the first letter
of the option name.

=over 4

=item C<--aggregate=xor>

Aggregation function to be used for summaries.
Must operate on the result of the checksum function.

=item C<--ask-pass>

Ask for passwords interactively.

=item C<--checksum='cksum8(%s)'>

Checksum function to be used. The quality of this function in term
of bit-spreading and uniformity is important for the quality of the
results. A poor function might miss differences because of collisions
or result in a more costly search. Cryptographic hash functions such as 
MD5 or SHA1 are a good choice.

=item C<--cleanup>

Drop checksum tables. Useful after C<--notemp>.

=item C<--factor=7>

Folding factor: log2 of the number of rows grouped together at each stage.
Default chosen after some basic tests on medium-size cases.

=item C<--help>

Show short help.

=item C<--man>

Show manual page.

=item C<--max-report=32>

Maximum search effort, search will stop if above this threshold: 
it gives up if a single query at any level involves more than this 
many differences. Use 0 for no limit.

=item C<--max-levels=0>

Maximum number of levels used. Allows to cut-off folding.
Default is 0, meaning no cut-off. Setting a value of 1
only uses the checksum table, without summaries.

=item C<--null="COALESCE(%s::TEXT,'null')">

String processing template to take care of null values.
If you set --null='%s', null values are set as they appear,
which might damage the results in null columns are used because 
multi-column checksums will all be 0.

=item C<--option>

Show option summary.

=item C<--prefix=cmp>

Name prefix for comparison tables. May be schema-qualified.

=item C<--report> C<--noreport>

Report keys as they are found. Default is to report.

=item C<--separator=:>

Separator string when concatenating columns.

=item C<--source='DBI:Pg:dbname=%b;host=%h;port=%p;'>

DBI source template. Changing this might allow to use this command 
with another type of database, as the SQL used is quite standard.
Tags are interpreted as follow: %b is base, %h is host, %p is port, %u 
is login.

=item C<--source2=...>

DBI source template for second connection. 
Default is same as previous --source option.

=item C<--assume-size=...>

Assume the provided value as the table size, thus skipping the COUNT
query.

=item C<--temporary --notemporary>

Whether to use temporary tables. Default is to use.
If you don't, the tables are kept at the end, so they will have
to be deleted by hand.

=item C<--threads>

Use threads: a synonymous for "segmentation fault":-)
It seems that DBI or DBD::Pg does not like threads at all...

=item C<--stats>

Show various statistics.

=item C<--verbose>

Be verbose about what is happening. The more you ask, the more verbose.

=item C<--where=...>

SQL boolean condition for partial comparison.

=back

=head1 ARGUMENTS

The two arguments describe database connections with the following URL-like
syntax, where square brackets denote optional parts. Although all parts
are optional, the arguments must not be empty!

  [login[:pass]@][host[:port]]/base/[schema.]table[?[key:]cols]

=over 4

=item B<login>

Login to use when connecting to database. Default is username.

=item B<pass>

Password to use when connecting to database. Default is none.

=item B<host>

Hostname to connect to. Default is localhost.

=item B<port>

Tcp-ip port to connect to. Default is 5432 for PostgreSQL.

=item B<base>

Database catalog to connect to. Default is username.

=item B<schema.table>

The possibly schema-qualified table to use for comparison.
Default is same as first connection.

=item B<keys>

Comma-separated list of key columns. Default is same as first connection.

=item B<cols>

Comma-separated list of columns to compare. Default is same as first 
connection.

=back

=head1 OUTPUT

The output of the command consists of lines describing the differences
found between the two tables. They are expressed in term of insertions,
updates or deletes and of tuple keys.

=over 4

=item B<UPDATE k>

Key I<k> tuple is updated from table 1 to table 2.
It exists in both tables with different values.

=item B<INSERT k>

Key I<k> tuple does not appear in table 2, but only in table 1. 
It must be inserted in table 2 to synchronize it wrt table 1.

=item B<DELETE k>

Key I<k> tuple appears in table 2, but not in table 1.
It must be deleted from 2 to synchronize it wrt table 1.

=back

In case of key-checksum or data-checksum collision, false positive or
false negative results may occur. Changing the checksum function would
help in such cases.

=head1 DEPENDENCES

Three support functions are needed on the database:

=over 2

=item 1

The C<COALESCE> function takes care of NULL values in columns.
It may be changed with the B<--null> option.

=item 2

A checksum function must be used to reduce and distribute key
and columns values. 
It may be changed with the B<--checksum> option.

PostgreSQL C<MD5> function can be used for this purpose.

Three other suitable implementations are available for PostgreSQL 
and can be loaded into the server by processing C<share/contrib/checksum.sql>.
The three functions C<cksum2>, C<cksum4> and C<cksum8> differ on the size
of the resulting checksum expressed in bytes. The default is to use
the C<cksum8> version.

=item 3

An aggregate function is used to summarize checksums for a range of rows. 
It must operate on the result of the checksum function.
It may be changed with the B<--aggregate> option.

Suitable implementations of a exclusive-or C<xor> aggregate are available 
for PostgreSQL and can be loaded into the server by processing 
C<share/contrib/xor_aggregate.sql>.

=back

Moreover several perl modules are useful to run this script:

=over 4

=item 

C<Getopt::Long> for option management.

=item

C<DBI> and C<DBD::Pg> to connect to PostgreSQL.

=item

C<Term::ReadPassword> for ask-pass option.

=item

C<Pod::Usage> for doc self-extraction (--man --opt --help).

=back

=head1 EXAMPLES

Compare tables calvin and hobbes in default database on localhost, 
with key I<id> and columns I<c1> and I<c2>:

    ./pg_comparator /calvin?id:c1,c2 /hobbes

Compare tables calvin in default database on localhost and the same
table in default database on sablons, with key I<id> and column I<data>:

    ./pg_comparator localhost/calvin?id:data sablons

=head1 ALGORITHM

The aim of the algorithm is to compare the content of two tables,
possibly on different remote servers, with minimum network traffic.
It is performed in three phases. 

=over 2

=item 1 

A checksum table is computed on each side for the target table.

=item 2 

A fist level summary table is computed on each side by aggregating chunks
of the checksum table. Other levels of summary aggregations are then performed
till there is only one row in the last table, which then stores a 
global checksum for the whole initial target tables. 

=item 3 

Starting from the upper summary tables, aggregated checksums are compared 
from both sides to look for differences, down to the initial checksum table.
Keys of differing tuples are displayed.

=back

=head2 CHECKSUM TABLE

The first phase computes the initial cheksum table I<t(0)> on each side.
Assuming that I<key> is the table key columns, and I<cols> is the
table data columns that are to be checked for differences, then
it is performed by querying target table I<t> as follow:

  CREATE TABLE t(0) AS
  SELECT key AS id, checksum(key) AS idc, checksum(key || cols) AS cks
  FROM t;

The inititial key is kept, as it will be used to show differing keys
at the end. The rational for the I<idc> column is to randomize the 
key-values distribution so as to balance aggrates in the next phase.
The key must appear in the cheksum also, otherwise content exchanged 
between two keys would not be detected in some cases.

=head2 SUMMARY TABLES

Now we compute a set of cascading summary tables by grouping I<f>
(folding factor) checksums together at each stage. The grouping is
based on a mask on the I<idc> column to take advantage of the 
checksum randomization. Starting from I<p=0> we build:

  CREATE TABLE t(p+1) AS
  SELECT idc & mask(p+1) AS idc, XOR(cks)
  FROM t(p)
  GROUP BY idc & mask(p+1);

The mask(p) is defined so that it groups together on average I<f>
checksums together: mask(0) = ceil2(size); mask(p) = mask(p-1)/f;
This leads to a hierarchy of tables, each one being a smaller summary
of the previous one:

=over 4

=item level B<0>

checksum table, I<size> rows, i.e. as many rows as the target table.

=item level B<1>

first summary table, (size/f) rows.

=item level B<p>

intermediate summary table, (size/f**p) rows.

=item level B<n-1>

one before last summary table, less than f rows.

=item level B<n>

last summary table, mask is 0, 1 row.

=back

It is important that the very same masks are used so that aggregations
are the same, allowing to compare matching contents on both sides.

=head2 SEARCH FOR DIFFERENCES

After all these support tables are built on both sides comes the search for
differences. When checking the checksum summary of the last tables (level I<n>)
with only one row, it is basically a comparison of the cheksum of the 
whole table contents. If they match, then both tables are equal, 
and we are done. Otherwise, if these checksums differ, some investigation 
is needed to detect offending keys.

The investigation is performed by going down the table hierarchy and
looking for all I<idc> for which there was a difference in the checksum
on the previous level. The same query is performed on both side
at each stage:

  SELECT idc, cks
  FROM t(p)
  WHERE idc & mask(p+1) IN (idc-with-diff-checksums-from-level-p+1)
  ORDER BY idc, cks;

And the results from both sides are merged together. 
When doing the merge procedure, four cases can arise:

=over 2

=item 1

Both I<idc> and I<cks> match. Then there is no difference.

=item 2

Although I<idc> does match, I<cks> does not. Then this I<idc> is
to be investigated at the next level, as the checksum summary differs.
If we are already at the last level, then the offending key can be shown.

=item 3

No I<idc> match, one supplemental I<idc> in the first side.
Then this I<idc> correspond to key(s) that must be inserted
for syncing the second table wrt the first.

=item 4

No I<idc> match, one supplemental I<idc> in the second side.
Then this I<idc> correspond to key(s) that must be deleted
for syncing the second table wrt the first.

=back

Cases 3 and 4 are simply symmetrical, and it is only an interpretation 
to decide whether it is an insert or a delete, taking the first side 
as the reference.

=head2 IMPLEMENTATION ISSUES

The checksum implementation gives integers, which are constant length
and easy to manipulate afterwards.

The xor aggregate is a good choice because there is no overflow issue with it
and it takes into account all bits of the input.

Null values must be taken care appropriatelly.

The folding factor and all modules are taken as power of two...

There is a special management of large chunks of deletes or inserts
which is implemented although not detailed in the algorithmic overview
above nor the complexity analysis below.

=head1 ANALYSIS

Let I<n> be the number of rows, I<r> the row size, I<f> the folding factor
and I<k> the number of differences to be detected. Then ISTM that:

=over 2

=item B<network volume>

is better than I<k*f*ceil(log(n)/log(f))>: it is independent of I<r>, 
the lower I<f> the better, and you want I<k<<n>.

=item B<number of requests>

maximum is I<6+2*ceil(log(n)/log(f))>,
minimum is I<6+ceil(log(n)/log(f))> for equal tables.

=item B<disk I/O traffic>

is about I<n*r+n*ln(n)*(f/(f-1))>. Here a not too small I<f> is better, as 
it reduces both the number of requests and of disk I/Os; 

=back

The choice of I<f> is indeed a tradeoff.

=head1 REFERENCES

This script and algorithm was somehow inspired by:

=over 2

I<Taming the Distributed Database Problem: A Case Study Using MySQL>
by Giuseppe Maxia in B<Sys Admin> vol 13 num 8, Aug 2004, pp 29-40.
See L<http://www.perlmonks.org/index.pl?node_id=381053> for details.

=back

In the above paper, three algorithms are presented. 
The first one compares two tables with a checksum technique.
The second one finds UPDATE or INSERT differences based on a 2-level 
(checksum and summary) table hierarchy. The algorithm is asymmetrical,
as different queries are performed on the two tables to be compared.
It seems that the network traffic volume is in I<k*(f+(n/f)+r)>,
that it has a probabilistically-buggy merge procedure, and
that it makes assumptions about the distribution of key values.
The third algorithm looks for DELETE differences based on counting,
with the implicit assumption that there are only such differences.

The algorithm used here implements all three tasks. It is fully symmetrical.
It finds UPDATE, DELETE and INSERT between the two tables. 
The checksum and summary hierarchical level idea is reused and generalized
so as to reduce the algorithmic complexity.

From the implementation standpoint, the script is as parametric 
as possible thru many options, and makes as few assumptions 
as possible about table structures, types and values.

=head1 SEE ALSO

Some products implement such features:
L<http://www.programurl.com/software/sql-server-comparison.htm>
L<http://www.dbbalance.com/db_comparison.htm>
L<http://www.dkgas.com/dbdiff.htm>
L<http://www.sql-server-tool.com/>
L<http://sourceforge.net/projects/mysqltoolkit>

=head1 BUGS

All softwares have bugs. This is a software, hence it has bugs.
Reporting bugs is good practice, so tell me if you find one!

=head1 VERSIONS

See L<http://pgfoundry.org/projects/pg-comparator/> for the latest
version. My personnal site for the tool is 
L<http://www.coelho.net/pg_comparator/>.

=over 4

=item B<version 1.4.2> 17/02/2008

Minor makefile fix asked for by Roberto C. Sanchez. 

=item B<version 1.4.1> 14/02/2008

Minor fix for 8.3 by Roberto C. Sanchez. 

=item B<version 1.4> 24/12/2007

Port for 8.2. Better documentation. Fix masq bug: although the returned answer
was correct, the table folding was not. DELETE/INSERT messages exchanged so as
to match a 'sync' or 'copy' semantics.

=item B<version 1.3> 31/08/2004

Project moved to L<http://pgfoundry.org/>.
Use cksum8 checksum function by default.
Minor doc updates.

=item B<version 1.2> 27/08/2004

Added B<--show-all-keys> option for handling big chunks of deletes
or inserts.

=item B<version 1.1> 26/08/2004

Fix algorithmic bug: checksums B<must> also include the key, 
otherwise exchanged data could be not detected if the keys were 
to be grouped together.

Algorithmic section added to manual page.
Thanks to I<Giuseppe Maxia> who asked for it.

Various code cleanups.

=item B<version 1.0> 25/08/2004

Initial revision.

=back

=head1 COPYRIGHT

Copyright (c) 2004-2007, Fabien Coelho <fabien at coelho dot net>
http://www.coelho.net/

This softwere is distributed under the terms of the BSD Licence. 
Basically, you can do whatever you want, but you have to keep
the license... and I'm not responsible for any consequences. 
Beware, you may lose your data or your hairs because of this software!
See the LICENSE file enclosed with the distribution for details.

If you are very happy with this software, I would appreciate a postcard
saying so (see my webpage for current address).

=cut

use strict; # I don't like perl;-)
use Getopt::Long;
use DBI;

# various option defaults
my ($factor, $temp, $ask_pass, $verb, $max_report, $max_levels) = 
    (7, 1, 0, 0, 32, 0);
my ($agg, $prefix, $sep) = ('xor', 'cmp', ':');
my $source = 'DBI:Pg:dbname=%b;host=%h;port=%p;';
my ($source2, $where);
my ($cksum, $null) = ("CKSUM8(%s)", "COALESCE(%s::TEXT,'null')");
my ($report, $threads, $cleanup, $stats, $skip) = (1, 0, 0, 0, 0);

# self extracting help
# usage(verbosity, exit value, message)
sub usage($$$)
{
    my ($verbose,$stat,$msg) = @_;
    print STDERR "ERROR: $msg\n" if $msg;
    require Pod::Usage;
    Pod::Usage::pod2usage(-verbose => $verbose, -exitval => $stat);
}

# parse a connection string... or many options instead?
# could we directly ask for the DBI connection string?
# ($u,$w,$h,$p,$b,$t,$k,$c) = parse_conn("connection-string")
# globals: $ENV{USER}, $verb
# format: fabien:secret@host.domain.co:5432/base/schema.table?key:col,list
# base and table are mandatory
sub parse_conn($)
{
    my $c = shift;
    my ($user, $pass, $host, $port, $base, $tabl, $keys, $cols) = # defaults
	($ENV{USER}, '', 'localhost', 5432, undef, undef, undef, undef);

    use URI;
    # http is rather artificial here
    my $uri = new URI("http://$c") or die "parse error on '$c'"; 
    $host = $uri->host if $uri->host;
    $port = $uri->port if $uri->port;
    ($user,$pass) = split /:/, $uri->userinfo if $uri->userinfo;
    $pass = '' unless defined $pass;
    ($base,$tabl) = ($uri->path_segments)[1,2];
    ($keys, $cols) = split /:/, $uri->query;

    my @res = ($user,$pass,$host,$port,$base,$tabl,$keys,$cols);
    die "unexpected connection string: @res" 
	unless defined $base and defined $tabl and 
	       defined $keys and defined $cols;
    print STDERR "connection parameters: @res\n" if $verb>1;
    return @res;
}

# $dbh = conn($source_template,$base,$host,$port,$user,$pass)
# globals: $verb
sub conn($$$$$$)
{
    my ($s,$b,$h,$p,$u,$w) = @_;
    $s =~ s/\%b/$b/g;  $s =~ s/\%h/$h/g;  $s =~ s/\%p/$p/g;  $s =~ s/\%u/$u/g;
    print STDERR "# connecting to s=$s u=$u\n" if $verb>1;
    my $dbh = DBI->connect($s, $u, $w, { RaiseError => 1, PrintError => 0 });
    print STDERR "# connected to $u\@$h:$p/$b\n" if $verb;
    return $dbh;
}

# $number_of_rows = count($dbh,$table)
sub count($$)
{
    my ($dbh,$table) = @_;
    # ??? count is not counted!
    return $dbh->selectrow_array("SELECT COUNT(*) FROM $table");
}

# @l = subs(format, @column_names)
sub subs($@)
{
    my $fmt = shift;
    for my $s (@_) {
	my $n = $fmt;
	$n =~ s/\%s/$s/g;
	$s = $n;
    }
    #print STDERR "$fmt: @_\n";
    return @_;
}

# returns an sql concatenation of fields
# $sql = concat($string_of_comma_separated_fields)
# globals: $sep ($null)
sub concat($)
{
    return join("||'$sep'||",subs($null, split(/,/,shift)));
}

# inspired from mysql-table-sync implementation
# mask is int4. could also use binary?
# not used yet.
sub best_int_type($)
{
    my ($mask) = @_;
    return 'INT4' if $mask & ~0x0000ffff;
    return 'INT2';
}

# global counter
my $query_nb = 0; # number of queries
my $query_sz = 0; # size of queries
my $query_fr = 0; # fetched rows

sub sql_do($$)
{
    my ($dbh,$query) = @_;
    $query_nb++;
    $query_sz += length($query);
    print STDERR "$query_nb\t$query\n" if $verb>2;
    return $dbh->do($query);
}

# $count = compute_cheksum($dbh,$table,$keys,$cols,$name,$skip)
# globals: $temp $cksum $verb $cleanup ($sep $null)
sub compute_cheksum($$$$$$)
{
    my ($dbh, $table, $keys, $cols, $name, $skip) = @_;
    print STDERR "building checksum table ${name}0\n" if $verb>1;
    sql_do($dbh, "DROP TABLE IF EXISTS ${name}0") if $cleanup;
    # ??? CREATE + INSERT SELECT to get row count?
    # would also allow to choose better types (int2/int4/int8...)?
    sql_do($dbh,
	   "CREATE ${temp}TABLE ${name}0 AS " .
	   "SELECT " . concat($keys) . " AS id, " .
	   join('', subs($cksum, concat($keys))) . " AS idc," .
	   join('', subs($cksum, concat("$keys,$cols"))) . " AS cks " .
	   "FROM $table " .
	   ($where? "WHERE $where": ''));
    # count should be available somewhere?
    return $skip? 0: count($dbh, "${name}0");
}

# compute_summaries($dbh, $name, @masks)
# globals: $verb $temp $agg $cleanup
sub compute_summaries($$@)
{
    my ($dbh, $name, @masks) = @_;
    # compute cascade of summary tables
    for my $level (1 .. @masks-1) {
	print STDERR "building summary table ${name}$level ($masks[$level])\n" 
	    if $verb>1;
	sql_do($dbh, "DROP TABLE IF EXISTS ${name}${level}") if $cleanup;
	sql_do($dbh,
	       "CREATE ${temp}TABLE ${name}${level} AS " .
	       # the "& mask" is really a modulo operation
	       "SELECT idc & $masks[$level] AS idc, $agg(cks) AS cks " .
	       "FROM ${name}" . ($level-1) . " " .
	       "GROUP BY idc & $masks[$level]");
    }
}

# get info for investigated a list of idc (hopefully not too long)
# $sth = selidc($dbh,$table,$mask,$get_id,@idc)
sub selidc($$$$@)
{
    my ($dbh, $table, $mask, $get_id, @idc) = @_;
    my $query = 'SELECT idc, cks' . ($get_id? ', id ': ' ') . "FROM $table ";
    # the "& mask" is really a modulo operation
    $query .= "WHERE idc & $mask IN (" . join(',', @idc) . ') ' if @idc;
    $query .= 'ORDER BY idc, cks';
    my $sth = $dbh->prepare($query);
    $query_nb++;
    $query_sz += length($query);
    print STDERR "$query_nb\t$query\n" if $verb>2;
    $sth->execute();
    return $sth;
}

# investigate an "idc/mask" list to show corresponding keys.
# get_bulk_keys($dbh, $table, $nature, @idc_masks)
# globals: $verb $report
sub get_bulk_keys($$$@)
{
    my ($dbh, $table, $nature, @idc_masks) = @_;
    return (0, undef) unless @idc_masks; # bye if nothing to investigate
    my @keys = (); # results
    my $cond = ''; # select query condition. must not be empty.
    print STDERR "# investigating $nature chunks\n" if $verb;
    for my $idc_mask (@idc_masks) {
	my ($idc,$mask) = split '/', $idc_mask;
	$cond .= ' OR ' if $cond;
	$cond .= "idc & $mask = $idc";
    }
    my $count = 0;
    my $query = "SELECT id FROM $table WHERE $cond ORDER BY id";
    my $sth = $dbh->prepare($query);
    $query_nb++;
    $query_sz += length($query);
    print STDERR "$query_nb\t$query\n" if $verb>2;
    $sth->execute();
    while (my @row = $sth->fetchrow_array()) {
	$count ++;
	push @keys, $row[0];
	print "$nature @row\n" if $report;
    }
    return ($count, \@keys);
}

# compute differences by climbing up the tree, output result on the fly.
# differences($dbh1, $dbh2, $name1, $name2, @masks)
# globals: $max_report $verb $report
sub differences($$$$@)
{
    my ($dbh1, $dbh2, $name1, $name2, @masks) = @_;
    my $level = @masks-1; # number of last summary table
    my ($mask, $count,$todo) = (0, 0, 1); # mask of previous table
    my (@insert, @update, @delete, @mask_insert, @mask_delete); # results
    my @idc = ();

    while ($level>=0 and $todo)
    {
	my @next_idc = ();
	print STDERR "investigating @idc, level=$level\n" if $verb>1;

	if ($max_report && @idc>$max_report) {
	    print "giving up at level $level: too many differences.\n" .
		"\tadjust max option (current is $max_report) to proceed.\n" .
		"\tidc list length is " . scalar @idc . ": @idc\n";
	    return;
	}

	# select statement handlers
	my $s1 = selidc($dbh1, ${name1}.$level, $mask, !$level, @idc); 
	my $s2 = selidc($dbh2, ${name2}.$level, $mask, !$level, @idc);
	# content of one row from the above select result
	my (@r1, @r2); 
	
	# let us merge the two ordered select
	while (1)
	{
	    # update current lists if necessary
	    $query_fr++, @r1 = $s1->fetchrow_array() 
		unless @r1 or not $s1->{Active};
	    $query_fr++, @r2 = $s2->fetchrow_array() 
		unless @r2 or not $s2->{Active};
	    last unless @r1 or @r2;
	    # else both lists are defined

	    if (@r1 && @r2 && $r1[0]==$r2[0]) { # matching idc
		if ($r1[1] != $r2[1]) { # non matching checksums
		    if ($level) {
			push @next_idc, $r1[0]; # to be investigated...
		    } else {
			# the level-0 table keeps the actual key
			$count ++;
			push @update, $r1[2];
			print "UPDATE $r1[2]\n" if $report; # final result
		    }
		}
		# both tuples are consummed
		@r1 = @r2 = ();
	    }
	    # if they do not match, one is missing or less than the other
	    elsif ((!@r2) || (@r1 && $r1[0]<$r2[0])) { # more idc in table 1
		if ($level) {
		    push @mask_insert, "$r1[0]/$masks[$#masks]"; # later
		} else {
		    $count ++;
		    push @insert, $r1[2];
		    print "INSERT $r1[2]\n" if $report; # final result
		}
		# left tuple is consummed
		@r1 = ();
	    } 
	    # this could be a else
	    elsif ((!@r1) || (@r2 && $r1[0]>$r2[0])) { # more idc in table 2
		if ($level) {
		    push @mask_delete, "$r2[0]/$masks[$#masks]"; # later
		} else {
		    $count ++;
		    push @delete, $r2[2];
		    print "DELETE $r2[2]\n" if $report; # final result
		}
		# right tuple is consummed
		@r2 = ();
	    }
	    else {
		die "this state should never happen";
	    }
	}
	$s1->finish(); 	$s2->finish();
	$level--; # next table! 0 is the initial checksum table
	$mask = pop @masks; # next mask
	@idc = @next_idc; # idcs to be investigated on next round
	$todo = @idc;
    }

   return ($count, \@insert, \@update, \@delete, \@mask_insert, \@mask_delete);
}

# option management
GetOptions("manual|man|m" => sub { usage(2, 0, ''); },
           "options|option|o" => sub { usage(1, 0, ''); },
           "help|h" => sub { usage(0, 0, ''); },
	   "verbose|v+" => \$verb,
	   "checksum-function|cksum|cf|c=s" => \$cksum,
	   "cleanup!" => \$cleanup,
	   "aggregate-function|af|a=s" => \$agg,
	   "factor|f=i" => \$factor,
	   "maximum-report|max-report|mr|x=i" => \$max_report,
	   "maximum-levels|max-levels|ml=i" => \$max_levels,
	   "null|n=s" => \$null,
	   "prefix|p=s" => \$prefix,
	   "report|r!" => \$report,
	   "separator|s=s" => \$sep,
	   "source|u=s" => \$source,
	   "source2|u2=s" => \$source2,
	   "where|w=s" => \$where,
	   "temporary|tmp|t!" => \$temp,
	   "threads!" => \$threads,
	   "statistics|stats!" => \$stats,
	   "assume-size|as=i" => \$skip,
	   "ask-pass|ap!" => \$ask_pass) or die "$! (try $0 --help)";

# fix source2
$source2 = $source unless defined $source2;

# fix --temp or --notemp option
$temp = $temp? 'TEMPORARY ': '';

# fix factor size
$factor = 1 if $factor<1;
$factor = 30 if $factor>30;

# intermediate table names
my ($name1,$name2) = ("${prefix}_1_", "${prefix}_2_");

# argument management
usage(0,0,'expecting 2 arguments') unless @ARGV == 2;
my ($u1,$w1,$h1,$p1,$b1,$t1,$k1,$c1) = parse_conn(shift);
my ($u2,$w2,$h2,$p2,$b2,$t2,$k2,$c2) = parse_conn(shift);

# fix some default values
$t2 = $t1 unless $t1;  $k2 = $k1 unless $k2;  $c2 = $c1 unless $c2;

if ($ask_pass) {
    require Term::ReadPassword;
    $w1 = Term::ReadPassword::read_password('connection 1 password> ');
    $w2 = Term::ReadPassword::read_password('connection 2 password> ');
}

# let us work on both data in parallel
sub conn_and_cksum($$$$$$$$$$$)
{
    my ($src,$b,$h,$p,$u,$w,$t,$k,$c,$n,$skip) = @_;
    my $dbh = conn($src,$b,$h,$p,$u,$w);
    my $count = compute_cheksum($dbh, $t, $k, $c, $n, $skip);
    return $dbh,$count;
}

use Time::HiRes qw(gettimeofday tv_interval);
my ($t0, $tcks, $tsum, $tmer, $tblk);
$t0 = [gettimeofday] if $stats;

print STDERR "# connecting to database servers...\n" if $verb;
my ($thr1, $dbh1, $count1, $thr2, $dbh2, $count2);
if ($threads)
{
    require threads;
    ($thr1) = threads->new(\&conn_and_cksum, $source,    
		      $b1, $h1, $p1, $u1, $w1, $t1, $k1, $c1, $name1, $skip)
	or die "cannot create thread 1-1";

    ($thr2) = threads->new(\&conn_and_cksum, $source2,
		      $b2, $h2, $p2, $u2, $w2, $t2, $k2, $c2, $name2, $skip)
	or die "cannot create thread 2-1";

    print STDERR "# waiting for connexions and counts...\n" if $verb;
    ($dbh1,$count1) = $thr1->join;
    ($dbh2,$count2) = $thr2->join;
}
else
{
    ($dbh1,$count1) = conn_and_cksum($source, 
		       $b1, $h1, $p1, $u1, $w1, $t1, $k1, $c1, $name1, $skip);
    
    ($dbh2,$count2) = conn_and_cksum($source2, 
		       $b2, $h2, $p2, $u2, $w2, $t2, $k2, $c2, $name2, $skip);
}

print STDERR "# computing size and masks after folding factor...\n" if $verb;
$count1 = $count2 = $skip if $skip;
my $size = $count1>$count2? $count1: $count2; # max?
my ($mask, $i, @masks) = (0, 0);
while ($mask<$size) {
    $mask = (1<<($i*$factor)) - 1;
    unshift @masks, $mask;
    $i++;
}
my $levels = @masks;
splice @masks, $max_levels if $max_levels; # cut-off
print STDERR "# masks=@masks\n" if $verb>2;

$tcks = [gettimeofday] if $stats;

print STDERR "# building summary tables...\n" if $verb;
if ($threads)
{
    $thr1 = threads->new(\&compute_summaries, $dbh1, $name1, @masks)
    	or die "cannot create thread 1-2";

    $thr2 = threads->new(\&compute_summaries, $dbh2, $name2, @masks)
	or die "cannot create thread 2-2";

    $thr1->join;
    $thr2->join;
}
else
{
    compute_summaries($dbh1, $name1, @masks);
    compute_summaries($dbh2, $name2, @masks);
}

$tsum = [gettimeofday] if $stats;

print STDERR "# looking for differences...\n" if $verb;
my ($count, $ins, $upt, $del, $bins, $bdel) = 
    differences($dbh1, $dbh2, $name1, $name2, @masks);

$tmer = [gettimeofday] if $stats;
		 
# now take care of big chunks of INSERT or DELETE if necessary
# should never happen in normal "few differences" conditions
print STDERR "bulk delete: @{$bdel}\n" if @$bdel and $verb;
print STDERR "bulk insert: @{$bins}\n" if @$bins and $verb;

my ($bic, $bdc, $insb, $delb) = (0, 0, undef, undef);
if (@$bins or @$bdel)
{
    # this cost two full table-0 scans, one on each side...
    if ($threads)
    {
	# hmmm... useless thread if list is empty
	$thr1 = threads->new(\&get_bulk_keys, 
			     $dbh1, "${name1}0", 'INSERT', @$bins)
	    or die "cannot create thread 1-3";
	
	$thr2 = threads->new(\&get_bulk_keys, 
			     $dbh2, "${name2}0", 'DELETE', @$bdel)
	    or die "cannot create thread 2-3";
	
	($bic, $insb) = $thr1->join;
	($bdc, $delb) = $thr2->join;
    }
    else
    {
	($bic, $insb) = get_bulk_keys($dbh1, "${name1}0", 'INSERT', @$bins);
	($bdc, $delb) = get_bulk_keys($dbh2, "${name2}0", 'DELETE', @$bdel);
    }
}

# update count with bulk contents
$count += $bic + $bdc;

# final stuff:
# $count: number of differences found
# @$ins @$insb: key insert (individuals and bulks)
# @$upd: key update
# @$del @$delb: key delete (ind & bulks)

print STDERR "# done, $count differences found...\n" if $verb; 
$dbh1->disconnect();  $dbh2->disconnect();

$tblk = [gettimeofday] if $stats;

if ($stats)
{
    # summary of performances
    print 
	"table count: $size\n",
	"folding factor: $factor\n",
	"        levels: ", scalar @masks, " (cut-off from $levels)\n",
	"query number: $query_nb\n",
	"    and size: $query_sz\n",
	"fetched rows: $query_fr\n",
	"differences found: $count\n",
	"total time: ", tv_interval($t0, $tblk), "\n",
	"  checksum: ", tv_interval($t0, $tcks), "\n",
	"   summary: ", tv_interval($tcks, $tsum), "\n",
	"     merge: ", tv_interval($tsum, $tmer), "\n",
	"     bulks: ", tv_interval($tmer, $tblk), "\n";
}
