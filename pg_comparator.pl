#! /usr/bin/perl -w
#
# $Id: pg_comparator.pl 699 2010-04-03 13:54:25Z fabien $
#
# HELP 1: pg_comparator --man
# HELP 2: pod2text pg_comparator
# HELP 3: read pod stuff bellow with your favorite text viewer
#

=head1 NAME

B<pg_comparator> - network and time efficient table content comparison.

=head1 SYNOPSIS

B<pg_comparator> [options as B<--help> B<--option> B<--man>] conn1 conn2

=head1 DESCRIPTION

This script performs a network and time efficient comparison of two
possibly large tables on two servers. It makes only sense to use it
if the expected differences are relatively small. Optionally, it can
synchronize the tables.

The implementation is quite generic: multi-column keys, no assumption
of data types other that they can be cast to text, subset of columns
can be used for the comparison, handling of NULL values...

=head1 OPTIONS

Options allow to request help or to adjust some internal parameters.
Short one-letter options are also available, usually with the first letter
of the option name.

=over 4

=item C<--aggregate=agg> or C<-a agg>

Aggregation function to be used for summaries, 'xor' or 'sum'.
Must operate on the result of the checksum function.
Default is 'xor'.

=item C<--ask-pass>

Ask for passwords interactively.

=item C<--assume-size=...>

Assume the provided value as the table size, thus skipping the COUNT query.

=item C<--checksum-function=fun> or C<--cf=fun> or C<-c fun>

Checksum function to use. Must be B<ck> or B<md5>. Default is B<ck>, which
is faster, especially if the operation is cpu-bound and the bandwidth is high.

=item C<--checksum-size=n> or C<--check-size=n> or C<--cs=n> or C<-z n>

Checksum size, 2 4 or 8 bytes. Default is 8.

=item C<--cleanup>

Drop checksum and summary tables beforehand.
Useful after a run with C<--no-temp>.

=item C<--clear>

Drop checksum and summary tables explicitely after the computation.
Note that they are dropped implicitely by default when the connection
is closed because they are temporary.

=item C<--expect n> or C<-e n>

Total number of differences to expect (updates, deletes and inserts),
used for non regression tests.

=item C<--folding-factor=8> or C<-f 8>

Folding factor: log2 of the number of rows grouped together at each stage.
Default chosen after some basic tests on medium-size cases.
The minimum value of 1 builds a binary tree.
Good values for large databases and low bandwidth is 10 to 12.

=item C<--help> or C<-h>

Show short help.

=item C<--man> or C<-m>

Show manual page.

=item C<--max-report=1024>

Maximum search effort, search will stop if above this threshold:
it gives up if a single query at any level involves more than this
many differences. Use 0 for no limit.

=item C<--max-levels=0>

Maximum number of levels used. Allows to cut-off folding.
Default is 0, meaning no cut-off. Setting a value of 1
only uses the checksum table, without summaries.

=item C<--null=how>

How to handle NULL values. Either B<hash> to hash all values, where NULL
has one special hash value, or B<text> where NULL values are substituted
by the C<NULL> string. Default is B<text> because it is faster.

=item C<--option> or C<-o>

Show option summary.

=item C<--prefix=cmp>

Name prefix for comparison tables. May be schema-qualified.

=item C<--report>, C<--no-report>

Report keys as they are found. Default is to report.

=item C<--separator=:> or C<-s :>

Separator string or character when concatenating key columns.
This character should not appear in the values.

=item C<--temporary>, C<--no-temporary>

Whether to use temporary tables. Default is to use.
If you don't, the tables are kept at the end, so they will have
to be deleted by hand.

=item C<--threads> or C<-T>

Use threads: a synonymous for "segmentation fault":-)
It seems that DBI or DBD::Pg does not like threads at all...
See bug report L<http://rt.cpan.org/Public/Bug/Display.html?id=55755>.

=item C<--stats>

Show various statistics.

=item C<--synchronize> or C<-S>

Actually perform operations to synchronize the second table wrt the first.
Well, not really. It is only done if you add C<--do-it> or C<-D>.
Save your data before attempting anything like that!

=item C<--use-key> or C<-u>

Whether to use the key of the tables to distribute tuples among branches.
The key must be simple, integer, not NULL, and evenly distributed.
Default is to hash the key, so as to handle any type, composition and
distribution.

=item C<--transaction>, C<--no-transaction>

Whether to wrap the whole algorithm in a single transaction.
It may be a little quicker in a transaction, so it is the default.

=item C<--verbose>

Be verbose about what is happening. The more you ask, the more verbose.

=item C<--version>

Show version information.

=item C<--where=...>

SQL boolean condition on table tuples for partial comparison.
Useful to reduce the load if you know that expected differences are in
some particular parts of your data, say those timestamped today...

=back

=head1 ARGUMENTS

The two arguments describe database connections with the following URL-like
syntax, where square brackets denote optional parts. Many parts are optional
with a default. The minimum syntactically correct specification is C</>.
See also the EXAMPLES section bellow.

  [driver://][login[:pass]@][host[:port]]/[base/[[schema.]table[?key:cols]]]

=over 4

=item B<driver>

Database driver to use. Use B<pgsql> for PostgreSQL, and B<mysql> for MySQL.
Heterogeneous databases may be compared and synchronized, however beware that
subtle typing, encoding and casting issues may prevent heterogeneous
comparisons or synchronizations to succeed.
Default is B<pgsql>.

=item B<login>

Login to use when connecting to database. Default is username for first
connection, and same as first connection for second.

=item B<pass>

Password to use when connecting to database.
Note that it is a bad idea to put a password as an command argument.
Default is none for the first connection, and the same password
as the first connection for the second I<if> the connection targets
the same host, port and uses the same login.
See also B<--ask-pass> option.

=item B<host>

Hostname to connect to. Default is localhost.

=item B<port>

Tcp-ip port to connect to.
Default is 5432 for PostgreSQL and 3306 for MySQL.

=item B<base>

Database catalog to connect to. Default is username for first connection.
Default is same as first connection for second connection.

=item B<schema.table>

The possibly schema-qualified table to use for comparison.
No default for first connection.
Default is same as first connection for second connection.

=item B<keys>

Comma-separated list of key columns.
No default for first connection.
Default is same as first connection for second connection.

=item B<cols>

Comma-separated list of columns to compare.
May be empty. No default for first connection.
Default is same as first connection for second connection.

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

=item 2

A checksum function must be used to reduce and distribute key
and columns values. It may be changed with the B<--checksum> option.
Its size can be selected with the B<--checksize> option (2, 4 or 8 bytes).

Suitable implementations are available for PostgreSQL and can be loaded into
the server by processing C<share/contrib/checksum.sql>. New checksums and
casts are also available for MySQL, see C<mysql_*.sql>.

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

C<DBI>,
C<DBD::Pg> to connect to PostgreSQL,
and C<DBD::mysql> to connect to MySQL.

=item

C<Term::ReadPassword> for ask-pass option.

=item

C<Pod::Usage> for doc self-extraction (--man --opt --help).

=back

=head1 EXAMPLES

Compare tables calvin and hobbes in database family on localhost,
with key I<id> and columns I<c1> and I<c2>:

  ./pg_comparator /family/calvin?id:c1,c2 /family/hobbes

Compare tables calvin in default database on localhost and the same
table in default database on sablons, with key I<id> and column I<data>:

  ./pg_comparator localhost/family/calvin?id:data sablons/

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

NULL values must be taken care appropriatelly.

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

B<Taming the Distributed Database Problem: A Case Study Using MySQL>
by I<Giuseppe Maxia> in B<Sys Admin> vol 13 num 8, Aug 2004, pp 29-40.
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

I<Michael Nacos> made a robust implementation L<http://pgdba.net/pg51g/>
based on triggers. He also noted that although database contents are compared
by the algorithm, the database I<structure> differences can also be detected
by comparing system tables which describe these.

I<Benjamin Mead Vandiver>'s PhD Thesis
B<Detecting and Tolerating Byzantine Faults in Database Systems>,
Massachusset's Institute of Technology, May 2008
(report number MIT-CSAIL-TR-2008-040).
There is an interesting discussion in Chapter 7, and experiments are presented
on a Java/JDBC/MySQL implementation of several algorithms.

Some products or projects implement such features, for instance:
L<http://code.google.com/p/maatkit/> (mk-table-sync, by I<Baron Schwartz>,
see L<http://tinyurl.com/mysql-data-diff-algorithm>)
L<http://www.programurl.com/software/sql-server-comparison.htm>
L<http://www.dbbalance.com/db_comparison.htm>
L<http://www.dkgas.com/dbdiff.htm>
L<http://www.sql-server-tool.com/>
L<http://sourceforge.net/projects/mysqltoolkit>
L<http://www.citrustechnology.com/solutions/data-comparison>
L<http://www.dbsolo.com/datacomp.html>
L<http://comparezilla.sourceforge.net/>
L<http://www.webyog.com/>

=head1 BUGS

All softwares have bugs. This is a software, hence it has bugs.

Reporting bugs is good practice, so tell me if you find one!

The implementation does not do many sanity checks.
For instance, it does not check that the declared key is indeed a key.

Do not attempt to synchronize while the table is being used!
Maybe I should lock the table?

Although the algorithm can work with some normalized columns
(say strings are trimmed, lowercased, unicode normalized...),
the implementation may not work at all.

Tables with binary keys or with NULL in keys may not work.

=head1 VERSIONS

See L<http://pgfoundry.org/projects/pg-comparator/> for the latest version.
My web site for the tool is L<http://www.coelho.net/pg_comparator/>.

=over 4

=item B<version @VERSION@> @DATE@ (r@REVISION@)

Add more functions (MD5, SUM) and sizes (2, 4, 8).
Remove template parametrization which is much too fragile to expose.
Add a wrapping transaction which may speed up things a little.
Implementation for MySQL, including synchronizing heterogeneous databases.
Improved documentation. Extensive validation/non regression tests.

=item B<version 1.5.2> 2010-03-22 (r564)

More documentation.
Improved connection parsing with more sensible defaults.
Make the mask computation match its above documentation with a bottom-up
derivation, instead of a simpler top-down formula which results in bad
performances when a power of the factor is close to the size (as pointed
out in I<Benjamin Mead Vandiver>'s PhD).
This bad mask computation was introduced somehow between 1.3 and 1.4 as
an attempt at simplifying the code.

=item B<version 1.5.1> 2010-03-21 (r525)

More documentation.
Add C<--expect> option for non regression tests.

=item B<version 1.5.0> 2010-03-20 (r511)

Add more links.
Fix so that with a key only (i.e. without additional columns), although
it could be optimized further in this case.
Integrate patch by I<Erik Aronesty>: More friendly "connection parsor".
Add synchronization option to actually synchronize the data.

=item B<version 1.4.4> 2008-06-03 (r438)

Manual connection string parsing.

=item B<version 1.4.3> 2008-02-17 (r424)

Grumble! wrong tar pushed out.

=item B<version 1.4.2> 2008-02-17 (r421)

Minor makefile fix asked for by I<Roberto C. Sanchez>.

=item B<version 1.4.1> 2008-02-14 (r417)

Minor fix for PostgreSQL 8.3 by I<Roberto C. Sanchez>.

=item B<version 1.4> 2007-12-24 (r411)

Port to PostgreSQL 8.2. Better documentation.
Fix mask bug: although the returned answer was correct, the table folding
was not.
DELETE/INSERT messages exchanged so as to match a 'sync' or 'copy' semantics,
as suggested by I<Erik Aronesty>.

=item B<version 1.3> 2004-08-31 (r239)

Project moved to L<http://pgfoundry.org/>.
Use cksum8 checksum function by default.
Minor doc updates.

=item B<version 1.2> 2004-08-27 (r220)

Added B<--show-all-keys> option for handling big chunks of deletes
or inserts.

=item B<version 1.1> 2004-08-26 (r210)

Fix algorithmic bug: checksums B<must> also include the key,
otherwise exchanged data could be not detected if the keys were
to be grouped together.

Algorithmic section added to manual page.
Thanks to I<Giuseppe Maxia> who asked for it.

Various code cleanups.

=item B<version 1.0> 2004-08-25  (r190)

Initial revision.

=back

=head1 COPYRIGHT

Copyright (c) 2004-@YEAR@, Fabien Coelho <pg dot comparator at coelho dot net>
L<http://www.coelho.net/>

This softwere is distributed under the terms of the BSD Licence.
Basically, you can do whatever you want, but you have to keep
the license... and I'm not responsible for any consequences.
Beware, you may lose your data or your hairs because of this software!
See the LICENSE file enclosed with the distribution for details.

If you are very happy with this software, I would appreciate a postcard
saying so (see my webpage for current address).

=cut

use strict; # I don't like perl;-)
use Getopt::Long qw(:config no_ignore_case);
use DBI;

my $script_version = '@VERSION@ (r@REVISION@)';

################################################################# SOME DEFAULTS

# various option defaults
my ($verb, $debug, $temp, $ask_pass) = (0, 0, 1, 0);
my ($factor, $max_report, $max_levels, $usekey) =  (8, 1024, 0, 0);
my ($report, $threads, $cleanup, $stats, $skip, $clear) = (1, 0, 0, 0, 0, 0);
my ($synchronize, $do_it, $do_transaction) = (0, 0, 1);
my $prefix = 'cmp';
my ($where, $expect);

# algorithm defaults
# hmmm... could rely on base64 to handle binary keys?
my ($null, $checksum, $checksize, $agg, $sep) = ('text', 'ck', 8, 'xor', ':');


######################################################################### UTILS

# self extracting help
# usage(verbosity, exit value, message)
sub usage($$$)
{
  my ($verbose,$stat,$msg) = @_;
  print STDERR "ERROR: $msg\n" if $msg;
  require Pod::Usage;
  Pod::Usage::pod2usage(-verbose => $verbose, -exitval => $stat);
}

# show message depending on verbosity
# verb(2, "something...")
sub verb($$)
{
  my ($level,$msg) = @_;
  print STDERR '#' x $level, " $msg\n" if $level<=$verb;
}

#################################################################### CONNECTION

my ($dbh1, $dbh2);

# parse a connection string... or many options instead?
# could we directly ask for the DBI connection string?
# ($db,$u,$w,$h,$p,$b,$t,$k,$c) = parse_conn("connection-string")
# globals: $verb
# pgsql://calvin:secret@host:5432/base/schema.table?key:col,list
sub parse_conn($)
{
  my $c = shift;
  my ($db, $user, $pass, $host, $port, $base, $tabl, $keys, $cols);

  # get driver name
  if ($c =~ /^pg(sql)?:\/\//i) {
    $db = 'pgsql';
    $c =~ s/^\w+:\/\///;
  }
  elsif ($c =~ /^my(sql)?:\/\//i) {
    $db = 'mysql';
    $c =~ s/^\w+:\/\///;
  }
  else {
    # default is PostgreSQL
    $db = 'pgsql';
  }

  # split authority and path
  die "invalid connection string '$c', must contain '\/'\n"
    unless $c =~ /^([^\/]*)\/(.*)/;

  my ($auth, $path) = ($1, $2);

  if ("$auth")
  {
    # parse authority if non empty. ??? url-translation?
    die "invalid authority string '$auth'\n"
      unless $auth =~ /^((\w+)(:([^.]*))?\@)?([^\@:\/]+)(:(\d+))?$/;

    $user=$2 if defined $1;
    $pass=$4 if defined $3;
    $host=$5;
    $port=$7 if defined $6;
    verb 3, "user=$user pass=$pass host=$host port=$port" if $debug;
  }

  if ("$path")
  {
    # parse path base/schema.table?key,part:column,list,part
    die "invalid path string '$path'\n"
      unless $path =~ /^(\w+)?(\/((\w+\.)?\w+))?(\?([\w\,]+)(\:([\w,]*))?)?$/;

    $base=$1 if defined $1;
    $tabl=$3 if defined $2;
    $keys=$6 if defined $5;
    $cols=$8 if defined $7;

    verb 3, "base=$base tabl=$tabl keys=$keys cols=$cols" if $debug;
  }

  # return result as a list
  my @res = ($db,$user,$pass,$host,$port,$base,$tabl,$keys,$cols);
  verb 2, "connection parameters: @res" if $debug;
  return @res;
}

# return connection template for database
sub source_template($)
{
  my ($db) = @_;
  if ($db eq 'pgsql') {
    return 'DBI:Pg:dbname=%b;host=%h;port=%p;';
  }
  elsif ($db eq 'mysql') {
    return 'DBI:mysql:database=%b;host=%h;port=%p;';
  }
  else {
    die "unexpected db ($db)";
  }
}

# $dbh = conn($db,$base,$host,$port,$user,$pass)
# globals: $verb
sub conn($$$$$$)
{
  my ($db,$b,$h,$p,$u,$w) = @_;
  my $s = source_template($db);
  $s =~ s/\%b/$b/g;  $s =~ s/\%h/$h/g;  $s =~ s/\%p/$p/g;  $s =~ s/\%u/$u/g;
  verb 3, "connecting to s=$s u=$u";
  my $dbh = DBI->connect($s, $u, $w,
		{ RaiseError => 1, PrintError => 0, AutoCommit => 1 });
  verb 4, "connected to $u\@$h:$p/$b";
  # start a big transaction...
  $dbh->begin_work if $do_transaction;
  return $dbh;
}

# global counter
my $query_nb = 0; # number of queries
my $query_sz = 0; # size of queries
my $query_fr = 0; # fetched rows

# sql_do($dbh, $query)
# execute an SQL query on a database
# side effects: keep a count of queries and communications
sub sql_do($$)
{
  my ($dbh, $query) = @_;
  $query_nb++;
  $query_sz += length($query);
  verb 3, "$query_nb\t$query";
  return $dbh->do($query);
}

# execute a parametric statement with col & key values
sub sth_param_exec($$$@)
{
  my ($doit, $sth, $keys, @cols) = @_;
  my $verbose = $verb>2;
  my $index = 1;
  print STDERR '### binding ' if $verbose;
  for my $val (@cols, split(/$sep/, $keys)) {
    $sth->bind_param($index++, $val) if $doit;
    print STDERR $dbh1->quote($val), ', ' if $verbose;
  }
  print STDERR "\n" if $verbose;
  $sth->execute() if $doit;
}

####################################################################### QUERIES

# $number_of_rows = count($dbh,$table)
sub count($$)
{
  my ($dbh, $table) = @_;
  # ??? count is not counted!
  return $dbh->selectrow_array("SELECT COUNT(*) FROM $table" .
			       (defined $where? " WHERE $where": ''));
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
  return @_;
}

# returns an sql concatenation of fields
# $sql = concat($db, $null, $sep, $string_of_comma_separated_fields)
sub concat($$$$)
{
  my ($db, $null, $sep, $list) = @_;
  if ($db eq 'pgsql') {
    return join("||'$sep'||", subs($null, split(/,/, $list)));
  }
  elsif ($db eq 'mysql') {
    return 'CONCAT(' . join(",'$sep',", subs($null, split(/,/,$list))) . ')';
  }
  die "unexpected db $db";
}

# return template
sub null_template($$$$)
{
  my ($db, $null, $algo, $size) = @_;
  if ($db eq 'pgsql') {
    if ($null eq 'text') {
      return "COALESCE(%s::TEXT,'NULL')"
    }
    elsif ($null eq 'hash') {
      return 'COALESCE(' . cksm_template($db, $algo, $size) . ',0)'
    }
    die "unexpected null $null";
  }
  elsif ($db eq 'mysql') {
    if ($null eq 'text') {
      return "COALESCE(CAST(%s AS BINARY),'NULL')"
    }
    elsif ($null eq 'hash') {
      return 'COALESCE(' . cksm_template($db, $algo, $size) . ',0)'
    }
    die "unexpected null $null";
  }
  die "unexpected db $db";
}

sub cast_size($$$)
{
  my ($db, $s, $size) = @_;
  if ($db eq 'pgsql') {
    return "${s}::INT$size";
  }
  elsif ($db eq 'mysql') {
    return "biginttoint$size(CAST($s AS SIGNED))";
  }
  die "unexpected db $db";
}

# return checksum template for a non-NULL string.
sub cksm_template($$$)
{
  my ($db, $algo, $size) = @_;
  if ($db eq 'pgsql') {
    if ($algo eq 'md5') {
      return cast_size($db,
		       "DECODE(MD5(%s::TEXT),'hex')::BIT(" . 8*$size . ")",
		       $size);
    }
    elsif ($algo eq 'ck') {
      return "CKSUM${size}((%s)::TEXT)";
    }
    die "unexpected algo $algo";
  }
  elsif ($db eq 'mysql') {
    if ($algo eq 'md5') {
      return cast_size($db, "CONV(LEFT(MD5(%s),". 2*$size ."),16,10)", $size);
    }
    elsif ($algo eq 'ck') {
      return "CKSUM${size}(CAST(%s AS BINARY))";
    }
    die "unexpected algo=$algo";
  }
  die "unexpected db $db";
}

# checksum/hash one or more attributes
sub ckatts($$$$$)
{
  my ($db, $null, $algo, $size, $atts) = @_;
  if ($db eq 'pgsql') {
    if ($atts =~ /,/) {
      return join '', subs(cksm_template($db, $algo, $size),
	  concat($db, null_template($db, $null, $algo, $size), $sep, $atts));
    }
    else {
      # simpler version when there is only one attribute...
      if ($algo eq 'md5') {
	return cast_size($db,
		   "COALESCE(DECODE(MD5(${atts}::TEXT),'hex'),''::BYTEA)" .
			 "::BIT(" .  8*$size . ")", $size);
      }
      else {
	  return "CKSUM$size(${atts}::TEXT)";
      }
    }
  }
  elsif ($db eq 'mysql') {
    if ($atts =~ /,/) {
      return join '', subs(cksm_template($db, $algo, $size),
	  concat($db, null_template($db, $null, $algo, $size), $sep, $atts));
    }
    else {
      # simpler version when there is only one attribute...
      if ($algo eq 'md5') {
	return cast_size($db,
	      "COALESCE(CONV(LEFT(MD5(${atts}),". 2*$size ."),16,10),0)",
			 $size);
      }
      else {
	return "CKSUM${size}(CAST(${atts} AS BINARY))";
      }
    }
  }
  die "not implemented yet for db $db";
}

# $count = compute_cheksum($dbh,$table,$keys,$cols,$name,$skip)
# globals: $temp $verb $cleanup $null $checksum $checksize
sub compute_cheksum($$$$$$$)
{
  my ($dbh, $db, $table, $keys, $cols, $name, $skip) = @_;
  verb 2, "building checksum table ${name}0";
  sql_do($dbh, "DROP TABLE IF EXISTS ${name}0") if $cleanup;
  # ??? CREATE + INSERT SELECT to get row count?
  # would also allow to choose better types (int2/int4/int8...)?
  sql_do($dbh,
	 "CREATE ${temp}TABLE ${name}0 AS " .
	 "SELECT " .
	 ($usekey? "$keys AS idc, ":
	  concat($db, null_template($db,'text',0,0), $sep, $keys)." AS id, ") .
	 # always use 4 bytes for hash(key), because mask is 4 bytes
	 ($usekey? '': ckatts($db, $null, $checksum, 4, $keys) . " AS idc,") .
	 # this could be skipped if cols is empty...
	 # it would be somehow redundant with the previous one if same size
	 ckatts($db, $null, $checksum, $checksize,
		"$keys" . ($cols?",$cols": '')) . " AS cks " .
	 "FROM $table " .
	 ($where? "WHERE $where": ''));
  # count should be available somewhere?
  return $skip? 0: count($dbh, "${name}0");
}

sub aggregate($$)
{
  my ($db,$agg) = @_;
  return 'bit_xor' if $db eq 'mysql' and $agg eq 'xor';
  return $agg;
}

# compute_summaries($dbh, $name, @masks)
# globals: $verb $temp $agg $cleanup
sub compute_summaries($$$@)
{
  my ($dbh, $db, $name, @masks) = @_;
  # compute cascade of summary tables
  for my $level (1 .. @masks-1) {
    verb 2, "building summary table ${name}$level ($masks[$level])";
    sql_do($dbh, "DROP TABLE IF EXISTS ${name}${level}") if $cleanup;
    sql_do($dbh,
	   "CREATE ${temp}TABLE ${name}${level} AS " .
	   # the "& mask" is really a modulo operation
	   "SELECT idc & $masks[$level] AS idc, " .
	   aggregate($db, $agg) . "(cks) AS cks " .
	   "FROM ${name}" . ($level-1) . " " .
	   "GROUP BY idc & $masks[$level]");
  }
}

# get info for investigated a list of idc (hopefully not too long)
# $sth = selidc($dbh,$table,$mask,$get_id,@idc)
sub selidc($$$$$@)
{
  my ($dbh, $db, $table, $mask, $get_id, @idc) = @_;
  my $query =
      'SELECT idc, cks' . ($get_id? ($usekey? ', idc': ', id'): '') .
      " FROM $table ";
  # the "& mask" is really a modulo operation
  $query .= "WHERE idc & $mask IN (" . join(',', @idc) . ') ' if @idc;
  $query .= 'ORDER BY idc, cks';
  my $sth = $dbh->prepare($query);
  $query_nb++;
  $query_sz += length($query);
  verb 3, "$query_nb\t$query";
  $sth->execute();
  return $sth;
}

# investigate an "idc/mask" list to show corresponding keys.
# get_bulk_keys($dbh, $table, $nature, @idc_masks)
# globals: $verb $report
sub get_bulk_keys($$$$@)
{
  my ($dbh, $db, $table, $nature, @idc_masks) = @_;
  return (0, undef) unless @idc_masks; # bye if nothing to investigate
  my @keys = (); # results
  my $cond = ''; # select query condition. must not be empty.
  verb 1, "investigating $nature chunks";
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
  verb 3, "$query_nb\t$query";
  $sth->execute();
  while (my @row = $sth->fetchrow_array()) {
    $count ++;
    push @keys, $row[0];
    print "$nature @row\n" if $report;
  }
  return ($count, \@keys);
}

############################################################### MERGE ALGORITHM

# compute differences by climbing up the tree, output result on the fly.
# differences($dbh1, $dbh2, $db1, $db2, $name1, $name2, @masks)
# globals: $max_report $verb $report
sub differences($$$$$$@)
{
  my ($dbh1, $dbh2, $db1, $db2, $name1, $name2, @masks) = @_;
  my $level = @masks-1; # number of last summary table
  my ($mask, $count,$todo) = (0, 0, 1); # mask of previous table
  my (@insert, @update, @delete, @mask_insert, @mask_delete); # results
  my @idc = ();

  while ($level>=0 and $todo)
  {
    my @next_idc = ();
    verb 3, "investigating level=$level (@idc)";

    if ($max_report && @idc>$max_report) {
      print "giving up at level $level: too many differences.\n" .
	    "\tadjust max option (current is $max_report) to proceed.\n" .
	    "\tidc list length is " . scalar @idc . ": @idc\n";
      return;
    }

    # select statement handlers
    my $s1 = selidc($dbh1, $db1, ${name1}.$level, $mask, !$level, @idc);
    my $s2 = selidc($dbh2, $db2, ${name2}.$level, $mask, !$level, @idc);
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
    $s1->finish();
    $s2->finish();
    $level--; # next table! 0 is the initial checksum table
    $mask = pop @masks; # next mask
    @idc = @next_idc; # idcs to be investigated on next round
    $todo = @idc;
  }

   return ($count, \@insert, \@update, \@delete, \@mask_insert, \@mask_delete);
}

####################################################################### OPTIONS

# option management
GetOptions(
  # help
  "help|h" => sub { usage(0, 0, ''); },
  "options|option|o" => sub { usage(1, 0, ''); },
  "manual|man|m" => sub { usage(2, 0, ''); },
  # verbosity
  "verbose|v+" => \$verb,
  "debug|d" => \$debug,
  # parametrization
  "checksum-function|checksum|cf|c=s" => \$checksum,
  "checksum-size|check-size|checksize|cs|z=i" => \$checksize,
  "aggregate-function|aggregate|agg|af|a=s" => \$agg,
  "null|n=s" => \$null,
  "where|w=s" => \$where,
  "separator|s=s" => \$sep,
  # algorithm parameters
  "use-key|uk|u" => \$usekey,
  "assume-size|as=i" => \$skip,
  "folding-factor|factor|f=i" => \$factor,
  "maximum-report|max-report|mr|x=i" => \$max_report,
  "maximum-levels|max-levels|ml=i" => \$max_levels,
  # table
  "temporary|temp|tmp|t!" => \$temp,
  "cleanup!" => \$cleanup,
  "clear!" => \$clear,
  "prefix|p=s" => \$prefix,
  # connection
  "ask-pass|ap!" => \$ask_pass,
  "transaction|trans|tr!" => \$do_transaction,
  # functions
  "synchronize|sync|S!" => \$synchronize,
  "do-it|do|D!" => \$do_it,
  "expect|e=i" => \$expect,
  "report|r!" => \$report,
  # misc
  "threads|T!" => \$threads,
  "statistics|stats!" => \$stats,
  "version|V" => sub { print "$0 version is $script_version\n"; }
) or die "$! (try $0 --help)";

# fix --temp or --no-temp option
$temp = $temp? 'TEMPORARY ': '';

# fix factor size
$factor = 1 if $factor<1;
$factor = 30 if $factor>30;

# intermediate table names
my ($name1, $name2) = ("${prefix}_1_", "${prefix}_2_");

# argument management
usage(0, 0, 'expecting 2 arguments') unless @ARGV == 2;

# first connection
my ($db1, $u1, $w1, $h1, $p1, $b1, $t1, $k1, $c1) = parse_conn(shift);

# set defaults and check minimum definitions.
$u1 = $ENV{USER} unless defined $u1;
$h1 = 'localhost' unless defined $h1;
$p1 = 5432 if not defined $p1 and $db1 eq 'pgsql';
$p1 = 3306 if not defined $p1 and $db1 eq 'mysql';

# these are necessary
die "no base on first connection" unless defined $b1;
die "no table on first connection" unless defined $t1;
die "no key on first connection" unless defined $k1;
$c1 = '' unless defined $c1;

# second connection
my ($db2, $u2, $w2, $h2, $p2, $b2, $t2, $k2, $c2) = parse_conn(shift);

# fix some default values for connection 2
$u2 = $u1 unless defined $u2;
$h2 = 'localhost' unless defined $h2;
$p2 = 5432 if not defined $p2 and $db2 eq 'pgsql';
$p2 = 3306 if not defined $p2 and $db2 eq 'mysql';
$w2 = $w1 unless $w2 or not $w1 or $u1 ne $u2 or $h1 ne $h2 or $p1!=$p2;
$b2 = $b1 unless defined $b2;
$t2 = $t1 unless defined $t2;
$k2 = $k1 unless defined $k2;
$c2 = $c1 unless defined $c2;

# build list of attributes
my @k1 = split ',', $k1;
my @k2 = split ',', $k2;
my @c1 = split ',', $c1;
my @c2 = split ',', $c2;

# more sanity checks
die "key number of attributes does not match" unless @k1 == @k2;
die "column number of attributes does not match" unless @c1 == @c2;

die "null should be 'text' or 'hash', got $null"
    unless $null =~ /^(text|hash)$/;

die "checksum should be 'md5' or 'ck', got ($checksum)"
    unless $checksum =~ /^(md5|ck)$/i;

die "checksize must be 2, 4 or 8, got ($checksize)"
    unless $checksize =~ /^[248]$/;

die "aggregate must be 'xor' or 'sum', got ($agg)"
    unless $agg =~ /^(xor|sum)$/i;

die "use-key option requires a simple integer key, got (@k1) and (@k2)"
    if $usekey and @k1 != 1;

# database connection...
if ($ask_pass)
{
  require Term::ReadPassword;
  $w1 = Term::ReadPassword::read_password('connection 1 password> ');
  # hmmm... I could reuse the previous pass?
  # what about on demand, when the connection fails?
  $w2 = Term::ReadPassword::read_password('connection 2 password> ');
}

############################################################ THREADED FUNCTIONS

# let us work on both data in parallel
sub conn_and_cksum($$$$$$$$$$$)
{
  my ($db, $b, $h, $p, $u, $w, $t, $k, $c, $n, $skip) = @_;
  verb 2, "connecting...";
  my $dbh = conn($db, $b, $h, $p, $u, $w);
  verb 2, "checksumming...";
  my $count = compute_cheksum($dbh, $db, $t, $k, $c, $n, $skip);
  return $dbh, $count;
}

use Time::HiRes qw(gettimeofday tv_interval);
my ($t0, $tcks, $tsum, $tmer, $tblk, $tsyn, $tclr, $tend);
$t0 = [gettimeofday] if $stats;

verb 1, "connection and checksum...";
my ($thr1, $count1, $thr2, $count2);
if ($threads)
{
  require threads;
  ($thr1) = threads->new(\&conn_and_cksum, $db1,
       $b1, $h1, $p1, $u1, $w1, $t1, $k1, $c1, $name1, $skip)
    or die "cannot create thread 1-1";

  ($thr2) = threads->new(\&conn_and_cksum, $db2,
       $b2, $h2, $p2, $u2, $w2, $t2, $k2, $c2, $name2, $skip)
    or die "cannot create thread 2-1";

  verb 1, "waiting for connexions and counts...";
  ($dbh1, $count1) = $thr1->join();
  ($dbh2, $count2) = $thr2->join();
}
else
{
  ($dbh1, $count1) = conn_and_cksum($db1,
      $b1, $h1, $p1, $u1, $w1, $t1, $k1, $c1, $name1, $skip);

  ($dbh2, $count2) = conn_and_cksum($db2,
      $b2, $h2, $p2, $u2, $w2, $t2, $k2, $c2, $name2, $skip);
}

verb 1, "computing size and masks after folding factor...";
$count1 = $count2 = $skip if $skip;

# ??? |count2-count1|>max_diff => abort ?

my $size = $count1>$count2? $count1: $count2; # max?
my ($mask, $nbits, @masks) = (0, 0);
while ($mask < $size) {
  $mask = 1+($mask<<1);
  $nbits++;
}
push @masks, $mask; # this is the full mask, which is skipped later on
while ($mask) {
  $mask >>= $factor;
  push @masks, $mask;
}
my $levels = @masks;
splice @masks, $max_levels if $max_levels; # cut-off
verb 3, "masks=@masks";

$tcks = [gettimeofday] if $stats;

verb 1, "building summary tables...";
if ($threads)
{
  $thr1 = threads->new(\&compute_summaries, $dbh1, $db1, $name1, @masks)
    or die "cannot create thread 1-2";

  $thr2 = threads->new(\&compute_summaries, $dbh2, $db2, $name2, @masks)
    or die "cannot create thread 2-2";

  $thr1->join();
  $thr2->join();
}
else
{
  compute_summaries($dbh1, $db1, $name1, @masks);
  compute_summaries($dbh2, $db2, $name2, @masks);
}

$tsum = [gettimeofday] if $stats;

verb 1, "looking for differences...";
my ($count, $ins, $upt, $del, $bins, $bdel) =
  differences($dbh1, $dbh2, $db1, $db2, $name1, $name2, @masks);

$tmer = [gettimeofday] if $stats;

# now take care of big chunks of INSERT or DELETE if necessary
# should never happen in normal "few differences" conditions
verb 1, "bulk delete: @{$bdel}" if @$bdel;
verb 1, "bulk insert: @{$bins}" if @$bins;

my ($bic, $bdc, $insb, $delb) = (0, 0, undef, undef);
if (@$bins or @$bdel)
{
  verb 1, "resolving bulk inserts and deletes...";
  # this cost two full table-0 scans, one on each side...
  if ($threads)
  {
    # hmmm... useless thread if list is empty
    $thr1 = threads->new(\&get_bulk_keys,
			 $dbh1, $db1, "${name1}0", 'INSERT', @$bins)
      or die "cannot create thread 1-3";

    $thr2 = threads->new(\&get_bulk_keys,
			 $dbh2, $db2, "${name2}0", 'DELETE', @$bdel)
      or die "cannot create thread 2-3";

    ($bic, $insb) = $thr1->join();
    ($bdc, $delb) = $thr2->join();
  }
  else
  {
    ($bic, $insb) = get_bulk_keys($dbh1, $db1, "${name1}0", 'INSERT', @$bins);
    ($bdc, $delb) = get_bulk_keys($dbh2, $db2, "${name2}0", 'DELETE', @$bdel);
  }
}

# update count with bulk contents
$count += $bic + $bdc;

# bulk timestamp
$tblk = [gettimeofday] if $stats;

############################################################### SYNCHRONIZATION

# perform an actual synchronization of data
if ($synchronize and
    (@$del or @$ins or @$upt or defined $insb or defined $delb))
{
  verb 1, "synchronizing...";

  $dbh2->begin_work if $do_it and not $do_transaction;

  my $where_k1 = (join '=? AND ', @k1) . '=?';
  my $where_k2 = (join '=? AND ', @k2) . '=?';
  my $set_c2 = (join '=?, ', @c2) . '=?';

  # deletions
  if (@$del or @$delb)
  {
    my $del_sql = "DELETE FROM $t2 WHERE " .
	($where? "$where AND ": '') . $where_k2;
    verb 2, $del_sql;
    my $del_sth = $dbh2->prepare($del_sql) if $do_it;
    for my $d (@$del, @$delb) {
      sth_param_exec($do_it, $del_sth, $d);
    }
    # undef $del_sth;
  }

  # get values for insert of update
  my ($val_sql, $val_sth);
  if ($c1)
  {
      $val_sql = "SELECT $c1 FROM $t1 WHERE " .
	  ($where? "$where AND ": '') . $where_k1;
      verb 2, $val_sql;
      $val_sth = $dbh1->prepare($val_sql)
	  if $c1 and (@$ins or @$insb or @$upt);
  }

  # insert value
  if (@$ins or @$insb)
  {
    my $ins_sql = "INSERT INTO $t2(" . ($c2? "$c2,":'') . "$k2) VALUES(?" .
	',?' x (@k2+@c2-1) . ')';
    verb 2, $ins_sql;
    my $ins_sth = $dbh2->prepare($ins_sql) if $do_it;
    for my $i (@$ins, @$insb)
    {
      sth_param_exec(1, $val_sth, $i) if $c1;
      sth_param_exec($do_it, $ins_sth, $i,
		     $c1? $val_sth->fetchrow_array(): ());
    }
    #  $ins_sth
  }

  # update
  if (@$upt)
  {
    die "there must be some columns to update" unless $c1;
    my $upt_sql = "UPDATE $t2 SET $set_c2 WHERE " .
	($where? "$where AND ": '') . $where_k2;
    verb 2, $upt_sql;
    my $upt_sth = $dbh2->prepare($upt_sql) if $do_it;
    for my $u (@$upt) {
      sth_param_exec(1, $val_sth, $u);
      sth_param_exec($do_it, $upt_sth, $u, $val_sth->fetchrow_array());
    }
    # $upt_sth
  }

  $dbh2->commit if $do_it and not $do_transaction;

  print
      "\n",
      "*** WARNING ***\n",
      "\n",
      "the synchro was not performed...\n",
      "set option --do-it if you really want to.\n",
      "BEWARE that you may lose your data!\n",
      "\n"
      unless $do_it;
}

$tsyn = [gettimeofday];

sub table_cleanup($$$)
{
  my ($dbh, $name, $levels) = @_;
  for my $i (0 .. $levels) {
    sql_do($dbh, "DROP TABLE ${name}$i");
  }
}

if ($clear)
{
  my $levels = @masks - 1;
  if ($threads)
  {
    $thr1 = threads->new(&table_cleanup, $dbh1, $name1, $levels);
    $thr2 = threads->new(&table_cleanup, $dbh2, $name2, $levels);
    $thr1->join();
    $thr2->join();
  }
  else
  {
    table_cleanup($dbh1, $name1, $levels);
    table_cleanup($dbh2, $name2, $levels);
  }
}

$tclr = [gettimeofday];

# end of the big transactions...
if ($do_transaction)
{
  $dbh1->commit;
  $dbh2->commit;
}

# final stuff:
# $count: number of differences found
# @$ins @$insb: key insert (individuals and bulks)
# @$upd: key update
# @$del @$delb: key delete (ind & bulks)

verb 1, "done, $count differences found...";
$dbh1->disconnect();  $dbh2->disconnect();

#################################################################### STATISTICS

sub delay($$)
{
  my ($t0,$t1) = @_;
  return sprintf "%.6f", tv_interval($t0,$t1);
}

if ($stats)
{
  # final timestamp
  $tend = [gettimeofday];

  # summary of performances/instrumentation
  print
    "   table count: $size\n",
    "folding factor: $factor\n",
    "        levels: ", scalar @masks, " (cut-off from $levels)\n",
    "  query number: $query_nb\n",
    "    query size: $query_sz\n",
    "  fetched rows: $query_fr\n",
    "   diffs found: $count\n",
      defined $expect? "     expecting: $expect\n": '',
    "    total time: ", delay($t0, $tend), "\n",
    "      checksum: ", delay($t0, $tcks), "\n",
    "       summary: ", delay($tcks, $tsum), "\n",
    "         merge: ", delay($tsum, $tmer), "\n",
    "         bulks: ", delay($tmer, $tblk), "\n",
    "       synchro: ", delay($tblk, $tsyn), "\n",
    "         clear: ", delay($tsyn, $tclr), "\n",
    "           end: ", delay($tclr, $tend), "\n";
}

# check count
die "unexpected number of differences (got $count, expecting $expect)"
  if defined $expect and $expect != $count;
