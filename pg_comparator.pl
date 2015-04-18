#!/usr/bin/env perl
#
# $Id: pg_comparator.pl 1540 2015-04-18 06:42:28Z coelho $
#
# HELP 1: pg_comparator --man
# HELP 2: pod2text pg_comparator
# HELP 3: read pod stuff bellow with your favorite text viewer
#

use strict;   # I don't like perl
use warnings; # neither do I trust perl

=head1 NAME

B<pg_comparator> - efficient table content comparison and synchronization

=head1 SYNOPSIS

B<pg_comparator> [options as B<--help> B<--option> B<--man>] conn1 conn2

=head1 DESCRIPTION

This script performs a network and time efficient comparison or
synchronization of two possibly large tables in B<PostgreSQL>, B<MySQL>
or B<SQLite> databases, so as to detect inserted, updated or deleted tuples
between these tables.
The algorithm is efficient especially if the expected differences are
relatively small.

The implementation is quite generic: multi-column keys
(but there must be a key!), no assumption
of data types other that they can be cast to text, subset of columns
can be used for the comparison, handling of NULL values...

This script focuses on the comparison algorithm, hence the many options.
The fact that it may do anything useful, such as checking that a replication
tool does indeed replicates your data, or such as synchronizing tables,
is a mere side effect.

=head1 OPTIONS

Options allow to request help or to adjust some internal parameters.
Short one-letter options are also available, usually with the first letter
of the option name.

=over 4

=item C<--aggregate=(sum|xor)> or C<-a (sum|xor)>

Aggregation function to be used for summaries, either B<xor> or B<sum>.
It must operate on the result of the checksum function.
For PostgreSQL and SQLite, the B<xor> aggregate needs to be loaded.
There is a signed/unsigned issue on the key hash when using B<xor> for
comparing tables on MySQL or SQLite vs PostgreSQL.
We provide a new C<ISUM> aggregate for SQLite because both C<SUM> and C<TOTAL>
do some incompatible handling of integer overflows.

Default is B<sum> because it is available by default and works in mixed mode.

=item C<--ask-pass>

Ask for passwords interactively. See also C<--env-pass> option below.

Default is not to ask for passwords.

=item C<--asynchronous> or C<-A>, C<--no-asynchronous> or C<-X>

Whether to run asynchronous queries. This provides some parallelism, however
the two connections are more or less synchronized per query.

Default is to use asynchronous queries to enable some parallelism.

=item C<--checksum-computation=(create|insert)> or C<--cc=...>

How to create the checksum table.
Use B<create> to use a C<CREATE ... AS SELECT ...> query,
or B<insert> to use a C<CREATE ...; INSERT ... SELECT ...> query.
The former will require an additional counting to get the table size,
so in the end there are two queries anyway.
There is a type size issue with the B<insert> strategy on MySQL, the
cumulated key string length must be under 64 bytes.

Default is B<create> because it always works for both databases.

=item C<--checksum-function=fun> or C<--cf=fun> or C<-c fun>

Checksum function to use, either B<ck>, B<fnv> or B<md5>.
For PostgreSQL, MySQL and SQLite the provided B<ck> and B<fnv> checksum
functions must be loaded into the target databases.
Choosing B<md5> does not come free either: the provided cast functions must be
loaded into the target databases and the computation is more expensive.

Default is B<ck>, which is fast, especially if the operation is cpu-bound
and the bandwidth is reasonably high.

=item C<--checksum-size=n> or C<--check-size=n> or C<--cs=n> or C<-z n>

Tuple checksum size, must be B<2>, B<4> or B<8> bytes.
The key checksum size is always 4 bytes long.

Default is B<8>, so that the false negative probability is very low.
There should be no reason to change that.

=item C<--cleanup>

Drop checksum and summary tables beforehand.
Useful after a run with C<--no-temp> and C<--no-clear>, typically used
for debugging.

Default is not to drop because it is not needed.

=item C<--clear>

Drop checksum and summary tables explicitly after the computation.
Note that they are dropped implicitly by default when the connection
is closed as they are temporary, see C<-(-no)-temporary> option.
This option is useful for debugging.

Default is B<not> to clear explicitly the checksum and summary tables,
as it is not needed.

=item C<--debug> or C<-d>

Set debug mode. Repeat for higher debug levels. See also C<--verbose>.
Beware that some safe gards about option settings are skipped under
debug so as to allow testing under different conditions.

Default is not to run in debug mode.

=item C<--env-pass='var'>

Take password from environment variables C<var1>, C<var2> or C<var>
for connection one, two, or both.
This is tried before asking interactively if C<--ask-pass> is also set.

Default is not to look for passwords from environment variables.

=item C<--expect n> or C<-e n>

Total number of differences to expect (updates, deletes and inserts).
This option is only used for non regression tests. See the TESTS section.

=item C<--folding-factor=7> or C<-f 7>

Folding factor: log2 of the number of rows grouped together at each stage,
starting from the leaves so that the first round always groups as many records
as possible. The power of two allows one to use masked computations.
The minimum value of 1 builds a binary tree.

Default folding factor log2 is B<7>, i.e. size 128 folds.
This default value was chosen after some basic tests on medium-size cases
with medium or low bandwidth. Values from 4 to 8 should be a reasonable
choice for most settings.

=item C<--help> or C<-h>

Show short help.

=item C<--key-checksum='kcs'> or C<--kcs=...>

Use key checksum attribute of this name, which must be already available in
the tables to compare. This option also requires option C<--tuple-checksum>.
See also the EXAMPLES section below for how to set a checksum trigger.
Consider C<--use-key> instead if you already have a reasonably distributed
integer primary key.

Default is to build both key and tuple checksums on the fly.

=item C<--lock>, C<--no-lock>

Whether to lock tables.
Setting the option explicitly overrides the default one way or another.
For PostgreSQL, this option requires C<--transaction>, which is enabled by
default.

Default depends on the current operation: the table is I<not locked> for a
comparison, but it is I<locked> for a synchronization.

=item C<--long-read-len=0> or C<-L 0>

Set max size for fetched binary large objects.
Well, it seems to be ignored at least by the PostgreSQL driver.

Default is to keep the default value set by the driver.

=item C<--man> or C<-m>

Show manual page interactively in the terminal.

=item C<--max-ratio=0.1>

Maximum relative search effort. The search is stopped if the number of results
is above this threshold expressed relatively to the table size.
Use 2.0 for no limit (all tuples were deleted and new ones are inserted).

Default is B<0.1>, i.e. an overall 10% difference is allowed before giving up.

=item C<--max-report=n>

Maximum absolute search effort. The search is stopped if the number of
differences goes beyond this threshold. If set, the previous C<--max-ratio>
option is ignored, otherwise the effort is computed with the ratio once
the table size is known.

Default is to compute the maximum number of reported differences based on
the C<--max-ratio> option, with a mimimum of 100 differences allowed.

=item C<--max-levels=0>

Maximum number of levels used. Allows one to cut-off folding. 0 means no cut-off.
Setting a value of 1 would only use the checksum table, without summaries.
A value of 3 or 4 would be raisonable, as the last levels of the tree are
nice for the theoretical complexity formula, but do not improve performance
in practice.

Default is B<0>.

=item C<--null='text'>

How to handle NULL values. Either B<hash> to hash all values, where NULL
has one special hash value, or B<text> where NULL values are substituted
by the C<NULL> string.

Default is B<text> because it is faster.

=item C<--option> or C<-o>

Show option summary.

=item C<--pg-copy=128>

Experimental option to use PostgreSQL's COPY instead of INSERT/UPDATE
when synchronizing, by chunks of the specified size.

=item C<--prefix='pgc_cmp'>

Name prefix, possibly schema qualified, used for generated comparison tables
by appending numbers to it. Consider changing the prefix if you expect several
comparisons to run concurrently agains the same database.

Default is C<pgc_cmp>. Cheksum tables is named C<pgc_cmp_1_0> and
C<pgc_cmp_2_0>, and summary tables are named by increasing the last number.

=item C<--report>, C<--no-report>

Report differing keys to stdout as they are found.

Default is to report.

=item C<--separator='|'> or C<-s '|'>

Separator string or character used when concatenating key columns for
computing checksums.

Defaults to the pipe '|' character.

=item C<--size=n>

Assume this value as the table size. It is sufficient for the algorithm to
perform well that this size is in the order of magnitude of the actual table
size.

Default is to query the table sizes, which is skipped if this option is set.

=item C<--source-1='DBI:...'>, C<--source-2='...'> or C<-1 '...'>, C<-2 '...'>

Take full control of DBI data source specification and mostly ignore
the comparison authentication part of the source or target URLs.
One can connect with "DBI:Pg:service=backup", use an alternate driver,
set any option allowed by the driver...
See C<DBD::Pg> and C<DBD:mysql> manuals for the various options that can
be set through the DBI data source specification.
However, the database server specified in the URL must be consistent with
this source specification so that the queries' syntax is the right one.

Default is to rely on the two URL arguments.

=item C<--skip-inserts>, C<--skip-updates>, C<--skip-deletes>

When synchronizing, do not perform these operations.

Default under C<--synchronize> is to do all operations.

=item C<--stats=(txt|csv)>

Show various statistics about the comparison performed in this format.
Also, option C<--stats-name> gives the test a name, useful to generate csv
files that will be processed automatically.

Default is B<not> to show statistics, because it requires additional
synchronizations and is not necessarily interesting to the user.

=item C<--synchronize> or C<-S>

Actually perform operations to synchronize the second table wrt the first.
Well, not really, it is only a dry run. It is actually done if you add
C<--do-it> or C<-D>. Save your data before attempting anything like that!

Default is not to synchronize.

=item C<--temporary>, C<--no-temporary>

Whether to use temporary tables. If you don't, the tables are kept by default
at the end, so they will have to be deleted by hand. See C<--clear> option
to request a cleanup. This option is useful for debugging.

Default is to use temporary tables that are automatically wiped out when the
connection is closed.

=item C<--unlogged>, C<--no-unlogged>

Use unlogged tables for storing checksums. These tables are not transactional,
so it may speed up things a little. However, they are not automatically cleaned
up at the end. See C<--clear> option to request a cleanup.

Default is not to use unlogged tables.

=item C<--threads> or C<-T>, C<--no-threads> or C<-N>

Highly EXPERIMENTAL feature.

Try to use threads to perform computations in parallel, with some hocus-pocus
because perl thread model does not really work well with DBI.
Perl threads are rather heavy and slow, more like communicating processes than
light weight threads, really.

This does NOT work at all with PostgreSQL.
It works partially with MySQL, at the price of turning off C<--transaction>.

Default is B<not> to use threads, as it does not work for all databases.

=item C<--timeout n>

Timeout comparison after C<n> seconds.

Default is no timeout. Be patient.

=item C<--transaction>, C<--no-transaction>

Whether to wrap the whole algorithm in a single transaction.

Default is to use a wrapping transaction, as it seems to be both faster and
safer to do so.

=item C<--tuple-checksum='tcs'> or C<--tcs=...>

Use tuple checksum attribute of this name, which must be already available
in the tables to compare. This option requires to set also either C<--use-key>
or C<--key-checksum=...> above. The provided checksum attributes must
not appear in the lists of key and value columns.
See also the EXAMPLES section below for how to set a checksum trigger.

Default is to build both key and tuple checksums on the fly.

=item C<--use-key> or C<-u>

Whether to directly use the value of the key to distribute tuples among
branches. The key must be simple, integer, not NULL, and evenly distributed.
If you have a reasonably spread integer primary key, consider using this option
to avoid half of the checksum table hash computations.

Default is to hash the key, so as to handle any type, composition and
distribution.

=item C<--use-null>, C<--no-use-null>

Whether to use the information that a column is declared NOT NULL to
simplify computations by avoiding calls to COALESCE to handle NULL values.

Default is to use this information, at the price of querying table metadata.

=item C<--verbose> or C<-v>

Be verbose about what is happening. The more you ask, the more verbose.

Default is to be quiet, so that possible warnings or errors stand out.

=item C<--version> or C<-V>

Show version information and exit.

=item C<--where=...>

SQL boolean condition on table tuples for partial comparison.
Useful to reduce the load if you know that expected differences are in
some parts of your data, say those time-stamped today...
The same condition is passed on both sides, so both tables must be pretty
similar so that it works. This is usually the case.

Default is to compare whole tables.

=back

=head1 ARGUMENTS

The two arguments describe database connections with the following URL-like
syntax, where square brackets denote optional parts. Many parts are optional
with a default. The minimum syntactically correct specification is C</>, but
that does not necessary mean anything useful.

  [driver://][login[:pass]@][host][:port]/[base/[[schema.]table[?key[:cols]]]]

See the EXAMPLES section bellow, and also the C<--source-*> options above.

Note that some default value used by DBI drivers may be changed with
driver-specific environment variables, and that DBI also provides its own
defaults and overrides, so what actually happens may not always be clear.
Default values for the second URL are mostly taken from the first URL.

=over 4

=item B<driver>

Database driver to use.
Use B<pgsql> for PostgreSQL, B<mysql> for MySQL, B<sqlite> for SQLite.
Heterogeneous databases may be compared and synchronized, however beware that
subtle typing, encoding and casting issues may prevent heterogeneous
comparisons or synchronizations to succeed.
Default is B<pgsql> for the first connection, and same as first for second.

For SQLite, the authentication part of the URL (login, pass, host, port)
is expected to be empty, thus the full URL should look like:

  sqlite:///base.db/table?key,col:other,columns

Moreover, setting the PGC_SQLITE_LOAD_EXTENSION environment variable with
C<:>-separated shared object files loads these into SQLite.

=item B<login>

Login to use when connecting to database. Default is username for first
connection, and same as first connection for second.

=item B<pass>

Password to use when connecting to database.
Note that it is a bad idea to put a password as a command argument.
Default is none for the first connection, and the same password
as the first connection for the second I<if> the connection targets
the same host, port and uses the same login.
See also C<--ask-pass> and C<--env-pass> options.

=item B<host>

Hostname or IP to connect to.
Default is the empty string, which means connecting to the database on
localhost with a UNIX socket.

=item B<port>

TCP-IP port to connect to.
Default is 5432 for PostgreSQL and 3306 for MySQL.

=item B<base>

Database catalog to connect to. Default is username for first connection.
Default is same as first connection for second connection.
For SQLite, provide the database file name. The path is relative by
default, but can be made absolute by prepending an additional '/':

  sqlite:////var/cache/sqlite/base.db/table?...

=item B<schema.table>

The possibly schema-qualified table to use for comparison.
No default for first connection.
Default is same as first connection for second connection.

Note that MySQL does not have I<schemas>, but strangely enough
their I<database> concept is just like a I<schema>,
so MySQL really does not have I<databases>, although there is
something of that name. Am I clear?

=item B<keys>

Comma-separated list of key columns.
Default is table primary key for first connection.
Default is same as first connection for second connection.
The key B<cannot> be empty. If you do not have a way of identifying
your tuples, then there is no point in looking for differences.

=item B<cols>

Comma-separated list of columns to compare. May be empty.
Default is all columns but B<keys> for first connection.
Default is same as first connection for second connection.
Beware that C<...?key:> means an empty cols, while C<...?key>
sets the default by querying table metadata.

=back

=head1 EXAMPLES

Compare tables calvin and hobbes in database family on localhost,
with key I<id> and columns I<c1> and I<c2>:

  ./pg_comparator /family/calvin?id:c1,c2 /family/hobbes

Compare tables calvin in default database on localhost and the same
table in default database on sablons, with key I<id> and column I<data>:

  ./pg_comparator localhost/family/calvin?id:data sablons/

Synchronize C<user> table in database C<wikipedia> from MySQL on
C<server1> to PostgreSQL on C<server2>.

  ./pg_comparator -S -D --ask-pass \
      mysql://calvin@server1/wikipedia/user pgsql://hobbes@server2/

For PostgreSQL, you may add trigger-maintained key and tuple checksums as:

  -- TABLE Foo(id SERIAL PRIMARY KEY, data ... NOT NULL);
  -- add a key and tuple checksum attributes
  -- the key checksum can be skipped if you use --use-key,
  -- for which the key must be a simple NOT NULL integer.
  ALTER TABLE Foo
    ADD COLUMN key_cs INT4 NOT NULL DEFAULT 0,
    ADD COLUMN tup_cs INT8 NOT NULL DEFAULT 0;
  -- function to update the tuple checksum
  -- if some attributes may be NULL, they must be coalesced
  CREATE FUNCTION foo_cs() RETURNS TRIGGER AS $$
    BEGIN
      -- compute key checksum
      NEW.key_cs = cksum4(NEW.id);
      -- compute tuple checksum
      NEW.tup_cs = cksum8(NEW.id || '|' || NEW.data);
      RETURN NEW;
    END; $$ LANGUAGE plpgsql;
  -- set trigger to call the checksum update function
  CREATE TRIGGER foo_cs_trigger
    BEFORE UPDATE OR INSERT ON Foo
    FOR EACH ROW EXECUTE PROCEDURE foo_cs();
  -- if table Foo is not initially empty,
  -- update its contents to trigger checksum computations
  UPDATE Foo SET id=id;

Then a fast comparison, which does not need to compute the initial checksum
table, can be requested with:

  ./pg_comparator --tcs=tup_cs --kcs=key_cs \
      admin@server1/app/Foo?id:data hobbes@server2/

As the primary key is a simple integer, the I<key_cs> could be left out
and the comparison could be launched with:

  ./pg_comparator --tcs=tup_cs --use-key \
      admin@server1/app/Foo?id:data hobbes@server2/

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

In case of tuple checksum collisions, false negative results may occur.
Changing the checksum function would help in such cases.
See the ANALYSIS sub-section.

=head1 DEPENDENCES

Three support functions are needed on the database:

=over 2

=item 1

The C<COALESCE> function takes care of NULL values in columns.

=item 2

A checksum function must be used to reduce and distribute key
and columns values. It may be changed with the C<--checksum> option.
Its size can be selected with the C<--checksize> option (currently 2, 4 or 8
bytes). The checksums also require casts to be converted to integers of
various sizes.

Suitable implementations are available for PostgreSQL and can be loaded into
the server by processing C<share/contrib/pgc_checksum.sql> and
C<share/contrib/pgc_casts.sql>. New checksums and casts are also available
for MySQL, see C<mysql_*.sql>. An loadable implementation of suitable
checksum functions is also available for SQLite, see C<sqlite_checksum.*>.

The C<ck> checksum is based on
L<Jenkins hash|https://en.wikipedia.org/wiki/Jenkins_hash>,
which relies on simple add, shift and xor integer operations.
The C<fnv> checksum is inspired by
L<FNV hash|https://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash>
(64 bits 1a version) which uses xor and mult integer operations,
although I also added some shift and add to help tweak high bits.

=item 3

An aggregate function is used to summarize checksums for a range of rows.
It must operate on the result of the checksum function.
It may be changed with the C<--aggregate> option.

Suitable implementations of a exclusive-or C<xor> aggregate are available
for PostgreSQL and can be loaded into the server by processing
C<share/contrib/xor_aggregate.sql>.

The C<sqlite_checksum.*> file also provides a C<xor> and C<sum> aggregates
for SQLite that are compatible with other databases.

=back

Moreover several perl modules are useful to run this script:

=over 4

=item

C<Getopt::Long> for option management.

=item

C<DBI>,
C<DBD::Pg> to connect to PostgreSQL,
C<DBD::mysql> to connect to MySQL,
and C<DBD::SQLite> to connect to SQLite.

=item

C<Term::ReadPassword> for C<--ask-pass> option.

=item

C<Pod::Usage> for doc self-extraction (C<--man> C<--opt> C<--help>).

=item

C<threads> for the experimental threaded version with option C<--threads>.

=item

C<Digest::MD5> for md5 checksum with SQLite.

=back

Modules are only loaded by the script if they are actually required.

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

The first phase computes the initial checksum table I<T(0)> on each side.
Assuming that I<key> is the table key columns, and I<cols> is the
table data columns that are to be checked for differences, then
it is performed by querying target table I<T> as follow:

  CREATE TABLE T(0) AS
    SELECT key AS pk,                   -- primary key
           checksum(key) AS kcs,        -- key checksum
           checksum(key || cols) AS tcs -- tuple checksum
    FROM t;

The initial key is kept, as it will be used to show differing keys
at the end. The rational for the I<kcs> column is to randomize the
key-values distribution so as to balance aggregates in the next phase.
The key must appear in the checksum also, otherwise content exchanged
between two keys would not be detected in some cases.

=head2 SUMMARY TABLES

Now we compute a set of cascading summary tables by grouping I<f>
(folding factor) checksums together at each stage. The grouping is
based on a mask on the I<kcs> column to take advantage of the
checksum randomization. Starting from I<p=0> we build:

  CREATE TABLE T(p+1) AS
    SELECT kcs & mask(p+1) AS kcs, -- key checksum subset
           XOR(tcs) AS tcs         -- tuple checksum summary
    FROM T(p)
    GROUP BY kcs & mask(p+1);

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

It is important that the very same masks are used on both sides so that
aggregations are the same, allowing to compare matching contents on both sides.

=head2 SEARCH FOR DIFFERENCES

After all these support tables are built on both sides comes the search for
differences. When checking the checksum summary of the last tables (level I<n>)
with only one row, it is basically a comparison of the checksum of the
whole table contents. If they match, then both tables are equal,
and we are done. Otherwise, if these checksums differ, some investigation
is needed to detect offending keys.

The investigation is performed by going down the table hierarchy and
looking for all I<kcs> for which there was a difference in the checksum
on the previous level. The same query is performed on both side
at each stage:

  SELECT kcs, tcs
  FROM T(p)
  WHERE kcs & mask(p+1) IN (kcs-with-diff-checksums-from-level-p+1)
  ORDER BY kcs [and on level 0: , id];

And the results from both sides are merged together.
When doing the merge procedure, four cases can arise:

=over 2

=item 1

Both I<kcs> and I<tcs> match. Then there is no difference.

=item 2

Although I<kcs> does match, I<tcs> does not. Then this I<kcs> is
to be investigated at the next level, as the checksum summary differs.
If we are already at the last level, then the offending key can be shown.

=item 3

No I<kcs> match, one supplemental I<kcs> in the first side.
Then this I<kcs> correspond to key(s) that must be inserted
for syncing the second table wrt the first.

=item 4

No I<kcs> match, one supplemental I<kcs> in the second side.
Then this I<kcs> correspond to key(s) that must be deleted
for syncing the second table wrt the first.

=back

Cases 3 and 4 are simply symmetrical, and it is only an interpretation
to decide whether it is an insert or a delete, taking the first side
as the reference.

=head2 ANALYSIS

Let I<n> be the number of rows, I<r> the row size, I<f> the folding factor,
I<k> the number of differences to be detected, I<c> the checksum size in bits,
then the costs to identify differences and the error rate is:

=over 2

=item B<network volume>

is better than I<k*f*ceil(log(n)/log(f))*(c+log(n))>.
the contents of I<k> blocks of size I<f> is transferred on the depth
of the tree, and each block identifier is of size I<log(n)> and contains
a checksum I<c>.
It is independent of I<r>, and you want I<k<<n>.
The volume of the SQL requests is about I<k*log(n)*ceil(log(n)/log(f))>,
as the list of non matching checksums I<k*log(n)> may be dragged
on the tree depth.

=item B<number of requests (on each side, the algorithm is symmetric)>

minimum is I<6+ceil(log(n)/log(f))> for equal tables,
maximum is I<6+2*ceil(log(n)/log(f))>.

=item B<disk I/O traffic>

is about I<n*r+n*ln(n)*(f/(f-1))>.

=item B<false negative probability>

I<i.e.> part of the tables are considered equal although they are different.
With a perfect checksum function, this is the probability of a checksum
collision at any point where they are computed and should have been different:
about I<k*ceil(log(n)/log(f))*2**-c>.
For a million row table, expecting 1000 changes with the default algorithm
parameter values, this is about I<2**10 *3/2**64>, that is about one chance
in I<2**52> merge runs.

=back

The lower the folding factor I<f> the better for the network volume,
but the higher the better for the number of requests and disk I/Os:
the choice of I<f> is a tradeoff.

The lower the checksum size I<c>, the better for the network volume,
but the worse for the false negative probability.

If the available bandwidth is reasonable, the comparison will most likely
be cpu-bound: the time is spent mainly on computing the initial checksum table.
Thus if you are planning to check for differences quite often, consider
maintaining a tuple checksum with a trigger, and possibly a key checksum
as well, and invoke with C<--tuple-checksum> and either C<--key-checksum>
or C<--use-key>.

=head2 IMPLEMENTATION ISSUES

The checksum implementation gives integers, which are constant length
and easy to manipulate afterwards.

The B<xor> aggregate is a good choice because there is no overflow issue with
it, it takes into account all bits of the input, and it can easily be defined
on any binary data. The B<sum> aggregate is also okay, but it requires some
kind of underlying integer type.

NULL values must be taken care appropriately.

The folding factor and all modules are taken as power of two so as to use
a masks.

There is a special management of large chunks of deletes or inserts
which is implemented although not detailed in the algorithmic overview
and complexity analysis.

There is some efforts to build a PostgreSQL/MySQL compatible implementation
of the algorithm, which added hacks to deal with type conversions and other
stuff.

This script is reasonably tested, but due to its proof of concept nature
there is a lot of options the combination of which cannot all be tested.

=head2 NOTE

If the tables to compare are in the same database, a simple SQL
query can extract the differences. Assuming Tables I<T1> and I<T2>
with primary key I<id> and non null contents I<data>, then their
differences, that is how I<T2> differs from the reference I<T1>,
is summarized by the following query:

	SELECT COALESCE(T1.id, T2.id) AS key,
	  CASE WHEN T1.id IS NULL THEN 'DELETE'
	       WHEN T2.id IS NULL THEN 'INSERT'
	       ELSE 'UPDATE'
	  END AS operation
	FROM T1 FULL JOIN T2 USING (id)
	WHERE T1.id IS NULL      -- DELETE
	   OR T2.id IS NULL      -- INSERT
	   OR T1.data <> T2.data -- UPDATE

=head2 REFERENCES

A paper was presented at a conference about this tool and its algorithm:
B<Remote Comparison of Database Tables> by I<Fabien Coelho>,
In Third International Conference on
Advances in Databases, Knowledge, and Data Applications (DBKDA),
pp 23-28, St Marteen, The Netherlands Antilles, January 2011.
ISBN: 978-1-61208-002-4.
Copyright IARIA 2011.
Online at L<Think Mind|http://www.thinkmind.org/index.php?view=article&articleid=dbkda_2011_2_10_30021>.

The algorithm and script was inspired by
B<Taming the Distributed Database Problem: A Case Study Using MySQL>
by I<Giuseppe Maxia> in B<Sys Admin> vol 13 num 8, Aug 2004, pp 29-40.
See L<Perl Monks|http://www.perlmonks.org/index.pl?node_id=381053> for details.
In this paper, three algorithms are presented.
The first one compares two tables with a checksum technique.
The second one finds UPDATE or INSERT differences based on a 2-level
(checksum and summary) table hierarchy. The algorithm is asymmetrical,
as different queries are performed on the two tables to compare.
It seems that the network traffic volume is in I<k*(f+(n/f)+r)>,
that it has a probabilistically-buggy merge procedure, and
that it makes assumptions about the distribution of key values.
The third algorithm looks for DELETE differences based on counting,
with the implicit assumption that there are only such differences.

In contrast to this approach, our fully symmetrical algorithm implements
all three tasks at once, to find UPDATE, DELETE and INSERT between the two
tables. The checksum and summary hierarchical level idea is reused and
generalized so as to reduce the algorithmic complexity.

From the implementation standpoint, the script is as parametric as possible
with many options, and makes few assumptions about table structures, types
and values.

=head1 SEE ALSO

I<Michael Nacos> made a robust implementation L<pg51g|http://pgdba.net/pg51g/>
based on triggers. He also noted that although database contents are compared
by the algorithm, the database schema differences can I<also> be detected
by comparing system tables which describe them.

I<Benjamin Mead Vandiver>'s PhD Thesis
B<Detecting and Tolerating Byzantine Faults in Database Systems>,
Massachusset's Institute of Technology, May 2008
(report number MIT-CSAIL-TR-2008-040).
There is an interesting discussion in Chapter 7, where experiments are
presented with a Java/JDBC/MySQL implementation of two algorithms, including
this one.

I<Baron Schwartz> discusses comparison algorithms in an
L<online post|http://tinyurl.com/mysql-data-diff-algorithm>.

Some more links:

=over 2

=item *
L<Adept SQL|http://www.adeptsql.com/>

=item *
L<Altova Database Spy|http://www.altova.com/databasespy/>

=item *
L<AUI Soft SQLMerger|http://auisoft.com/sqlmerger/>

=item *
L<Citrus Tech Data Comparison|http://www.citrustechnology.com/solutions/data-comparison>

=item *
L<Clever Components dbcomparer|http://www.clevercomponents.com/products/dbcomparer/>

=item *
L<Comparezilla|http://comparezilla.sourceforge.net/>

=item *
L<Datanamic Datadiff|http://www.datanamic.com/datadiff/>

=item *
L<DB Balance|http://www.dbbalance.com/db_comparison.htm>

=item *
L<DBSolo datacomp|http://www.dbsolo.com/datacomp.html>

=item *
L<dbForge Data Compare|http://www.devart.com/dbforge/sql/datacompare/>

=item *
L<DiffKit|http://www.diffkit.org/>

=item *
L<DKGAS DBDiff|http://www.dkgas.com/dbdiff.htm>

=item *
L<Maakit mk-table-sync|http://code.google.com/p/maatkit/>

=item *
L<MySQL DBCompare|http://dev.mysql.com/doc/workbench/en/mysqldbcompare.html>

=item *
L<List of SQL Server Tools|http://www.programurl.com/software/sql-server-comparison.htm>

=item *
L<SQL Server tablediff Utility|http://msdn.microsoft.com/en-US/library/ms162843.aspx>

=item *
L<Red Gate SQL Data Compare|http://www.red-gate.com/products/sql-development/sql-data-compare/>

=item *
L<Spectral Core OmegaSync|http://www.spectralcore.com/omegasync/>,

=item *
L<SQL Delta|http://www.sqldelta.com/>

=item *
L<AlfaAlfa SQL Server Comparison Tool|http://www.sql-server-tool.com/>

=item *
L<SQLyog MySQL GUI|http://www.webyog.com/>

=item *
L<xSQL Software Data Compare|http://www.xsqlsoftware.com/Product/Sql_Data_Compare.aspx>

=back

=head1 TESTS

The paper reports numerous performance tests with PostgreSQL under various
bandwidth constraints.

Moreover, non regression tests are run over randomly generated tables
when the software is upgraded:

=over 4

=item I<sanity> - about 30 seconds & 30 runs

Run a comparison, synchronization & check for all databases combinaisons
and all working asynchronous queries and threading options.

=item I<fast> - about 5 minutes & 360 runs

Run 12 tests similar to the previous one with varrying options (number of
key columns, number of value columns, aggregate function, checksum function,
null handling, folding factor, table locking or not...).

=item I<feature> - about 5 minutes & 171 or 477 runs

Test various features:
I<cc> for checksum computation strategies,
I<auto> for trigger-maintained checksums on PostgreSQL,
I<pgcopy> for PostgreSQL copy test,
I<empty> for corner cases with empty tables,
I<quote> for table quoting,
I<engine> for InnoDB vs MyISAM MySQL backends,
I<width> for large columns,
I<nullkey> for possible NULL values in keys,
I<sqlite> for SQLite test,
I<mylite> for SQLite/MySQL mixed mode with some restrictions,
I<pglite> for SQLite/PostgreSQL mixed mode with some restrictions.

=item I<release> - about 20 minutes & 944 runs

This is I<feature> with two table sizes, I<fast>, and I<collisions>
to test possible hash collisions.

=item I<hour> - about 1 hour & 2880 runs

A combination of 8 I<fast> validations with varrying table sizes and
difference ratio ranging from 0.1% to 99.9%.

=item I<full> - about 6 hours & 16128 runs... seldom run

A combinatorial test involving numerous options: aggregation, checksums,
null handling, foldings, number of key and value attributes...

=back

=head1 BUGS

All software have bugs. This is a software, hence it has bugs.

Reporting bugs is good practice, so tell me if you find one.
If you have a fix, this is even better!

The implementation does not do many sanity checks.

Although the algorithm can work with some normalized columns
(say strings are trimmed, lowercased, Unicode normalized...),
the implementation may not work at all.

The script is really tested with integer and text types, issues may arise
with other types.

The script handles one table at a time. In order to synchronize several
linked tables, you must disable referential integrity checks, then
synchronize each tables, then re-enable the checks.

There is no real attempt at doing some sensible identifier quoting,
although quotes provided in the connection url are kept, so it may work
after all for simple enough cases.

There is no neat user interfaces, this is a earthly command line tool.
This is not a bug, but a feature.

There are too many options.

Using another language such as Python for this application seems attractive,
but there is no cleanly integrated manual-page style support such as POD, and
the documentation is 50% of this script.

Mixed SQLite vs PostgreSQL or MySQL table comparison may not work properly in
all cases, because of SQLite dynamic type handling and reduced capabilities.

The script creates (temporary) tables on both sides for comparing the target
tables: this imply that you must be allowed to do that for the comparison...
However, read-only replicas do not allow creating objects, which mean that you
cannot use pg_comparator to compare table contents on a synchronized replica.

=head1 TODO

Allow larger checksum sizes.

Make it a PostgreSQL extension.

Add an option to avoid IN (x,y,...) syntax, maybe with a temporary table
to hold values and use a JOIN on that. I'm not sure about the performance
implications, though.

=head1 VERSIONS

See L<web site|http://www.coelho.net/pg_comparator/> for the latest version.

=over 4

=item B<version @VERSION@> (r@REVISION@ on @DATE@)

Fix some typos found by Lintian and pointed out by I<Ivan Mincik>.
Add support for FNV (Fowler Noll Vo) version 1a inspired hash functions.
Add option to skip inserts, updates or deletes when synchronizing,
which may be useful to deal with foreign keys, issue pointed
out by I<Graeme Bell>.
The I<release> validation was run successfully
on PostgreSQL 9.4.1 and MySQL 5.5.41.

=item B<version 2.2.5> (r1512 on 2014-07-24)

Fix broken URL defaults to use UNIX sockets with an empty host name,
per report by I<Ivan Mincik>.
Fix C<--where> condition handling with C<--pg-copy> in corner cases.
Do not take execution timestamps when not required.
Allow a larger number of differences by default for small table comparisons.
Add more sanity checks.
Improve some error messages.
The I<release> validation was run successfully
on PostgreSQL 9.4b1 and MySQL 5.5.38.

=item B<version 2.2.4> (r1506 on 2014-07-13)

Add experimental support for using COPY instead of INSERT/UPDATE for PostgreSQL,
in chunks of size specified with option C<--pg-copy>,
as suggested by I<Graeme Bell>.
Minor fix when computing the maximum number of differences to report.
The I<release> validation was run successfully
on PostgreSQL 9.4b1 and MySQL 5.5.37.

=item B<version 2.2.3> (r1494 on 2014-04-19)

Improved documentation.
Add C<--unlogged> option to use unlogged tables.
The I<release> validation was run successfully
on PostgreSQL 9.3.4 and MySQL 5.5.35.

=item B<version 2.2.2> (r1485 on 2014-01-08)

Fix some warnings reported by I<Ivan Mincik>.
Minor doc changes.
The I<release> validation was run successfully
on PostgreSQL 9.3.2 and MySQL 5.5.34.

=item B<version 2.2.1> (r1480 on 2013-05-09)

Do not die on missing driver in URL, regression reported by I<Ivan Mincik>.
The I<release> validation was run successfully
on PostgreSQL 9.2.4 and MySQL 5.5.31.

=item B<version 2.2.0> (r1473 on 2013-03-07)

Bug fix by I<Robert Coup>, which was triggered on hash collisions (again).
This bug was introduced in 2.1.0 when getting rid of the key separator,
and not caught by the validation.
Factor out database dependencies in a separate data structure,
so that adding new targets should be simpler in the future.
Add SQLite support.
Add experimental Firebird support.
Fix some warnings.
Update C<cksum8> function to propagate the first checksum half
into the computation of the second half.
Improved documentation.
Improved validation, in particular with a I<collisions> test.
The I<release> and I<hour> validations were run successfully
on PostgreSQL 9.2.3 and MySQL 5.5.29.

=item B<version 2.1.2> (r1402 on 2012-10-28)

Fix an issue when table names were quoted, raised by I<Robert Coup>.
Improved documentation, especially Section L</"SEE ALSO">.
More precise warning.
Improved validation.
The I<release> and I<hour> validations were run successfully
on PostgreSQL 9.2.1 and MySQL 5.5.27.

=item B<version 2.1.1> (r1375 on 2012-08-20)

Synchronization now handles possible NULLs in keys.
Warn if key is nullable or not an integer under C<--use-key>.
Improved documentation, in particular non regression tests are described.
The I<release> and I<hour> validations were run successfully
on PostgreSQL 9.1.4 and MySQL 5.5.24.

=item B<version 2.1.0> (r1333 on 2012-08-18)

Add C<--tuple-checksum> and C<--key-checksum> options so as to use existing
possibly trigger-maintained checksums in the target tables instead of
computing them on the fly.
Add C<--checksum-computation> option to control how the checksum table is
built, either C<CREATE ... AS ...> or C<CREATE ...; INSERT ...>.
For MySQL, rely directly on the count returned by C<CREATE ... AS> if available.
Add C<--lock> option for locking tables, which is enabled when synchronizing.
Improve asynchronous query handling, especially when creating checksum tables
and getting initial table counts, and in some other cases.
Remove redundant data transfers from checksum table under option C<--use-key>.
Get rid of the separator when retrieving keys of differing tuples. Note that
it is still used when computing checksums.
Fix bug in bulk insert and delete key recovery under option C<--use-key>.
Fix potential bug in handling complex conditions with C<--where>.
Change default prefix to B<pgc_cmp> so that it is clearer that it belongs
to B<pg_comparator>.
Fix initial count query which was not started asynchronously under C<--tcs>.
Ensure that if not null detection is in doubt, a column is assumed nullable
and thus is coalesced.
Fix query counters so that they are shared under C<--threads>.
Fix threading for explicit cleanup phase.
Warn if nullable key attributes are encountered.
Make default driver for second connection be the same as first.
Rename option C<--assume-size> as C<--size>.
Add short documentation about C<--debug>.
Multiple C<--debug> set DBI tracing levels as well.
Improve the difference computation code so that the algorithm is more readable.
Improve documentation.
Add and improve comments in the code.
The I<release> and I<hour> validations were run successfully
on PostgreSQL 9.1.4 and MySQL 5.5.24.

=item B<version 2.0.1> (r1159 on 2012-08-10)

Add C<--source-*> options to allow taking over DBI data source specification.
Change default aggregate to C<sum> so that it works as expected by default
when mixing PostgreSQL and MySQL databases. The results are okay with C<xor>,
but more paths than necessary were investigated, which can unduly trigger
the max report limit.
Improved documentation. In particular default option settings are provided
systematically.
The I<fast> validation was run successfully on PostgreSQL 9.1.4 and
MySQL 5.5.24.

=item B<version 2.0.0> (r1148 on 2012-08-09)

Use asynchronous queries so as to provide some parallelism to the comparison
without the issues raised by threads. It is enabled by default and can be
switched off with option C<--no-asynchronous>.
Allow empty hostname specification in connection URL to use a UNIX socket.
Improve the documentation, in particular the analysis section.
Fix minor typos in the documentation.
Add and fix various comments in the code.
The I<fast> validation was run successfully on PostgreSQL 9.1.4 and
MySQL 5.5.24.

=item B<version 1.8.2> (r1117 on 2012-08-07)

Bug fix in the merge procedure by I<Robert Coup> that could result in
some strange difference reports in corner cases, when there were collisions
on the I<kcs> in the initial checksum table.
Fix broken synchronization with '|' separator, raised by I<Aldemir Akpinar>.
Warn about possible issues with large objects.
Add C<--long-read-len> option as a possible way to circumvent such issues.
Try to detect these issues.
Add a counter for metadata queries.
Minor documentation improvements and fixes.

=item B<version 1.8.1> (r1109 on 2012-03-24)

Change default separator again, to '|'.
Fix C<--where> option mishandling when counting, pointed out by
I<Enrique Corona>.

Post release note: the synchronisation is broken with the default separator
in 1.8.1, do not use it, or use --separator='%'.

=item B<version 1.8.0> (r1102 on 2012-01-08)

Change default separator to '%', which seems less likely,
after issues run into by I<Emanuel Calvo>.
Add more pointers and documentation.

=item B<version 1.7.0> (r1063 on 2010-11-12)

Improved documentation.
Enhancement and fix by I<Maxim Beloivanenko>: handle quoted table and
attribute names;
Work around bulk inserts and deletes which may be undefined.
More stats, more precise, possibly in CSV format.
Add timeout and use-null options.
Fix subtle bug which occurred sometimes on kcs collisions in table I<T(0)>.

=item B<version 1.6.1> (r754 on 2010-04-16)

Improved documentation.
Key and columns now defaults to primary key and all other columns of table
in first connection.
Password can be supplied from the environment.
Default password for second connection always set depending on the first.
Add max ratio option to express the relative maximum number of differences.
Compute grouping masks by shifting left instead of right by default (that
is doing a divide instead of a modulo).
Threads now work a little, although it is still quite experimental.
Fix a bug that made perl see differing checksum although they were equal, in
some unclear conditions.

=item B<version 1.6.0> (r701 on 2010-04-03)

Add more functions (MD5, SUM) and sizes (2, 4, 8).
Remove template parameterization which is much too fragile to expose.
Add a wrapping transaction which may speed up things a little.
Implementation for MySQL, including synchronizing heterogeneous databases.
Improved documentation. Extensive validation/non regression tests.

=item B<version 1.5.2> (r564 on 2010-03-22)

More documentation.
Improved connection parsing with more sensible defaults.
Make the mask computation match its above documentation with a bottom-up
derivation, instead of a simpler top-down formula which results in bad
performances when a power of the factor is close to the size (as pointed
out in I<Benjamin Mead Vandiver>'s PhD).
This bad mask computation was introduced somehow between 1.3 and 1.4 as
an attempt at simplifying the code.

=item B<version 1.5.1> (r525 on 2010-03-21)

More documentation.
Add C<--expect> option for non regression tests.

=item B<version 1.5.0> (r511 on 2010-03-20)

Add more links.
Fix so that with a key only (i.e. without additional columns), although
it could be optimized further in this case.
Integrate patch by I<Erik Aronesty>: More friendly "connection parser".
Add synchronization option to actually synchronize the data.

=item B<version 1.4.4> (r438 on 2008-06-03)

Manual connection string parsing.

=item B<version 1.4.3> (r424 on 2008-02-17)

Grumble! wrong tar pushed out.

=item B<version 1.4.2> (r421 on 2008-02-17)

Minor makefile fix asked for by I<Roberto C. Sanchez>.

=item B<version 1.4.1> (r417 on 2008-02-14)

Minor fix for PostgreSQL 8.3 by I<Roberto C. Sanchez>.

=item B<version 1.4> (r411 on 2007-12-24)

Port to PostgreSQL 8.2. Better documentation.
Fix mask bug: although the returned answer was correct, the table folding
was not.
DELETE/INSERT messages exchanged so as to match a 'sync' or 'copy' semantics,
as suggested by I<Erik Aronesty>.

=item B<version 1.3> (r239 on 2004-08-31)

Project moved to PG Foundry.
Use cksum8 checksum function by default.
Minor doc updates.

=item B<version 1.2> (r220 on 2004-08-27)

Added C<--show-all-keys> option for handling big chunks of deletes
or inserts.

=item B<version 1.1> (r210 on 2004-08-26)

Fix algorithmic bug: checksums B<must> also include the key,
otherwise exchanged data could be not detected if the keys were
to be grouped together.
Algorithmic section added to manual page.
Thanks to I<Giuseppe Maxia> who asked for it.
Various code cleanups.

=item B<version 1.0> (r190 on 2004-08-25)

Initial revision.

=back

=head1 COPYRIGHT

Copyright (c) 2004-@YEAR@, I<Fabien Coelho>
<pg dot comparator at coelho dot net> L<http://www.coelho.net/>

This software is distributed under the terms of the BSD Licence.
Basically, you can do whatever you want, but you have to keep the license
and I'm not responsible for any consequences.
Beware, you may lose your data, your friends or your hairs because of this
software!
See the LICENSE file enclosed with the distribution for details.

If you are very happy with this software, I would appreciate a postcard
saying so. See my webpage for current address.

=cut

my $script_version = '@VERSION@ (r@REVISION@)';
my $revision = '$Revision: 1540 $';
$revision =~ tr/0-9//cd;

################################################################# SOME DEFAULTS

# various option defaults
my ($verb, $debug, $temp, $unlog, $ask_pass, $clear) = (0, 0, 1, 0, 0, 0);
my ($max_ratio, $max_levels, $report, $threads, $async) =  (0.1, 0, 1, 0, 1);
my ($cleanup, $size, $usekey, $usenull, $synchronize) = (0, 0, 0, 1, 0);
my ($do_it, $do_trans, $prefix, $ckcmp) = (0, 1, 'pgc_cmp', 'create');
my ($maskleft, $name, $key_size, $col_size, $where) = (1, 'none', 0, 0, '');
my ($factor, $expect_warn) = (7, 0);
my ($skip_inserts, $skip_updates, $skip_deletes) = (0, 0, 0);
# condition, tests, max size of blobs, data sources...
my ($expect, $longreadlen, $source1, $source2, $key_cs, $tup_cs, $do_lock,
    $env_pass, $max_report, $stats, $pg_copy);

# algorithm defaults
# hmmm... could rely on base64 to handle binary keys?
# the textual representation cannot be trusted to avoid the separator
my ($null, $checksum, $checksize, $agg, $sep) = ('text', 'ck', 8, 'sum', '|');

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
# globals: $verb (verbosity level)
# verb(2, "something...")
sub verb($$)
{
  my ($level, $msg) = @_;
  print STDERR '#' x $level, " $msg\n" if $level<=$verb;
}

############################################################ DATABASE SPECIFICS

sub pgsql_cast($$) {
  my ($s, $sz) = @_;
  return "${s}::INT$sz";
}

sub mysql_cast($$) {
  my ($s, $sz) = @_;
  # MySQL casts are a joke, you cannot really select any target type.
  # so I reimplemented that in a function which returns a BIGINT whatever.
  return "biginttoint$sz(CAST($s AS SIGNED))";
}

sub sqlite_cast($$) {
  my ($s, $sz) = @_;
  # it seems that there is only one 8 byte signed integer type
  #return "CAST($s AS INTEGER) & 255" if $sz == 1;
  return "CAST($s AS INTEGER) & 65535" if $sz == 2;
  return "CAST($s AS INTEGER) & 4294967295" if $sz == 4;
  return "CAST($s AS INTEGER)" if $sz == 8;
}

sub firebird_cast($$) {
  my ($s, $sz) = @_;
  #return "BIN_AND(CAST(($s) AS INTEGER), 255)" if $sz == 1;
  return "BIN_AND(CAST(($s) AS INTEGER), 65535)" if $sz == 2;
  # argh! firebird casts detect overflows...
  return "CAST(BIN_AND(($s), 2147483647) AS INTEGER)" if $sz == 4;
  return "CAST(($s) AS BIGINT)" if $sz == 8;
}

sub pgsql_cksum_template($$) {
  my ($algo, $sz) = @_;
  return "CKSUM$sz((%s)::TEXT)" if $algo eq 'ck';
  return "FNV$sz((%s)::TEXT)" if $algo eq 'fnv';
  return pgsql_cast("DECODE(MD5(%s::TEXT),'hex')::BIT(" . 8*$sz . ")", $sz)
    if $algo eq 'md5';
  die "unexpected checksum $algo for pgsql";
}

sub mysql_cksum_template($$) {
  my ($algo, $sz) = @_;
  return "CKSUM$sz(CAST(%s AS BINARY))" if $algo eq 'ck';
  return "FNV$sz(CAST(%s AS BINARY))" if $algo eq 'fnv';
  return mysql_cast("CONV(LEFT(MD5(%s),". 2*$sz ."),16,10)", $sz)
    if $algo eq 'md5';
  die "unexpected checksum $algo for mysql";
}

sub sqlite_cksum_template($$) {
  my ($algo, $sz) = @_;
  return "CKSUM$sz(CAST(%s AS TEXT))" if $algo eq 'ck';
  return "FNV$sz(CAST(%s AS TEXT))" if $algo eq 'fnv';
  return "PGC_MD5($sz, CAST(%s AS TEXT))" if $algo eq 'md5';
  die "unexpected checksum $algo for sqlite";
}

sub firebird_cksum_template($$) {
  my ($algo, $sz) = @_;
  return firebird_cast("HASH(CAST((%s) AS BLOB))", $sz) if $algo eq 'ck';
  die "unexpected checksum $algo for firebird";
}

sub pgsql_null_template($$$) {
  my ($null, $algo, $sz) = @_;
  return "COALESCE(%s::TEXT,'NULL')" if $null eq 'text';
  return 'COALESCE(' . pgsql_cksum_template($algo, $sz) . ',0)'
    if $null eq 'hash';
  die "unexpected null $null";
}

sub mysql_null_template($$$) {
  my ($null, $algo, $sz) = @_;
  return "COALESCE(CAST(%s AS BINARY),'NULL')"  if $null eq 'text';
  return 'COALESCE(' . mysql_cksum_template($algo, $sz) . ',0)'
    if $null eq 'hash';
  die "unexpected null $null";
}

sub sqlite_null_template($$$) {
  my ($null, $algo, $sz) = @_;
  return "COALESCE(%s,'NULL')" if $null eq 'text';
  return 'COALESCE(' . sqlite_cksum_template($algo, $sz) . ',0)'
    if $null eq 'hash';
  die "unexpected null $null";
}

sub firebird_null_template($$$) {
  my ($null, $algo, $sz) = @_;
  return "COALESCE(%s,'NULL')" if $null eq 'text';
  return 'COALESCE(' . firebird_cksum_template($algo, $sz) . ',0)'
    if $null eq 'hash';
  die "unexpected null $null";
}

sub bb_concat($$) {
  my ($sep, $list) = @_;
  return join("||'$sep'||", @$list);
}

sub mysql_concat($$) {
  my ($sep, $list) = @_;
  return 'CONCAT(' . join(",'$sep',", @$list) . ')';
}

sub dq_unquote($) {
  my ($str) = @_;
  if ($str =~ /^\"(.*)\"$/) {
    $str = $1;
    $str =~ s/\"\"/\"/g;
  }
  return $str;
}

sub dq_quote($) {
  my ($str) = @_;
  if ($str =~ /^\"(.*)\"$/) {
    $str = $1;
    $str =~ s/\"\"/\"/g;
  }
  return $str;
}

sub mysql_unquote($) {
  my ($str) = @_;
  if ($str =~ /^\`(.*)\`$/) {
    $str = $1;
    $str =~ s/\`\`/\`/g;
  }
  return $str;
}

sub pgsql_tableid($) {
  my ($table) = @_;
  # ??? this is not really a parser as it should be...
  die "too many '.' in table $table" if $table =~ /\..*\./;
  $table = ".$table" if $table !~ /\./;
  my ($s, $t) = split '\.', $table;
  return (dq_unquote($s), dq_unquote($t));
}

sub mysql_tableid($) {
  my ($table) = @_;
  # no 'schema' under MySQL... well, there is one, but it is named 'database'.
  return (undef, mysql_unquote($table));
}

sub sqlite_tableid($) {
  my ($table) = @_;
  return (undef, dq_unquote($table));
}

sub pgsql_get_result($) {
  my ($dbh) = @_;
  return $dbh->pg_result();
}

sub mysql_get_result($) {
  my ($dbh) = @_;
  my @res;
  # work around the fails if there is no current async query
  eval {
    # hmmm... under -A we can have some
    # "Gathering async_query_in_flight results for the wrong handle"
    # warn "undefined query?" if not defined $dbh->mysql_async_ready();
    @res = $dbh->mysql_async_result();
  };
  if ($@ and $debug) { # keep it under the carpet...
    die "$@";
  }
  return @res;
}

sub sqlite_initialize($)
{
  my ($dbh) = @_;
  # help integer typing
  $dbh->{sqlite_see_if_its_a_number} = 1;
  # database locking with begin transaction
  # note: there must be two databases, otherwise this is a dead lock!
  $dbh->{sqlite_use_immediate_transaction} = 1 if $do_lock;
  # load checksum and possiby other extensions
  if (exists $ENV{PGC_SQLITE_LOAD_EXTENSION}) {
    $dbh->sqlite_enable_load_extension(1);
    for my $extension (split /:/, $ENV{PGC_SQLITE_LOAD_EXTENSION}) {
      sql_do($dbh, 'sqlite', "SELECT load_extension('$extension');");
    }
  }
  if ($checksum eq 'md5') {
    # use perl own implementation!
    require Digest::MD5;
    $dbh->sqlite_create_function("PGC_MD5", 2,
      sub {
	my ($bytes, $data) = @_;
	my $fmt = $bytes==2? 'x14s': $bytes==4? 'x12l': 'x8q';
	return unpack($fmt, Digest::MD5::md5($data));
      });
  }
}

sub firebird_initialize($)
{
  my ($dbh) = @_;
  #$dbh->{AutoCommit} = 0;
}

sub amp_and($$) { return join(' & ', @_); }
sub bin_and($$) { return 'BIN_AND(' . join(',', @_) . ')'; }

sub pgsql_lock($$) {
  my ($t, $ro) = @_; return "LOCK TABLE $t IN ACCESS EXCLUSIVE MODE";
}

sub mysql_lock($$) { # for mysql... locks are seen as a weak transaction tool
  my ($t, $ro) = @_; return "LOCK TABLES $t " . ($ro? 'READ': 'WRITE');
}

# define all database specific "attributes" and "methods"
# this low key OO approach avoids relying on several files
my %M = (
  #
  # PostgreSQL
  #
  'pgsql' => {
    # DBI driver
    'driver' => 'DBI:pg:',
    # DBI connection template
    'source' => 'DBI:Pg:dbname=%b;host=%h;port=%p;',
    # default port
    'port' => 5432,
    # async attributes for prepare/do
    'attrs' => {},
    # sql-comparison which is null-safe
    'safeeq' => ' IS NOT DISTINCT FROM ?',
    # sql temporary or unlogged table
    'temporary' => 'TEMPORARY ',
    'unlogged' => 'UNLOGGED ',
    # sql drop table
    'drop_table' => 'DROP TABLE IF EXISTS',
    # actual aggregates to use
    'xor' => 'XOR',
    'sum' => 'SUM',
    # sql-concatenate a list of stuff: concat($sep,\@list)
   'concat' => \&bb_concat,
    # sql lock table: lock('table name', is-read-only)
    # for pgsql, the lock will be released at the end of the transaction
    'lock' => \&pgsql_lock,
    # unquote sql identifier
    'unquote' => \&dq_unquote,
    # quote an identifier. currently unused.
    # ??? what about $dbh->quote_identifier?
    'quote' => \&dq_quote,
    # sql checksum type: cktype{cksize}
    'cktype' => { 2 => 'INT2', 4 => 'INT4', 8 => 'INT8' },
    # sql cast to size: cast(stuff,size)
    'cast' => \&pgsql_cast,
    # sql text cast prefix & suffix
    'tcast_prefix' => '',
    'tcast_suffix' => '::TEXT',
    # sql text type
    'text' => 'TEXT',
    # whether CREATE TABLE ... AS is implemented
    'create_as' => 1,
    # whether CREATE TABLE ... AS returns the number of rows
    'create_as_returns_count' => 0,
    # whether column_info is available
    'column_info' => 1,
    # whether async queries are available
    'async' => 1,
    # whether the driver work with threads
    'threads' => 0,
    # sql query for column size: colsize($table, \@cols)
    'colsize' => sub {
      my ($table, $cols) = @_ ;
      return "SELECT ROUND(AVG(pg_column_size(" .
        join(')+pg_column_size(', @$cols) . ")),0) FROM $table";
    },
    # sql checksum for one attribute: cksum{$algo}($size, $att)
    'ckoneatt' => {
      'md5' => sub { my ($sz, $att) = @_;
	return pgsql_cast(
	  "COALESCE(DECODE(MD5(${att}::TEXT),'hex'),''::BYTEA)" .
	  "::BIT(" .  8*$sz . ")", $sz);
	},
      'ck' => sub { my ($sz, $att) = @_; return "CKSUM$sz(${att}::TEXT)"; },
      'fnv' => sub { my ($sz, $att) = @_; return "FNV$sz(${att}::TEXT)"; }
    },
    # sql checksum template: cksum($algo, $size)
    'cksum' => \&pgsql_cksum_template,
    # sql null template: null($null, $algo, $size)
    'null' => \&pgsql_null_template,
    # return unquoted (schema, table)
    'tableid' => \&pgsql_tableid,
    # get result from an asynchronous query: get_result($dbh)
    'get_result' => \&pgsql_get_result,
    # 'initialize' database handler: initialize($dbh)
    # bitwise and operation: andop($s1,$s2)
    'andop' => \&amp_and,
  },
  #
  # MySQL
  #
  'mysql' => {
    'driver' => 'DBI:mysql:',
    'source' => 'DBI:mysql:database=%b;host=%h;port=%p;',
    'port' => 3306,
    'attrs' => {},
    'safeeq' => '<=>?',
    'temporary' => 'TEMPORARY ',
    'unlogged' => '', # mysql myisam is always unlogged?
    'drop_table' => 'DROP TABLE IF EXISTS',
    'xor' => 'BIT_XOR',
    'sum' => 'SUM',
    'concat' => \&mysql_concat,
    'lock' => \&mysql_lock,
    'unquote' => \&mysql_unquote,
    'quote' => sub { my ($str) = @_; $str =~ s/\`/\`\`/g; return "\`$str\`"; },
    'cktype' => { 2 => 'INTEGER', 4 => 'INTEGER', 8 => 'BIGINT' },
    'cast' => \&mysql_cast,
    'tcast_prefix' => 'CAST(',
    'tcast_suffix' => ' AS BINARY)',
    'text' => 'TEXT',
    'create_as' => 1,
    'create_as_returns_count' => 1,
    'column_info' => 1,
    'async' => 1,
    'threads' => 1,
    'colsize' => sub {
      my ($table, $cols) = @_ ;
      warn "col_size() not well implemented for mysql";
      return "SELECT ROUND(AVG(LENGTH(" . mysql_concat('', $cols) . ")),0) " .
        "FROM $table";
    },
    'ckoneatt' => {
      'md5' => sub { my ($sz, $att) = @_;
        return mysql_cast(
	  "COALESCE(CONV(LEFT(MD5($att),". 2*$sz ."),16,10),0)", $sz);
	},
      'ck' => sub { my ($sz, $att) = @_;
	return "CKSUM$sz(CAST($att AS BINARY))"
      },
      'fnv' => sub { my ($sz, $att) = @_;
	return "FNV$sz(CAST($att AS BINARY))"
      }
    },
    'cksum' => \&mysql_cksum_template,
    'null' => \&mysql_null_template,
    'tableid' => \&mysql_tableid,
    'get_result' => \&mysql_get_result,
    # no 'initialize'
    'andop' => \&amp_and,
  },
  #
  # SQLite
  #
  'sqlite' => {
    'driver' => 'DBI:SQLite:',
    'source' => 'DBI:SQLite:dbname=%b',
    # no port
    'attrs' => {},
    'safeeq' => '=?', # ???
    'temporary' => 'TEMPORARY ',
    'unlogged' => 'TEMPORARY ',
    'drop_table' => 'DROP TABLE IF EXISTS',
    'xor' => 'XOR',
    'sum' => 'ISUM',# work around 'SUM' and 'TOTAL' overflow handling
    'concat' => \&bb_concat,
    # no table 'lock', but possible database locking with the transaction
    'unquote' => \&dq_unquote,
    'quote' => \&dq_quote,
    'cktype' => { 2 => 'INTEGER', 4 => 'INTEGER', 8 => 'INTEGER' },
    'cast' => \&sqlite_cast,
    'tcast_prefix' => 'CAST(',
    'tcast_suffix' => ' AS TEXT)',
    'text' => 'TEXT',
    'create_as' => 1,
    'create_as_returns_count' => 0,
    'column_info' => 0,
    'async' => 0,
    'threads' => 0,
    'colsize' => sub {
      my ($table, $cols) = @_ ;
      warn "col_size() not well implemented for sqlite";
      return "SELECT ROUND(AVG(LENGTH(" . bb_concat('', $cols) . ")),0) " .
        "FROM $table";
    },
    'ckoneatt' => {
      'md5' => sub { my ($sz, $att) = @_;
        return "PGC_MD5($sz, CAST($att AS TEXT))";
	},
      'ck' => sub { my ($sz, $att) = @_;
        return "CKSUM$sz(CAST($att AS TEXT))";
      },
      'fnv' => sub { my ($sz, $att) = @_;
        return "FNV$sz(CAST($att AS TEXT))";
      }
    },
    'cksum' => \&sqlite_cksum_template,
    'null' => \&sqlite_null_template,
    'tableid' => \&sqlite_tableid,
    # no 'get_result'
    'initialize' => \&sqlite_initialize,
    'andop' => \&amp_and,
  },
  #
  # Firebird: this is a strange bird...
  #
  # - it is based on windows-oriented open-sourced Borland's InterBase.
  # - it mixes both client-server features (host, port, user, pass) and
  #   sqlite-like single-file storage for which you must know the path.
  #   this seems pretty inconsistent.
  # - <sigh>passwords are stored in plaintext</sigh>.
  # - the documentation is only "differential" from one version to the next,
  #   or one database to the other: I have found no consolidated reference.
  # - the DBD driver seems buggy (see close_cursor workaround) and is missing
  #   column_info and other metadata features that I need.
  # - the "isql" command cannot connect from the shell, one must issue a
  #   "CONNECT 'path';" interactively, this makes testing harder.
  # - some features are implemented strangely: e.g. temporary tables are "global
  #   temporary" and are not dropped, only their contents is deleted on exit.
  # - there is no "DROP TABLE IF EXISTS ...", thus manual cleaning on errors.
  # - there is no "CREATE TABLE ... AS SELECT ..."
  # - typename 'BLOB SUB_TYPE TEXT'... the standard TEXT would have been nicer.
  # - SUM() and CAST( ...) detect overflows where I wish they would not.
  # - I found no simple way to add a new aggregate function.
  # - the project seems on a slow development space.
  # - the comparisons seem very slow.
  #
  'firebird' => {
    'driver' => 'DBI:Firebird:',
    'source' => 'DBI:Firebird:database=%b;host=%h;port=%p;',
    'port' => 3060,
    'attrs' => {},
    'safeeq' => ' IS NOT DISTINCT FROM ?',
    'temporary' => 'GLOBAL TEMPORARY ', # not dropped...
    'unlogged' => '', # ???
    'drop_table' => 'DROP TABLE',
    'xor' => '???',
    'sum' => 'SUM', # ??? too clever, detects integer overflows
    'concat' => \&bb_concat,
    #'lock' => sub { my ($t, $ro) = @_; return "???unclear"; },
    'unquote' => \&dq_unquote,
    'quote' => \&dq_quote,
    # 4 use BIGINT instead of INTEGER to avoid SUM overflows
    'cktype' => { 2 => 'INTEGER', 4 => 'BIGINT', 8 => 'BIGINT' },
    'cast' => \&firebird_cast,
    'tcast_prefix' => 'CAST(',
    'tcast_suffix' => ' AS BLOB)',
    'text' => 'BLOB SUB_TYPE TEXT',
    'create_as' => 0,
    'create_as_returns_count' => 0,
    'column_info' => 0,
    'async' => 0,
    'threads' => 0,
    'colsize' => sub {
      my ($table, $cols) = @_ ;
      warn "col_size() not well implemented for firebird";
      return "SELECT ROUND(AVG(LENGTH(" . bb_concat('', $cols) . ")),0) " .
        "FROM $table";
    },
    # sql checksum for one attribute: cksum{$algo}($size, $att)
    'ckoneatt' => {
      'md5' => sub { die "MD5 not implemented with firebird"; },
      'ck' => sub {
	my ($sz, $att) = @_;
	return firebird_cast("HASH($att)", $sz);
      }
    },
    # sql checksum template: cksum($algo, $size)
    'cksum' => \&firebird_cksum_template,
    # sql null template: null($null, $algo, $size)
    'null' => \&firebird_null_template,
    'tableid' => \&sqlite_tableid,
    # no 'get_result'
    'initialize' => \&firebird_initialize,
    'andop' => \&bin_and,
    # work around firebird cursor issue with prepare/(execute/fetch)*
    'close_cursor' => sub { my ($sth) = @_; $sth->fetchrow_array(); },
  }
);

#################################################################### CONNECTION

use DBI;

my ($dbh1, $dbh2);

# parse a connection url
# ($db,$u,$w,$h,$p,$b,$t,$k,$c) = parse_conn("connection-url")
# globals: $verb
# pgsql://calvin:secret@host:5432/base/schema.table?key:column,list
# mysql://calvin:secret@host:3306/base/table?key:column,list
# firebird://calvin:secret@host:3050/base_file_path/table?key:column,list
# sqlite:///base_file_relative_path/table?key:column,list
sub parse_conn($)
{
  my $c = shift;
  my ($db, $user, $pass, $host, $port, $base, $tabl, $keys, $cols);

  # get driver name
  if ($c =~ /^(\w+):\/\//) {
    $db = $1;
    $db .= 'sql' if $db eq 'pg' or $db eq 'my'; # allow pg & my for pgsql & mysql
    $c =~ s/^\w+:\/\///; # remove driver part
  }
  else {
    verb 2, "no driver in URL: $c" if $debug;
  }

  # split authority and path on first '/'
  die "invalid connection string '$c', must contain '\/'\n"
    unless $c =~ /^([^\/]*)\/(.*)/;

  my ($auth, $path) = ($1, $2);

  if ("$auth") {
    # parse authority if non empty. ??? url-translation?
    die "invalid authority string '$auth'\n"
      unless $auth =~ /^((\w+)         # login
			 (:([^.]*)     # :password
			  )?\@)?       # @ => auth string is before
		       ([^\@:\/]*)     # host
		       (:(\d+))?$      # :port
		      /x;

    $user=$2 if defined $1;
    $pass=$4 if defined $3;
    $host=$5; # may be empty, but is always defined *if* there is a non empty auth
    $port=$7 if defined $6;
    verb 3, "user=$user pass=$pass host=$host port=" . defined $port? $port: '?'
      if $debug;
  }

  if ("$path") {
    my $kc_str;

    if (defined $db and ($db eq 'sqlite' or $db eq 'firebird')) {
      # note: there may be "/" in the base file path...
      # if so, the last "/" is mandatory to mark the table name
      die "invalid path string '$path'\n"
        unless $path =~ /
	  ^((.*)                    # base file (longest) path
	    \/(\w+|\"[^\"+]\")?)?   # table
	  (\?(.+))?                 # key,part:column,list...
	/x;
      $base = $2 if defined $2;
      $tabl = $3 if defined $3;
      $kc_str = $5 if defined $5;
    }
    else { # pgsql & mysql
      # parse path base/schema.table?key,part:column,list,part
      # note that it may be empty when using implicit defaults
      # accept postgresql (") and mysql (`) name quotes in table.
      die "invalid path string '$path'\n"
        unless $path =~ /
          ^(\w+)?                                   # base
           (\/((\w+\.|\"[^\"]+\"\.|\`[^\`]+\`\.)?   # schema.
               (\w+|\"[^\"]+\"|\`[^\`]+\`)))?       # table
           (\?(.+))?                                # key,part:column,list...
        /x;
      $base=$1 if defined $1;
      $tabl=$3 if defined $2;
      $kc_str = $7 if defined $7;
    }

    if (defined $kc_str)
    {
      my $in_cols = 0;
      my ($k, $c, @k, @c);
      while ($kc_str =~
	/(\w+                      # simple identifier
         |\"[^\"]*(\"\"[^\"]*)*\"  # pgsql quoted identifier
         |\`[^\`]*(\`\`[^\`]*)*\`  # mysql quoted identifier
         )([,:]?)/xg)
      {
	if ($in_cols) {
	  push @c, $1; $c++;
	}
	else {
	  push @k, $1; $k++;
	}
	die "':' key and column separation already seen"
	    if $4 eq ':' and $in_cols;
	$in_cols=1 if $4 eq ':';
      }
      $keys = [@k] if $k;
      $cols = [@c] if $in_cols;
    }
  }

  # return result as a list
  my @res = ($db, $user, $pass, $host, $port, $base, $tabl, $keys, $cols);
  verb 2, "connection parameters: @res" if $debug;
  return @res;
}

# store: dbh -> current asynchronous query
# really needed only for mysql
my %current_async_query = ();

# wait for the end of an asynchronous query, and return last result
sub async_wait($$$)
{
  my ($dbh, $db, $from) = @_;
  die "must be in async mode!" unless $async;
  return if not exists $current_async_query{$dbh};
  verb 5, "async_wait $db $from: $current_async_query{$dbh}";
  verb 6, "dbh is $dbh" if $debug;
  my @res = &{$M{$db}{get_result}}($dbh);
  delete $current_async_query{$dbh};
  return @res;
}

# serialize database connection for handling through threads
# no-op if not threaded.
sub dbh_serialize($$)
{
  my ($dbh, $db) = @_;
  if ($threads) {
    verb 5, "serializing db=$db";
    # wait for asynchronous query completion, if any
    async_wait($dbh, $db, 'serialize') if $async;
    # then serialize
    $_[0] = $dbh->take_imp_data
	or die $dbh->errstr;
  }
}

# materialize database connection handled through threads
# no-op if not threaded.
sub dbh_materialize($$)
{
  my ($dbh, $db) = @_;
  if ($threads) {
    verb 5, "materializing db=$db";
    $_[0] = DBI->connect($M{$db}{driver}, undef, undef,
			 { 'dbi_imp_data' => $dbh })
	or die $DBI::errstr;
  }
}

# global counters for the report
my $query_nb = 0;   # number of queries
my $query_sz = 0;   # size of queries
my $query_fr = 0;   # fetched summary rows
my $query_fr0 = 0;  # fetched checksum rows
my $query_data = 0; # fetched data rows for synchronizing
my $query_meta = 0; # special queries to metadata

# sql_do($dbh, $query)
# execute an SQL query on a database
# side effects: keep a count of queries and communications
sub sql_do($$$)
{
  my ($dbh, $db, $query) = @_;
  $query_nb++;
  $query_sz += length($query);
  verb 3, "$query_nb\t$query";
  # for mysql, if there is a query under way?
  # not needed for postgresql which will wait automatically
  async_wait($dbh, $db, 'sql_do') if $async; # and $db eq 'mysql';
  $current_async_query{$dbh} = $query; # keep track of current query
  return $dbh->do($query, $M{$db}{attrs});
}

# execute a parametric statement with col & key values
sub sth_param_exec($$$$@)
{
  my ($doit, $what, $sth, $keys, @cols) = @_;
  my $index = 1;
  verb 6, "executing $what";
  # ??? $sth->execute(@cols, @$keys);
  for my $val (@cols, @{$keys}) {
    $sth->bind_param($index++, $val) if $doit;
  }
  $sth->execute() if $doit;
}

# $dbh = conn($db,$base,$host,$port,$user,$pass,$source,$table,$first)
# globals: $verb
sub conn($$$$$$$$$)
{
  my ($db, $b, $h, $p, $u, $w, $src, $table, $ro) = @_;

  # prepare dbi source
  my $s;
  if (not defined $src) {
    # derive data source specification from URL
    $s = $M{$db}{source};
    $s =~ s/\%b/$b/g; # database
    $s =~ s/\%h/$h/g if defined $h; # host
    $s =~ s/host=;// if not defined $h or $h eq ''; # cleanup if unused host.
    $s =~ s/\%p/$p/g if defined $p; # port
    $s =~ s/\%u/$u/g if defined $u; # user (not used)
  }
  else {
    verb 2, "overriding DBI data source specification with: $src";
    $s = $src;
  }

  # actual connection
  verb 3, "connecting to s=$s u=$u";
  my $dbh = DBI->connect($s, $u, $w,
		{ RaiseError => 1, PrintError => 0, AutoCommit => 1 })
      or die $DBI::errstr;
  verb 4, "connected to $u\@$h:$p/$b";

  # multiple --debug leads to DBI tracing levels
  $dbh->trace($debug-1) if $debug>1;

  &{$M{$db}{initialize}}($dbh) if exists $M{$db}{initialize};

  # handle transaction
  if ($do_trans) {
    # start a big transaction...
    $dbh->begin_work or die $dbh->errstr;
  }

  # handle explicit table locking
  if ($do_lock and exists $M{$db}{lock}) {
    verb 2, "locking $table";
    sql_do($dbh, $db, &{$M{$db}{lock}}($table, $ro));
  }

  return $dbh;
}

# connect as a function for threading
sub build_conn($$$$$$$$$)
{
  my ($db, $b, $h, $p, $u, $w, $s, $t, $ro) = @_;
  verb 2, "connecting...";
  my $dbh = conn($db, $b, $h, $p, $u, $w, $s, $t, $ro);
  # max length of blobs to fetch, may be ignored by driver...
  $dbh->{LongReadLen} = $longreadlen if defined $longreadlen;
  $dbh->{LongTruncOk} = 0;
  # back to serialized form for threads
  dbh_serialize($dbh, $db);
  return $dbh;
}

############################################################## DATABASE QUERIES

# build cols=? sql-expression
sub is_equal($$$$)
{
  my ($dbh, $dhpbt, $db, $cols) = @_;
  my $expr = '';
  for my $att (@$cols) {
    $expr .= ' AND ' if $expr;
    $expr .= $att;
    if (col_is_not_null($dbh, $dhpbt, $att)) {
      $expr .= '=?';
    }
    else {
      $expr .= $M{$db}{safeeq};
    }
  }
  return $expr;
}

# get all attribute names, possibly ignoring a set of columns
sub get_table_attributes($$$$@)
{
  my ($dbh, $db, $base, $table, @ignore) = @_;
  dbh_materialize($dbh, $db);
  $query_meta++;
  async_wait($dbh, $db, 'attributes') if $async;
  my $sth = $dbh->column_info($base, &{$M{$db}{tableid}}($table), '%');
  my ($row, %cols);
  while ($row = $sth->fetchrow_hashref()) {
    $cols{$$row{COLUMN_NAME}} = 1;
  }
  $sth->finish;
  for my $k (@ignore) {
    delete $cols{$k};
  }
  dbh_serialize($dbh, $db);
  return sort keys %cols;
}

# return the primary key
sub get_table_pkey($$$$)
{
  my ($dbh, $db, $base, $table) = @_;
  dbh_materialize($dbh, $db);
  $query_meta++;
  async_wait($dbh, $db, 'pkey') if $async;
  my @keys = $dbh->primary_key($base, &{$M{$db}{tableid}}($table));
  dbh_serialize($dbh, $db);
  return @keys;
}

# wrap column_info with a cache
# dhpbt is db:host:port:base:table
my %column_info = ();
sub get_column_info($$$)
{
  my ($dbh, $dhpbt, $col) = @_;
  my ($db, $base, $table) = (split /:/, $dhpbt)[0,3,4];
  return undef unless $M{$db}{column_info};
  if (not exists $column_info{"$dhpbt/$col"})
  {
    # else try to get it
    $query_meta++;
    # ??? for some obscure reason, this fails is postgresql under -T
    async_wait($dbh, $db, 'column info') if $async;
    verb 6, "column_info: $db $base $table $col" if $debug;
    my $sth =
      $dbh->column_info($base, &{$M{$db}{tableid}}($table),
			&{$M{$db}{unquote}}($col));
    die "column_info not implemented by driver" unless defined $sth;
    my $h = $sth->fetchrow_hashref();
    die "column information not returned" unless defined $h;
    $column_info{"$dhpbt/$col"} = $h;
    $sth->finish();
  }
  return $column_info{"$dhpbt/$col"};
}

# tell whether a column is declared NOT NULL, assume nullable if unknown
sub col_is_not_null($$$)
{
  my ($dbh, $dhpbt, $col) = @_;
  my $h = get_column_info($dbh, $dhpbt, $col);
  my $notnull = 0; # default is to assume that it is nullable
  if (not defined $h) {
    $notnull = 0;
  }
  elsif (defined ${$h}{NULLABLE}) {
    $notnull = ${$h}{NULLABLE}==0;
  }
  elsif (defined ${$h}{IS_NULLABLE}) {
    $notnull = ${$h}{IS_NULLABLE} eq 'NO';
  }
  # else default is 0
  verb 6, "not null for $dhpbt:$col is $notnull" if $debug;
  return $notnull;
}

# return type of column
sub col_type($$$$)
{
  my ($dbh, $dhpbt, $db, $col) = @_;
  # sqlite and firebird do not implement column_info
  # ??? hmmm... should it be text or int? it depends.
  return $M{$db}{cktype}{4} unless $M{$db}{column_info};
  #return $M{$db}{text} unless $M{$db}{column_info};
  # mysql & pgsql
  my $h = get_column_info($dbh, $dhpbt, $col);
  die 'column type not found' unless defined ${$h}{TYPE_NAME};
  # pgsql
  return ${$h}{pg_type} if $db eq 'pgsql';
  # mysql
  my $type = ${$h}{TYPE_NAME};
  # ??? workaround the full type name...
  $type .= '(' . ${$h}{COLUMN_SIZE} . ')';
  # if $type =~ /^(VAR)?CHAR$/i and ${$h}{COLUMN_SIZE};
  return $type;
}

# count table rows
# $sth = count($dbh,$db,$table,$condition)
sub count($$$$)
{
  my ($dbh, $db, $table, $where) = @_;
  my $query = "SELECT COUNT(*) FROM " . $table . ($where? " WHERE $where": '');
  $query_nb++;
  $query_sz += length($query);
  async_wait($dbh, $db, 'count') if $async;
  verb 3, "$query_nb\t$query";
  $current_async_query{$dbh} = $query;
  my $sth = $dbh->prepare($query, $M{$db}{attrs}) or die $dbh->errstr;
  $sth->execute() or die $dbh->errstr;
  return $sth;
}

# return the average whole row size considered by the comparison
# this query is not counted, it is just for statistics
sub col_size($$$$)
{
  my ($dbh, $db, $table, $cols) = @_;
  return (0) unless $cols and @$cols;
  my $q = &{$M{$db}{colsize}}($table, $cols);
  verb 4, "col_size query: $q";
  async_wait($dbh, $db, 'col_size') if $async;
  return $dbh->selectrow_array($q);
}

# @l = subs(format, @column_names)
sub subs($@)
{
  my $fmt = shift;
  my (@cols) = @_; # copy!
  for my $s (@cols) {
    my $n = $fmt;
    $n =~ s/\%s/$s/g;
    $s = $n;
  }
  return @cols;
}

# substitute null only if necessary
sub subs_null($$$$)
{
  my ($fmt, $dbh, $dhpbt, $lref) = @_;
  my @l = ();
  for my $s (@$lref) {
    push @l, col_is_not_null($dbh, $dhpbt, $s)? $s: (subs($fmt, $s))[0];
  }
  return [@l];
}

# checksum/hash one or more attributes
sub ckatts($$$$)
{
  my ($db, $algo, $sz, $atts) = @_;
  die "expecting at least one attribute" unless @$atts;
  if (@$atts > 1) {
    # several attributes
    return join '', subs(&{$M{$db}{cksum}}($algo, $sz),
			 &{$M{$db}{concat}}($sep, $atts));
  }
  else {
    # checksum one attribute
    return &{$M{$db}{ckoneatt}{$algo}}($sz, $$atts[0]);
  }
}

# generate variant of the list of pk<n>
sub key_pk_get($$$$$)
{
  my ($dbh, $dhpbt, $db, $keys, $what) = @_;
  my ($res, $i) = ('', 0);
  my ($castatt, $cast, $as, $decl, $args) =
    ($what eq 'CASTATT', $what eq 'CAST', $what eq 'AS',
     $what eq 'DECL', $what eq 'LIST');
  die "unexpected what=$what"
    unless $castatt or $cast or $as or $decl or $args;
  for my $att (@$keys) {
    $res .= ', ' if $res;
    $res .= $M{$db}{tcast_prefix} if $cast or $castatt;
    $res .= "$att" if $castatt or $as;
    $res .= ' AS ' if $as;
    $res .= "pk$i" unless $castatt;
    $res .= $M{$db}{tcast_suffix} if $cast or $castatt;
    $res .= ' ' . col_type($dbh, $dhpbt, $db, $att) if $decl;
    $i++;
  }
  return $res;
}

# build initial checksum table, dbh must be serialized
# NOTE: if 'insert' the number of rows is returned or underway
# keys: list of key attributes
# pkeys: null-protected keys
sub build_cs_table($$$$$$$$)
{
  my ($dbh, $dhpbt, $db, $table, $keys, $pkeys, $cols, $name) = @_;
  verb 2, "building checksum table ${name}0";
  sql_do($dbh, $db, "$M{$db}{drop_table} ${name}0") if $cleanup;

  # CREATE AS vs INSERT SELECT to get row count & choose types.
  my $build_checksum =
    "SELECT " .
    # KEY CHECKSUM
    # ??? hmmm... should rather use quote_nullable()? then how to unquote?
    # always use 4 bytes for hash(key), because mask is 4 bytes anyway.
    # however under usekey the key type is kept as such
    ($usekey? "@$keys": ckatts($db, $checksum, 4, $pkeys)) . " AS kcs, " .
    # then TUPLE CHECKSUM
    # this could be skipped if cols is empty...
    # it would be somehow redundant with the previous one if same size
    ckatts($db, $checksum, $checksize, [@$pkeys, @$cols]) . " AS tcs" .
    # keep KEY, only if needed
    ($usekey? '': ', ' . key_pk_get($dbh, $dhpbt, $db, $keys, 'AS')) .
    " FROM $table" . ($where? " WHERE $where": '');

  # ??? What about using quoted strings or using an array for values?
  # what would be the impact on the cksum? on pg/my compatibility?

  my $count = -1;
  if ($ckcmp eq 'create' and $M{$db}{create_as})
  {
    $count =
      sql_do($dbh, $db,
	     "CREATE " .
	     ($temp? $M{$db}{temporary}: $unlog? $M{$db}{unlogged}: '') .
	     "TABLE ${name}0 AS $build_checksum");
    # count should be available somewhere,
    # but alas does not seem to be returned by do("CREATE TABLE ... AS ... ")
    # by pgsql, although it is returned by mysql
  }
  elsif ($ckcmp eq 'insert' or not $M{$db}{create_as})
  {
    sql_do($dbh, $db,
	   "CREATE ".
	   ($temp? $M{$db}{temporary}: $unlog? $M{$db}{unlogged}: '') .
	   "TABLE ${name}0 (".
	   # KEY CHECKSUM NN?
	   'kcs ' .
       ($usekey? col_type($dbh, $dhpbt, $db, "@$pkeys"): $M{$db}{cktype}{4}) .
	   # TUPLE CHECKSUM NN?
	   ' NOT NULL, tcs ' . $M{$db}{cktype}{$checksize} . ' NOT NULL' .
	   # KEY...
	   ($usekey? '': ', ' . key_pk_get($dbh, $dhpbt, $db, $keys, 'DECL')) .
	   ");");
    $count =
      sql_do($dbh, $db, "INSERT INTO ${name}0(kcs, tcs" .
	    ($usekey? '': ', ' . key_pk_get($dbh, $dhpbt, $db, $keys, 'LIST')) .
	     ") ". $build_checksum);
  }
  else {
    die "unexpect checksum computation variant: $ckcmp";
  }
  return $count;
}

# may return a count statement if needed
# BEWARE this is expected to be called JUST AFTER build_cs_table
# which may get this information from a currently running asynchronous query
sub start_count($$$$)
{
  my ($dbh, $dhpbt, $db, $table) = @_;
  # not needed for mysql, as CREATE TABLE AS returns the created table size
  return count($dbh, $db, $table, '')
    if $ckcmp eq 'create' and not $M{$db}{create_as_returns_count};
}

# get the row count
# BEWARE this is expected to be called JUST AFTER start_count
# and may get the information from a currently running asynchronous query
sub get_count($$$$$)
{
  my ($dbh, $dhpbt, $db, $sth, $count) = @_;
  if ($ckcmp eq 'create') { # get the current count
    ($count) = async_wait($dbh, $db, 'count create async') if $async;
    unless ($M{$db}{create_as_returns_count}) {
      ($count) = $sth->fetchrow_array();
      $sth->finish();
    }
  }
  elsif ($ckcmp eq 'insert') { # 'insert' under way, just wait for the result
    ($count) = async_wait($dbh, $db, 'count insert async') if $async;
    # else returned count was okay
  }
  else {
    die "unexpect checksum computation variant: $ckcmp";
  }
  return $count;
}

# $count = compute_checksum($dbh,$dhpbt,$table,$key,$pkeys,$cols,$name,$size)
# globals: $temp $verb $cleanup $null $checksum $checksize...
sub compute_checksum($$$$$$$$$)
{
  my ($dbh, $dhpbt, $db, $table, $keys, $pkeys, $cols, $name, $size) = @_;
  dbh_materialize($dbh, $db);
  my $count = build_cs_table(
    $dbh, $dhpbt, $db, $table, $keys, $pkeys, $cols, $name);
  if (not $size) { # we need to get the count
    my $sth = start_count($dbh, $dhpbt, $db, "${name}0");
    $count = get_count($dbh, $dhpbt, $db, $sth, $count);
  }
  dbh_serialize($dbh, $db);
  return $count;
}

# compute a summary for a given level
# assumes that dbh is materialized...
sub compute_summary($$$$$$@)
{
  my ($dbh, $db, $name, $table, $skey, $level, @masks) = @_;
  die "level must be positive, got $level" unless $level>0;
  verb 2, "building summary for ${table}: ${name}$level ($masks[$level])";
  sql_do($dbh, $db, "$M{$db}{drop_table} ${name}${level}") if $cleanup;
  # from table and attributes
  my ($kcs, $tcs, $from) = ('kcs', 'tcs', ${name} . ($level-1));
  if (defined $tup_cs and $level==1)
  {
    $tcs = $tup_cs;
    $kcs = $key_cs if defined $key_cs;
    $kcs = "@$skey" if $usekey; # must be simple!
    $from = $table;
  }
  # create summary table
  my $create_table =
    "CREATE " .
    ($temp? $M{$db}{temporary}: $unlog? $M{$db}{unlogged}: '') .
    "TABLE ${name}${level}";
  # summary table contents
  my $select = "SELECT " .
                 &{$M{$db}{andop}}($kcs, $masks[$level]) . " AS kcs, " .
                  $M{$db}{$agg} . "(${tcs}) AS tcs " .
	       "FROM ${from} " .
	       # the "& mask" is really a modulo operation
	       "GROUP BY " . &{$M{$db}{andop}}(${kcs}, $masks[$level]);
  if ($M{$db}{create_as}) {
    sql_do($dbh, $db, "$create_table AS $select");
  }
  else { # create + insert
    sql_do($dbh, $db,
	   "$create_table(kcs $M{$db}{cktype}{4}, " .
	                 "tcs $M{$db}{cktype}{$checksize})");
    sql_do($dbh, $db, "INSERT INTO ${name}${level}(kcs,tcs) $select");
  }
}

# compute_summaries($dbh, $name, @masks)
# globals: $verb $temp $unlog $agg $cleanup
sub compute_summaries($$$$$@)
{
  my ($dbh, $db, $name, $table, $skey, @masks) = @_;
  dbh_materialize($dbh, $db);
  # compute cascade of summary tables
  for my $level (1 .. @masks-1) {
    compute_summary($dbh, $db, $name, $table, $skey, $level, @masks);
  }
  dbh_serialize($dbh, $db); # will async_wait if needed
}

# get info for investigated a list of key checksums (kcs)
# $sth = selkcs($dbh, $table, $mask, $get_id, @kcs)
# note that kcs is a key but for level 0 where there may be collisions.
sub selkcs($$$$$@)
{
  my ($dbh, $db, $table, $skey, $level, $mask, $get_key, @kcs) = @_;
  my ($kcs, $tcs) = ('kcs', 'tcs');
  if (defined $tup_cs and $level==0)
  {
    $tcs = $tup_cs;
    $kcs = $key_cs if defined $key_cs;
    $kcs = "@$skey" if $usekey;
  }
  my $query =
      "SELECT $kcs AS kcs, $tcs AS tcs" .
        # if kcs==pk, do not transfer the key
        (($get_key and not $usekey)?
	 ', ' . key_pk_get(0, 0, $db, $skey, $tup_cs? 'AS': 'LIST'): '') .
      " FROM $table ";
  # the "& mask" is really a modulo operation
  $query .= "WHERE " . &{$M{$db}{andop}}($kcs, $mask) .
                   " IN (" . join(',', @kcs) . ') ' if @kcs;
  $query .= "ORDER BY $kcs";
  $query .= ', ' . key_pk_get(0, 0, $db, $skey, $tup_cs? 'CASTATT': 'CAST')
    if $get_key and not $usekey;
  # keep trac of running query
  verb 3, "$query_nb\t$query";
  $current_async_query{$dbh} = $query;
  my $sth = $dbh->prepare($query, $M{$db}{attrs}) or die $dbh->errstr;
  $query_nb++;
  $query_sz += length($query);
  $sth->execute();
  return $sth;
}

# investigate an "kcs/mask" list to show corresponding keys.
# get_bulk_keys($dbh, $table, $nature, @kcs_masks)
# globals: $verb $report
sub get_bulk_keys($$$$$@)
{
  my ($dbh, $db, $table, $kcs_att, $key_att, $nature, @kcs_masks) = @_;
  verb 1, "investigating $nature chunks (@kcs_masks): $table $kcs_att $key_att";

  # shortcut, nothing to investigate
  return [] unless @kcs_masks;

  dbh_materialize($dbh, $db);
  my @keys = (); # results
  my $cond = ''; # select query condition. must not be empty.
  for my $kcs_mask (@kcs_masks) {
    my ($kcs,$mask) = split '/', $kcs_mask;
    $cond .= ' OR ' if $cond;
    $cond .= &{$M{$db}{andop}}($kcs_att, $mask) . " = $kcs";
  }
  $cond = "($where) AND ($cond)" if defined $tup_cs and $where;
  my $count = 0;
  my $query = "SELECT $key_att FROM $table WHERE $cond";
  # ORDER BY?
  # no asynchronous query: we need the result right away
  my $sth = $dbh->prepare($query);
  $query_nb++;
  $query_sz += length($query);
  verb 3, "$query_nb\t$query";
  $sth->execute();
  while (my (@key) = $sth->fetchrow_array()) {
    $count ++;
    push @keys, [@key];
    print "$nature @key\n" if $report;
  }
  dbh_serialize($dbh, $db);

  verb 4, "$nature count=$count";
  return \@keys;
}

sub table_cleanup($$$$)
{
  my ($dbh, $db, $name, $levels) = @_;
  verb 5, "cleaning $db/$name";
  dbh_materialize($dbh, $db);
  sql_do($dbh, $db, "DROP TABLE ${name}0") unless $tup_cs;
  for my $i (1 .. $levels) {
    sql_do($dbh, $db, "DROP TABLE ${name}$i");
  }
  dbh_serialize($dbh, $db); # async_wait if needed
}

# compare list items
sub list_cmp(\@\@)
{
  my ($l1, $l2) = @_;
  verb 6, "comparing @{$l1} and @{$l2}\n" if $debug;
  die "list length must match" unless @{$l1} == @{$l2};
  for my $i (0 .. $#{$l1}) {
    my $cmp = $$l1[$i] cmp $$l2[$i];
    return $cmp if $cmp != 0;
  }
  return 0; # all list items are equal
}

############################################################### MERGE ALGORITHM

# this is the core of the comparison algorithm
# compute differences by climbing up the tree, output result on the fly.
# differences($dbh1, $dbh2, $db1, $db2, $n1, $n2, $t1, $t2, $k1, $k2, @masks)
# globals: $max_report $verb $report
sub differences($$$$$$$$$$@)
{
  my ($dbh1, $dbh2, $db1, $db2, $n1, $n2, $t1, $t2, $k1, $k2, @masks) = @_;
  my $level = @masks-1; # number of last summary table
  my ($mask, $count, $todo) = (0, 0, 1); # mask of previous table
  my (@insert, @update, @delete, @mask_insert, @mask_delete); # results
  my @kcs = ();

  dbh_materialize($dbh1, $db1);
  dbh_materialize($dbh2, $db2);

  while ($level>=0 and $todo)
  {
    my @next_kcs = ();
    verb 3, "investigating level=$level (@kcs)";

    if ($max_report && $level>0 && @kcs>$max_report) {
      print "giving up at level $level: too many differences.\n" .
	    "\tadjust --max-ratio option to proceed " .
	    "(current ratio is $max_ratio, $max_report diffs)\n" .
	    "\tkcs list length is " . scalar @kcs . ": @kcs\n";
      dbh_serialize($dbh1, $db1);
      dbh_serialize($dbh2, $db2);
      return;
    }

    # select statement handlers
    my ($tab1, $tab2) = ($n1.$level, $n2.$level);
    ($tab1, $tab2) = ($t1, $t2) if $tup_cs and $level==0;
    my $s1 = selkcs($dbh1, $db1, ${tab1}, $k1, $level, $mask, !$level, @kcs);
    my $s2 = selkcs($dbh2, $db2, ${tab2}, $k2, $level, $mask, !$level, @kcs);

    # wait for results...
    if ($async) {
      async_wait($dbh1, $db1, 'diff 1');
      async_wait($dbh2, $db2, 'diff 2');
    }

    # content of one row from the above select result
    my ($kcs1, $kcs2, $tcs1, $tcs2, @key1, @key2);

    # let us merge the two ordered select
    while (1)
    {
      # update current lists if necessary
      if (not defined $kcs1 and $s1->{Active}) {
	($kcs1, $tcs1, @key1) = $s1->fetchrow_array();
	if (defined $kcs1) { # new row
	  @key1 = ($kcs1) if !$level and $usekey; # fix key, not transferred
	  $level? $query_fr++: $query_fr0++;
	  #print "read 1: $kcs1, $tcs1", defined $key1? $key1:'', "\n";
	}
      }
      if (not defined $kcs2 and $s2->{Active}) {
	($kcs2, $tcs2, @key2) = $s2->fetchrow_array();
	if (defined $kcs2) { # new row
	  @key2 = ($kcs2) if !$level and $usekey; # fix key, not transferred
	  $level? $query_fr++: $query_fr0++;
	  #print "read 2: $kcs2, $tcs2", defined $key2? $key2:'', "\n";
	}
      }
      # nothing left on both side, merge is complete
      last unless defined $kcs1 or defined $kcs2;
      verb 6, "merging: $kcs1,$tcs1,@key1 / $kcs2,$tcs2,@key2" if $debug;
      # else at least one of the list contains something
      if (# we are dealing with two tuples
	  defined $kcs1 and defined $kcs2 and
	  # their key checksums are equal
	  $kcs1==$kcs2 and
	  # for level 0, the keys are also equal
	  ($level or list_cmp(@key1,@key2)==0))
      {
	die "unexpected undefined tuple checksum" # if not null is wrong...
	  unless defined $tcs1 and defined $tcs2;
	if ($tcs1 ne $tcs2) { # but non matching checksums
	  if ($level) {
	    push @next_kcs, $kcs1; # to be investigated at next level...
	  } else {
	    # the level-0 table keeps the actual key
	    $count ++;
	    push @update, [@key1];
	    print "UPDATE @key1\n" if $report; # final result
	  }
	}
	# else the tuple checksums match, nothing to do!
	# both tuples are consummed
	undef $kcs1; undef $tcs1; undef @key1;
	undef $kcs2; undef $tcs2; undef @key2;
      }
      # if they do not match, one is missing or less than the other
      elsif (# right side is empty, only something on the left side
	     not defined $kcs2 or
	     # or the left side id checksum is less than right side
	     (defined $kcs1 and ($kcs1<$kcs2 or
	       # or special case for level 0 on kcs collision
	       (not $level and $kcs1==$kcs2 and list_cmp(@key1,@key2)<0))))
      {
	# more kcs (/key) in table 1
	if ($level) {
	  # a whole chunck is empty on the right side, managed later
	  push @mask_insert, "$kcs1/$masks[$#masks]";
	} else {
	  $count ++;
	  push @insert, [@key1];
	  print "INSERT @key1\n" if $report; # final result
	}
	# left tuple is consummed
	undef $kcs1; undef $tcs1; undef @key1;
      }
      # this could be a else
      elsif (# left side is empty, only something in the right side
	     not defined $kcs1 or
	     # or the right side id checksum is less than left side
	     (defined $kcs2 and ($kcs1>$kcs2 or
	       # special case for level 0 on kcs collision
	       (not $level and $kcs1==$kcs2 and list_cmp(@key1,@key2)>0))))
      {
	# more kcs in table 2
	if ($level) {
	  # a whole chunck is empty on the left side, managed later
	  push @mask_delete, "$kcs2/$masks[$#masks]";
	} else {
	  $count ++;
	  push @delete, [@key2];
	  print "DELETE @key2\n" if $report; # final result
	}
	# right tuple is consummed
	undef $kcs2; undef $tcs2; undef @key2;
      }
      else {
	die "this state should never happen\n" .
	  " - 1: $kcs1, $tcs1, @key1\n" .
	  " - 2; $kcs2, $tcs2, @key2\n";
      }
    }
    # close queries
    $s1->finish();
    $s2->finish();
    # make ready for next round
    $level--; # next table! 0 is the initial checksum table
    $mask = pop @masks; # next mask
    @kcs = @next_kcs; # kcss to be investigated on next round
    $todo = @kcs;
  }

  dbh_serialize($dbh1, $db1);
  dbh_serialize($dbh2, $db2);

  return ($count, \@insert, \@update, \@delete, \@mask_insert, \@mask_delete);
}

####################################################################### OPTIONS

use Getopt::Long qw(:config no_ignore_case);

# option management
GetOptions(
  # help
  "help|h" => sub { usage(0, 0, ''); },
  "options|option|o" => sub { usage(1, 0, ''); },
  "manual|man|m" => sub { usage(2, 0, ''); },
  # verbosity
  "verbose|v+" => \$verb,
  "debug|d+" => \$debug,
  # parameterization of the algorithm
  "checksum-computation|cc=s" => \$ckcmp,
  "checksum-function|checksum|cf|c=s" => \$checksum,
  "checksum-size|check-size|checksize|cs|z=i" => \$checksize,
  "aggregate-function|aggregate|agg|af|a=s" => \$agg,
  "null|n=s" => \$null,
  "where|w=s" => \$where,
  "separator|s=s" => \$sep,
  # algorithm parameters and variants
  "use-key|uk|u!" => \$usekey,
  "key-checksum|kcs=s" => \$key_cs,
  "use-null|usenull|un!" => \$usenull,
  "tuple-checksum|tup-checksum|tcs=s" => \$tup_cs,
  "size=i" => \$size,
  "folding-factor|factor|f=i" => \$factor,
  "maximum-ratio|max-ratio|max|mr|x=f" => \$max_ratio,
  "maximum-levels|max-levels|ml=i" => \$max_levels,
  "maximum-report|max-report=i" => \$max_report,
  "mask-left|maskleft" => sub { $maskleft = 1; },
  "mask-right|maskright" => sub { $maskleft = 0; },
  "time-out|timeout|to=i" => sub {
    # ??? some stats output?
    my $timeout_delay = $_[1];
    $SIG{ALRM} = sub { die "timeout $timeout_delay\n"; };
    alarm $timeout_delay;
  },
  # auxiliary tables
  "temporary|temp|tmp|t!" => \$temp,
  "unlogged|unlog|U!" => \$unlog,
  "cleanup!" => \$cleanup,
  "clear!" => \$clear,
  "prefix|p=s" => \$prefix,
  # connection
  "source-1|source1|1=s" => \$source1,
  "source-2|source2|2=s" => \$source2,
  "ask-password|ask-passwd|ask-pass|ap!" => \$ask_pass,
  "environment-password|env-password|env-passwd|env-pass|ep=s" => \$env_pass,
  "transaction|trans|tr!" => \$do_trans,
  "lock!" => \$do_lock,
  # functions
  "synchronize|sync|S!" => \$synchronize,
  "do-it|do|D!" => \$do_it,
  "skip-inserts!" => \$skip_inserts,
  "skip-updates!" => \$skip_updates,
  "skip-deletes!" => \$skip_deletes,
  "expect|e=i" => \$expect,
  "expect-warn" => \$expect_warn, # hidden option used by the validation
  "report|r!" => \$report,
  # parallelism
  "asynchronous|A!" => \$async,
  "na|nA|X" => sub { $async = 0; },
  "threads|T!" => \$threads,
  "nt|nT|N" => sub { $threads = 0; },
  # stats
  "statistics|stats:s" => \$stats,
  "stats-name=s" => \$name, # name of test
  # misc
  "long-read-len|lrl|L=i" => \$longreadlen,
  "version|V" => sub { print "$0 version is $script_version\n"; exit 0; },
  "pg-copy:i" => \$pg_copy
) or die "$! (try $0 --help)";

# propagate expect specification
$max_report = $expect
  if defined $expect and not $expect_warn and not defined $max_report;

# set default locking if not set
$do_lock = $synchronize if not defined $do_lock;

# handle stats option
$stats = 'txt' if defined $stats and $stats eq '';

die "--temporary and --unlogged are exclusive"
  if $temp and $unlog;

die "invalid value for stats option: $stats  for 'txt' or 'csv'"
  unless not defined $stats or $stats =~ /^(csv|txt)$/;

# consistency check for --cc
die "invalid checksum computation variant: '$ckcmp' for 'create' or 'insert'"
  unless $ckcmp eq 'create' or $ckcmp eq 'insert';

# minimal check for provided data sources
die "data source 1 must be a DBI connection string: $source1"
  if defined $source1 and $source1 !~ /^dbi:/i;

die "data source 2 must be a DBI connection string: $source2"
  if defined $source2 and $source2 !~ /^dbi:/i;

# fix factor size
$factor = 1 if $factor<1;
$factor = 30 if $factor>30;

# use pg_copy if possible, currently for inserts
$pg_copy = 128 if defined $pg_copy and ($pg_copy eq '' or $pg_copy eq '0');

# intermediate table names
# what about putting the table name as well?
my ($name1, $name2) = ("${prefix}_1_", "${prefix}_2_");

# argument management
usage(0, 0, 'expecting 2 arguments') unless @ARGV == 2;

# first connection
my ($db1, $u1, $w1, $h1, $p1, $b1, $t1, $k1, $c1) = parse_conn(shift);

die "unexpected auth in first URI under sqlite"
  if defined $db1 and $db1 eq 'sqlite' and
    (defined $u1 or defined $h1 or defined $p1);

# set defaults and check minimum definitions.
$db1 = 'pgsql' unless defined $db1;
$u1 = $ENV{USER} unless defined $u1;
$h1 = '' unless defined $h1; # defaults to Unix socket
$p1 = $M{$db1}{port} if not defined $p1 and exists $M{$db1}{port};
# k/c defaults set later

# these are obviously necessary:-)
die "no base on first connection" unless defined $b1 or defined $source1;
die "no table on first connection" unless defined $t1 or defined $source1;

# second connection
my ($db2, $u2, $w2, $h2, $p2, $b2, $t2, $k2, $c2) = parse_conn(shift);

die "unexpected auth in second URI under sqlite"
  if defined $db2 and $db2 eq 'sqlite' and
    (defined $u2 or defined $h2 or defined $p2);

# default values for connection 2 is mostly to reuse from connection 1
$db2 = $db1 unless defined $db2;
$u2 = $u1 unless defined $u2;
$h2 = $h1 unless defined $h2;
# same as fist iff same driver, or driver default
$p2 = ($db2 eq $db1)? $p1: $M{$db2}{port}
  unless defined $p2 and exists $M{$db2}{port};
$b2 = $b1 unless defined $b2;
$t2 = $t1 unless defined $t2;
# k/c defaults set later

die "null should be 'text' or 'hash', got $null"
  unless $null =~ /^(text|hash)$/i;

die "checksum should be 'md5', 'ck' or 'fnv', got ($checksum)"
  unless $checksum =~ /^(md5|ck|fnv)$/i;

die "checksize must be 2, 4 or 8, got ($checksize)"
  unless $checksize =~ /^[248]$/;

die "aggregate must be 'xor' or 'sum', got ($agg)"
  unless $agg =~ /^(xor|sum)$/i;

# database connection...
if (defined $env_pass and not defined $w1) {
  $w1 = $ENV{"${env_pass}1"};
  $w1 = $ENV{$env_pass} unless defined $w1;
}
if ($ask_pass and not defined $w1) {
  require Term::ReadPassword;
  $w1 = Term::ReadPassword::read_password('connection 1 password> ');
}

# post cleanup
($h1, $p1, $u1, $w1) = ('', '', '', '') if $db1 eq 'sqlite';
($h2, $p2, $u2, $w2) = ('', '', '', '') if $db2 eq 'sqlite';

$w2 = $w1 unless $w2 or not $w1 or $u1 ne $u2 or $h1 ne $h2 or $p1 ne $p2;

if (defined $env_pass and not defined $w2) {
  $w2 = $ENV{"${env_pass}2"};
  $w2 = $ENV{$env_pass} unless defined $w2;
}
if ($ask_pass and not defined $w2) {
  require Term::ReadPassword;
  $w2 = Term::ReadPassword::read_password('connection 2 password> ');
}

# some sanity checks
die "sorry, --pg-copy option requires connections to postgresql"
  if defined $pg_copy and ($db1 ne 'pgsql' or $db2 ne 'pgsql');

die "--pg_copy must be strictly positive, got '$pg_copy'"
  if defined $pg_copy and $pg_copy <= 0;

# sanity check skipped under debugging so as to test
die "sorry, threading does not seem to work with PostgreSQL driver"
  if not $debug and $threads and ($db1 eq 'pgsql' or $db2 eq 'pgsql');

die "sorry, --pg-copy currently requires --no-async"
  if not $debug and defined $pg_copy and $async;

# fix some settings for SQLite
if (not $debug and ($db1 eq 'sqlite' or $db2 eq 'sqlite'))
{
  # SQLite/PostgreSQL & SQLite/MySQL
  if ($db1 ne $db2) {
    # ??? not null detection optimization does not work for SQLite...
    if ($null eq 'hash') {
      warn "hash null handling does not work in mixed SQLite mode, using text";
      $null = 'text';
    }
    # see unpack in PGC_MD5 definition?
    if ($checksum eq 'md5') {
      warn "md5 checksum does not work in mixed SQLite mode, using ck";
      $checksum = 'ck';
    }
    # signed integer issue...
    if ($agg eq 'xor' and ($db1 eq 'pgsql' or $db2 eq 'pgsql')) {
      warn "xor aggregate does not work in SQLite/PostgreSQL mode, using sum";
      $agg = 'sum';
    }
  }
  # adjust some settings
  if ($do_lock and $db1 eq $db2 and $b1 eq $b2) {
    warn "sorry, disabling locking, cannot lock same base twice with sqlite";
    $do_lock = 0;
  }
  if ($do_lock and not $do_trans) {
    warn "locking requires a transaction with sqlite";
    $do_trans = 1;
  }
}

if (not $debug)
{
  if ($async and (not $M{$db1}{async} or not $M{$db2}{async})) {
    warn "sorry, no asynchronous mode with $db1 or $db2";
    $async = 0;
  }
  if ($threads and (not $M{$db1}{threads} or not $M{$db2}{threads})) {
    warn "sorry, no threads with $db1 or $db2";
    $threads = 0;
  }
  # fix default options when using threads...
  if ($threads) {
    my $changed = 0;
    # it seems that statements are closed when playing with threads
    # so we cannot use transactions.
    $do_trans = 0, $changed++ unless $do_trans==0;
    #warn "WARNING $changed options fixed for threads..." if $changed;
    warn "WARNING option '--transaction' disabled for threads..." if $changed;
    # note: do_lock & temp==0 seems a bad idea.
  }
  # work around multiple issues with firebird...
  if ($db1 eq 'firebird' or $db2 eq 'firebird') {
    if ($do_trans) {
      warn "sorry, no transaction with firebird";
      $do_trans = 0;
    }
    if ($temp) {
      warn "sorry, no temporary tables with firebird";
      $temp = 0;
    }
    if ($do_lock) {
      warn "sorry, no table locks with firebird";
      $do_lock = 0;
    }
    unless ($clear) {
      warn "sorry, forcing --clear with firebird";
      $clear = 1;
    }
    if ($agg eq 'xor') {
      warn "sorry, xor is not implemented for firebird, using sum";
      # sum does not work well either
      $agg = 'sum';
    }
    if ($checksum eq 'md5') {
      warn "sorry, md5 checksum not implemented for firebird, using ck";
      $checksum = 'ck';
    }
    if ($checksize==8) {
      warn "sorry, downgrading checksize to 4 for firebird";
      $checksize = 4;
    }
  }
}

# consistency check for --lock & --transaction
if ($do_lock and ($db1 eq 'pgsql' or $db2 eq 'pgsql')) {
  die "--lock requires --transaction for pgsql" unless $do_trans;
}
if ($do_lock and ($db1 eq 'sqlite' or $db2 eq 'sqlite')) {
  die "--lock requires --transaction for sqlite" unless $do_trans;
}

# there is signed (pg)/unsigned (my) issue with key xor4 in mixed mode
# at least with md5. note that the answer seems okay in the end, but more
# path than necessary are investigated.
die "sorry, xor aggregate does not work well in pg/my mixed mode"
  if not $debug and $agg eq 'xor' and
     ($db1 eq 'pgsql' and $db2 eq 'mysql' or
      $db1 eq 'mysql' and $db2 eq 'pgsql');

# consistency checks for --use-(key|tuple)(-checksum)?
die "--use-key and --key-checksum are not compatible"
  if $usekey and defined $key_cs;

die "--tuple-checksum implies either --use-key or --key-checksum"
  if defined $tup_cs and not ($usekey or defined $key_cs);

die "--key-checksum implies --tuple-checksum"
  if defined $key_cs and not defined $tup_cs;

# set needed attributes for asynchronous queries
if ($async)
{
  if ($db1 eq 'pgsql' or $db2 eq 'pgsql') {
    use DBD::Pg qw(:async); # just for 1 & 4 constants
    $M{pgsql}{attrs} = { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT };
  }
  if ($db1 eq 'mysql' or $db2 eq 'mysql') {
    # alas, mysql lacks the nice lazyness of PG_OLDQUERY_WAIT,
    # so I have to always try to wait before a prepare/do
    $M{mysql}{attrs} = { async => 1 };
  }
}

# ??? what about other checks?

########################################## THREADED AND NON THREADED OPERATIONS

#    .   initializations
#    .   parse options
# 0 | |  connection1 // connection2        # connect to databases
#   | |  begin1      // begin2             # start transaction
#   | |  lock1       // lock2              # lock tables
#    .   get default columns if necessary
# 1 | |  checksum1 // checksum2            # build checksum tables
#   | |  count1    // count2               # get table sizes
#    .   compute masks based on sizes
# 2 | |  summary1 // summary2              # build summary tables
#    *   compute differences               # start recursive algorithm
# 3 | |  bulk1 // bulk2                    # get keys of bulk inserts/deletes
#    *   synchronize if required           # synchronize table contents
# 4 | |  cleanup1 // cleanup2              # remove "temporary" tables
#    .   commit                            # close transaction
#    .   disconnect
#    .   report if required

use Time::HiRes qw(gettimeofday tv_interval);
my ($t0, $tcks, $tsum, $tmer, $tblk, $tsyn, $tclr, $tend);
$t0 = [gettimeofday] if $stats;

verb 1, "connecting...";
my ($thr1, $thr2);
if ($threads)
{
  # share global counters
  # ??? should also take care of race conditions...
  require threads;
  require threads::shared;
  threads::shared::share(\$query_nb);
  threads::shared::share(\$query_sz);
  threads::shared::share(\$query_fr);
  threads::shared::share(\$query_fr0);
  threads::shared::share(\$query_data);
  threads::shared::share(\$query_meta);

  ($thr1) = threads->new(\&build_conn,
			 $db1, $b1, $h1, $p1, $u1, $w1, $source1, $t1, 1)
    or die "cannot create thread 1-0";

  ($thr2) = threads->new(\&build_conn,
			 $db2, $b2, $h2, $p2, $u2, $w2, $source2, $t2,
			 !$synchronize)
    or die "cannot create thread 2-0";

  verb 1, "waiting for connexions and counts...";
  ($dbh1) = $thr1->join();
  ($dbh2) = $thr2->join();
}
else
{
  ($dbh1) = build_conn($db1, $b1, $h1, $p1, $u1, $w1, $source1, $t1, 1);
  ($dbh2) = build_conn($db2, $b2, $h2, $p2, $u2, $w2, $source2, $t2,
		       !$synchronize);
}

# get/set k/c defaults once connected
if (not defined $k1) {
  $k1 = [get_table_pkey($dbh1, $db1, $b1, $t1)];
  warn "default key & attribute on first connection but not on second..."
    if defined $k2;
  die "no primary key found on first connection table $t1" unless @$k1;
}
if (not defined $c1) {
  $c1 = [get_table_attributes($dbh1, $db1, $b1, $t1, @$k1)];
  # warn, as this may lead to unexpected results...
  warn "default attributes on first connection but not on second..."
    if defined $c2;
}

# fix second connection default
$k2 = $k1 unless defined $k2;
$c2 = $c1 unless defined $c2;

# some sanity checks
die "empty key on first connection, must specify one" unless @$k1;
die "empty key on second connection, must specify one" unless @$k2;
die "key number of attributes does not match" unless @$k1 == @$k2;
die "column number of attributes does not match" unless @$c1 == @$c2;

# whether to use nullability
my ($pk1, $pk2, $pc1, $pc2);
my $fmt1 = &{$M{$db1}{null}}($null, $checksum, $checksize);
my $fmt2 = &{$M{$db2}{null}}($null, $checksum, $checksize);
my $dhpbt1 = "$db1:$h1:$p1:$b1:$t1";
my $dhpbt2 = "$db2:$h2:$p2:$b2:$t2";

# needed by next test and subs_null
dbh_materialize($dbh1, $db1);
dbh_materialize($dbh2, $db2);

# use-key checks
if ($usekey) {
  # key 1
  die "use-key option requires a scalar key, got (@$k1)" if @$k1 != 1;
  my $type1 = col_type($dbh1, $dhpbt1, $db1, $$k1[0]);
  # both next checks are usually okay from sqlite
  warn "use-key option requires an integer key 1, got $type1"
    unless $type1 =~ /int/i;
  warn "use-key option requires a NOT NULL key 1"
    unless col_is_not_null($dbh1, $dhpbt1, $$k1[0]);
  # key 2
  # size is already checked as same as k1
  my $type2 = col_type($dbh2, $dhpbt2, $db2, $$k2[0]);
  # idem, okay from sqlite
  warn "use-key option requires an integer key 2, got $type2"
    unless $type2 =~ /int/i;
  warn "use-key option requires a NOT NULL key 2"
    unless col_is_not_null($dbh2, $dhpbt2, $$k2[0]);
}

if ($usenull)
{
  # hmmm... I should ckeck that it is coherent
  # null-proctected keys, possibly hash or text
  $pk1 = subs_null($fmt1, $dbh1, $dhpbt1, $k1);
  $pk2 = subs_null($fmt2, $dbh2, $dhpbt2, $k2);
  $pc1 = subs_null($fmt1, $dbh1, $dhpbt1, $c1);
  $pc2 = subs_null($fmt2, $dbh2, $dhpbt2, $c2);
}
else
{
  $pk1 = [subs($fmt1, @$k1)];
  $pk2 = [subs($fmt2, @$k2)];
  $pc1 = [subs($fmt1, @$c1)];
  $pc2 = [subs($fmt2, @$c2)];
}

dbh_serialize($dbh1, $db1);
dbh_serialize($dbh2, $db2);

verb 1, "checksumming...";
my ($count1, $count2);
if ($tup_cs) # no checksum table to compute
{
  verb 2, "using provided checksum '$tup_cs'...";
  if (not $size) # but count is needed
  {
    verb 2, "computing sizes...";
    die "not implemented" if $threads;
    # ELSE no thread
    my $s1 = count($dbh1, $db1, $t1, $where);
    my $s2 = count($dbh2, $db2, $t2, $where);
    if ($async) {
      async_wait($dbh1, $db1, 'count 1');
      async_wait($dbh2, $db2, 'count 2');
    }
    ($count1) = $s1->fetchrow_array();
    ($count2) = $s2->fetchrow_array();
    $s1->finish();
    $s2->finish();
  }
}
else # must compute checksum table
{
  if ($threads) {
    ($thr1) = threads->new(\&compute_checksum, $dbh1, $dhpbt1, $db1, $t1,
			   $k1, $pk1, $pc1, $name1, $size)
      or die "cannot create thread 1-1";

    ($thr2) = threads->new(\&compute_checksum, $dbh2, $dhpbt2, $db2, $t2,
			   $k2, $pk2, $pc2, $name2, $size)
      or die "cannot create thread 2-1";

    verb 1, "waiting for connexions and possibly counts...";
    ($count1) = $thr1->join();
    ($count2) = $thr2->join();
  }
  else { # no thread
    # CREATE TABLE & SELECT
    ($count1) = build_cs_table($dbh1, $dhpbt1, $db1, $t1,
			       $k1, $pk1, $pc1, $name1);
    ($count2) = build_cs_table($dbh2, $dhpbt2, $db2, $t2,
			       $k2, $pk2, $pc2, $name2);
    # SELECT COUNT
    if (not $size) {
      # decomposition is needed to take advantage of asynchronous queries
      my ($s1) = start_count($dbh1, $dhpbt1, $db1, "${name1}0");
      my ($s2) = start_count($dbh2, $dhpbt2, $db2, "${name2}0");
      ($count1) = get_count($dbh1, $dhpbt1, $db1, $s1, $count1);
      ($count2) = get_count($dbh2, $dhpbt2, $db2, $s2, $count2);
    }
  }
}

verb 5, "count1=$count1 count2=$count2" if not $size;
verb 1, "computing size and masks after folding factor...";
$count1 = $count2 = $size if $size;
$size = $count1>$count2? $count1: $count2; # MAX size of both tables

# stop at this number of differences
if (not (defined $max_report or $expect_warn and defined $expect)) {
  $max_report = int($max_ratio * $size);
  # bee cool with small stuff...
  $max_report = 100 if $max_report < 100;
}

# can we already stop now?
my $min_diff = abs($count2-$count1);
die "too many differences, at least $min_diff > $max_report, " .
    "consider raising --max-ratio or --max-report"
  if defined $max_report and $min_diff>$max_report;

# compute initial "full" masks which must be larger than size
my ($mask, $nbits, @masks) = (0, 0);
while ($mask < $size) {
  $mask = 1+($mask<<1);
  $nbits++;
}
push @masks, $mask; # this is the full mask, which is skipped later on
while ($mask) {
  if ($maskleft) {
    $mask &= ($mask << $factor);
  }
  else {
    $mask >>= $factor;
  }
  push @masks, $mask;
}
my $levels = @masks;
# handle cut-off option
splice @masks, $max_levels if $max_levels and @masks>$max_levels;
verb 3, "masks=(@masks)";

if ($stats) {
  # under skip async nothread, the checksum may still be underway
  if ($async and not $threads and not $size) {
    async_wait($dbh1, $db1, 'stats 1');
    async_wait($dbh2, $db2, 'stats 2');
  }
  $tcks = [gettimeofday];
}
# note: if stats are not required, asynchronous queries may still be underway

verb 1, "building summary tables...";
if ($threads)
{
  $thr1 = threads->new(\&compute_summaries, $dbh1, $db1,
		       $name1, $t1, $k1, @masks)
    or die "cannot create thread 1-2";

  $thr2 = threads->new(\&compute_summaries, $dbh2, $db2,
		       $name2, $t2, $k2, @masks)
    or die "cannot create thread 2-2";

  $thr1->join();
  $thr2->join();
}
else
{
  #compute_summaries($dbh1, $db1, $name1, @masks);
  #compute_summaries($dbh2, $db2, $name2, @masks);
  # hmmm... possibly try to parallelize with asynchronous queries...
  # no threads here, no need to materialize and serialize handlers
  for my $level (1 .. @masks-1) {
    compute_summary($dbh1, $db1, $name1, $t1, $k1, $level, @masks);
    compute_summary($dbh2, $db2, $name2, $t2, $k2, $level, @masks);
  }
  if ($async) {
    async_wait($dbh1, $db1, 'summary 1');
    async_wait($dbh2, $db2, 'summary 2');
  }
}

$tsum = [gettimeofday] if $stats;

verb 1, "looking for differences...";
my ($count, $ins, $upt, $del, $bins, $bdel) =
  differences($dbh1, $dbh2, $db1, $db2, $name1, $name2,
	      $t1, $t2, $k1, $k2, @masks);
verb 2, "differences done";

$tmer = [gettimeofday] if $stats;

# now take care of big chunks of INSERT or DELETE if necessary
# should never happen in normal "few differences" conditions
verb 1, "bulk delete: @{$bdel}" if defined $bdel and @$bdel;
verb 1, "bulk insert: @{$bins}" if defined $bins and @$bins;

my ($bic, $bdc, $insb, $delb) = (0, 0);
if ((defined $bins and @$bins) or (defined $bdel and @$bdel))
{
  verb 1, "resolving bulk inserts and deletes...";
  # this cost two full table-0 scans, one on each side...
  if ($threads)
  {
    # hmmm... thread is useless if the list is empty
    $thr1 = threads->new(\&get_bulk_keys, $dbh1, $db1,
			 # table
			 defined $tup_cs? $t1: "${name1}0",
			 # key checksum attribute
	        defined $key_cs? $key_cs: ($usekey and $tup_cs)? "@$k1": 'kcs',
			 # key attribute
		defined $tup_cs? key_pk_get(0, 0, $db1, $k1, 'AS'):
		  $usekey? 'kcs': key_pk_get(0, 0, $db1, $k1, 'LIST'),
			 'INSERT', @$bins)
      or die "cannot create thread 1-3";

    $thr2 = threads->new(\&get_bulk_keys, $dbh2, $db2,
			 # table
			 defined $tup_cs? $t2: "${name2}0",
			 # key checksum attribute
		defined $key_cs? $key_cs: ($usekey and $tup_cs)? "@$k2": 'kcs',
			 # key attribute
		defined $tup_cs? key_pk_get(0, 0, $db2, $k2, 'AS'):
		  $usekey? 'kcs': key_pk_get(0, 0, $db2, $k2, 'LIST'),
			 'DELETE', @$bdel)
      or die "cannot create thread 2-3";

    $insb = $thr1->join();
    $delb = $thr2->join();
  }
  else
  {
    $insb = get_bulk_keys($dbh1, $db1,
			  # table
			  defined $tup_cs? $t1: "${name1}0",
			  # key checksum attribute
		defined $key_cs? $key_cs: ($usekey and $tup_cs)? "@$k1": 'kcs',
			  # key attribute
		defined $tup_cs? key_pk_get(0, 0, $db1, $k1, 'AS'):
		  $usekey? 'kcs': key_pk_get(0, 0, $db1, $k1, 'LIST'),
			  'INSERT', @$bins);
    $delb = get_bulk_keys($dbh2, $db2,
			  # table
			  defined $tup_cs? $t2: "${name2}0",
			  # key checksum attribute
	        defined $key_cs? $key_cs: ($usekey and $tup_cs)? "@$k2": 'kcs',
			  # key attribute
		defined $tup_cs? key_pk_get(0, 0, $db2, $k2, 'AS'):
		  $usekey? 'kcs': key_pk_get(0, 0, $db2, $k2, 'LIST'),
			  'DELETE', @$bdel);
  }

  # ??? fix?
  $insb = [] unless defined $insb;
  $delb = [] unless defined $delb;

  $bic = @$insb;
  $bdc = @$delb;
}
else
{
  # ??? is it necessary?
  $insb = [] unless defined $insb;
  $delb = [] unless defined $delb;
}

# update count with bulk contents
$count += $bic + $bdc;

# bulk timestamp
$tblk = [gettimeofday] if $stats;

############################################################### SYNCHRONIZATION

# perform an actual synchronization of data
if ($synchronize and
    # is there something to do?
    (@$del or @$ins or @$upt or defined $insb or defined $delb))
{
  verb 1, "synchronizing...";

  dbh_materialize($dbh1, $db1);
  dbh_materialize($dbh2, $db2);

  # if the overall comparison is not under a transaction,
  # the synchronization is nevertheless.
  $dbh2->begin_work if $do_it and not $do_trans;

  # build query helpers
  my $where_k1 = is_equal($dbh1, $dhpbt1, $db1, $k1);
  my $where_k2 = is_equal($dbh2, $dhpbt2, $db2, $k2);
  my $set_c2 = (join '=?, ', @$c2) . '=?';

  # DELETE rows, including updates with copy
  if (@$del or @$delb or ($pg_copy and @$upt))
  {
    my $del_sql = "DELETE FROM $t2 WHERE " .
	($where? "($where) AND ": '') . $where_k2;
    verb 2, $del_sql;
    my $del_sth = $dbh2->prepare($del_sql) if $do_it;
    my @alldels = ();
    push @alldels, (@$del, @$delb) unless $skip_deletes;
    push @alldels, @$upt if $pg_copy and not $skip_updates;
    for my $d (@alldels) {
      sth_param_exec($do_it, "DELETE $t2", $del_sth, $d);
    }
    # undef $del_sth;
  }

  # insert/update rows
  # note: I could skip fetching if there is no data column
  if ($pg_copy and (@$ins or @$upt or defined $insb)) { # use COPY
    sql_do($dbh2, $db2, "COPY $t2(" . join(',', @$k2, @$c2) . ") FROM STDIN");
    #async_wait($dbh2, $db2, 'copy from 2') if $async;
    my $select = "SELECT " . join(',', @$k1, @$c1) . " FROM $t1 WHERE ";
    $select .= "($where) AND " if $where;
    $select .= "(" . join(',', @$k1) . ") IN (";
    # we COPY both inserts and updates
    my @allins = ();
    push @allins, (@$ins, @$insb) unless $skip_inserts;
    push @allins, @$upt unless $skip_updates;
    while (@allins) {
      my $bulk = '';
      for my $k (splice(@allins, 0, $pg_copy)) { # chunked
	$bulk .= ',' if $bulk;
	#$copy_bulk .= $dbh1->quote($k);
	$bulk .= "(@$k)";
	$query_data++;
      }
      sql_do($dbh1, $db1, "COPY ($select$bulk)) TO STDOUT");
      #async_wait($dbh1, $db1, 'copy to 1') if $async;
      my $row = '';
      while (($dbh1->pg_getcopydata($row)) != -1) {
	$dbh2->pg_putcopydata($row) if $do_it;
      }
    }
    $dbh2->pg_putcopyend();
  }
  else { # use generic INSERT/UPDATE

    # get values for insert or update
    my ($val_sql, $val_sth);
    if ($c1 and @$c1)
    {
      $val_sql = "SELECT " . join(',', @$c1) . " FROM $t1 WHERE " .
      ($where? "($where) AND ": '') . $where_k1;
      verb 2, $val_sql;
      $val_sth = $dbh1->prepare($val_sql)
        if @$ins or @$insb or @$upt;
    }

    # handle inserts
    if ((@$ins or @$insb) and not $skip_inserts)
    {
      my $ins_sql = "INSERT INTO $t2(" . join(',', @$c2, @$k2) . ") " .
	'VALUES(?' . ',?' x (@$k2+@$c2-1) . ')';
      verb 2, $ins_sql;
      my $ins_sth = $dbh2->prepare($ins_sql) if $do_it;
      for my $i (@$ins, @$insb) {
	$query_data++;
	my @c1values = ();
	# query the other column values for key $i
	if ($c1 and @$c1) {
	  sth_param_exec(1, "SELECT $t1", $val_sth, $i);
	  @c1values = $val_sth->fetchrow_array();
	  # hmmm... may be raised on blobs?
	  die "unexpected values fetched for insert"
	    unless @c1values and @c1values == @$c1;

	  &{$M{$db1}{close_cursor}}($val_sth) if exists $M{$db1}{close_cursor};
	}
	# then insert the missing tuple
	sth_param_exec($do_it, "INSERT $t2", $ins_sth, $i, @c1values);
      }
      #  $ins_sth
    }

    # handle updates
    if (@$upt and not $skip_updates)
    {
      die "there must be some columns to update" unless $c1;
      my $upt_sql = "UPDATE $t2 SET $set_c2 WHERE " .
      ($where? "($where) AND ": '') . $where_k2;
      verb 2, $upt_sql;
      my $upt_sth = $dbh2->prepare($upt_sql) if $do_it;
      for my $u (@$upt)
      {
	$query_data++;
	# get value for key $u
	sth_param_exec(1, "SELECT $t1", $val_sth, $u);
	my @c1values = $val_sth->fetchrow_array();
	# hmmm... may be raised on blobs?
	die "unexpected values fetched for update"
        unless @c1values and @c1values == @$c1;
	# use it to update the other table
	sth_param_exec($do_it, "UPDATE $t2", $upt_sth, $u, @c1values);

	&{$M{$db1}{close_cursor}}($val_sth) if exists $M{$db1}{close_cursor};
      }
      # $upt_sth
    }
  }

  # close synchronization transaction if any
  $dbh2->commit if $do_it and not $do_trans;

  dbh_serialize($dbh1, $db1);
  dbh_serialize($dbh2, $db2);

  print
      "\n",
      "*** WARNING ***\n",
      "\n",
      "The synchronization was not performed, sorry...\n",
      "Also set non documented option --do-it if you really want to do it.\n",
      "BEWARE that you may lose your data and your friends!\n",
      "Back-up before running a synchronization!\n",
      "\n"
      unless $do_it;
}

$tsyn = [gettimeofday] if $stats;

if ($clear)
{
  verb 4, "clearing...";
  my $levels = @masks - 1;
  if ($threads)
  {
    $thr1 = threads->new(\&table_cleanup, $dbh1, $db1, $name1, $levels)
	or die "cannot create thread 1-4";
    $thr2 = threads->new(\&table_cleanup, $dbh2, $db2, $name2, $levels)
	or die "cannot create thread 2-4";
    $thr1->join();
    $thr2->join();
  }
  else
  {
    table_cleanup($dbh1, $db1, $name1, $levels);
    table_cleanup($dbh2, $db2, $name2, $levels);
  }
  verb 4, "clearing done."
}

$tclr = [gettimeofday] if $stats;

# recreate database handler for the end...
dbh_materialize($dbh1, $db1);
dbh_materialize($dbh2, $db2);

# unlock for mysql
if ($do_lock)
{
  if ($db1 eq 'mysql') {
    sql_do($dbh1, $db1, "UNLOCK TABLES");
    async_wait($dbh1, $db1, 'unlock 1') if $async;
  }
  if ($db2 eq 'mysql') {
    sql_do($dbh2, $db2, "UNLOCK TABLES");
    async_wait($dbh2, $db2, 'unlock 2') if $async;
  }
}

# end of the big transactions...
if ($do_trans)
{
  $dbh1->commit or die $dbh1->errstr;
  $dbh2->commit or die $dbh2->errstr;
}

# final timestamp
$tend = [gettimeofday] if $stats;

# some stats are collected out of time measures
if ($stats)
{
  my $tk1 = subs_null(&{$M{$db1}{null}}('text', 0, 0), $dbh1, $dhpbt1, $k1);
  $key_size = col_size($dbh1, $db1, $t1, $tk1);
  $col_size = col_size($dbh1, $db1, $t1,
		       [subs(&{$M{$db1}{null}}('text', 0, 0), @$c1)]);
}

# final stuff:
# $count: number of differences found
# @$ins @$insb: key insert (individuals and bulks)
# @$upd: key update
# @$del @$delb: key delete (ind & bulks)

# close both connections
$dbh1->disconnect() or warn $dbh1->errstr;
$dbh2->disconnect() or warn $dbh2->errstr;

#################################################################### STATISTICS

verb 1, "done, $count differences found...";

sub delay($$)
{
  my ($t0,$t1) = @_;
  return sprintf "%.6f", tv_interval($t0,$t1);
}

if (defined $stats)
{
  # ??? some of these statistics are not trustworthy when running with threads

  # build options as a bit vector
  my $options =
      (($pg_copy?1:0) << 11) |  # --pg-copy=...
      (($tup_cs?1:0) << 10) |   # --tuple-checksum=...
      (($key_cs?1:0) << 9) |    # --key-checksum=...
      ($do_lock << 8) |         # --lock
      ($async << 7) |           # --asynchronous
      ($usenull << 6) |         # --use-null
      ($maskleft << 5) |        # --mask-left
      (($temp?1:0) << 4) |      # --temporary
      ($do_trans << 3) |        # --transaction
      ($usekey << 2) |          # --use-key
      ($threads << 1) |         # --thread
      $synchronize;             # --synchronize

  # summary of performances/instrumentation
  if ($stats eq 'csv')
  {
    # CSV format is:
    # test_name TEXT,
    # (tables): size INT,
    # db: db1 TEXT, db2 TEXT,
    # tables: diffs INT, expect INT, key_size INT, col_size INT,
    # algo: revision INT, factor INT, levels INT, checksum TEXT, cksize INT,
    #       aggregate TEXT, options INT,
    # query: nb INT, size INT, nrows INT,
    # times: cksum, summary, merge, bulks, sync, clear, end FLOAT
    # test_date TIMESTAMP
    my ($s0,$m0,$h0,$d0,$mo0,$y0) = gmtime($$t0[0]);

    # timestamp string in SQL format
    my $date =
      sprintf "%04d-%02d-%02d %02d:%02d:%02d",
	1900+$y0, 1+$mo0, $d0, $h0, $m0, $s0;

    # output CSV result, for a machine
    print "$name,$size,$db1,$db2,$count,",
      (defined $expect? $expect: -1),
      ",$key_size,$col_size,$revision,$factor,",
      scalar @masks, ",$checksum,$checksize,",
      "$agg,$options,",
      # query counters
      "$query_nb,$query_sz,$query_fr,$query_fr0,$query_data,$query_meta,",
      delay($t0, $tcks), ",",
      delay($tcks, $tsum), ",",
      delay($tsum, $tmer), ",",
      delay($tmer, $tblk), ",",
      delay($tblk, $tsyn), ",",
      delay($tsyn, $tclr), ",",
      delay($tclr, $tend), ",$date\n";
  }
  else
  {
    # print stats for a human being.
    print
      "      revision: $revision\n",
      "       testing: $db1/$db2\n",
      "    table size: $size\n",
      "folding factor: $factor\n",
      "        levels: ", scalar @masks, " (cut-off from $levels)\n",
      "  query number: $query_nb\n",
      "    query size: $query_sz\n",
      "  fetched sums: $query_fr\n",
      "  fetched chks: $query_fr0\n",
      "  fetched data: $query_data\n",
      "query metadata: $query_meta\n",
      "      key size: $key_size\n",
      "      col size: $col_size\n",
      "   diffs found: $count\n",
      "     expecting: ", (defined $expect? $expect: 'undef'), "\n",
      "       options: $options\n",
      "    total time: ", delay($t0, $tend), "\n",
      "      checksum: ", delay($t0, $tcks), "\n",
      "       summary: ", delay($tcks, $tsum), "\n",
      "         merge: ", delay($tsum, $tmer), "\n",
      "         bulks: ", delay($tmer, $tblk), "\n",
      "       synchro: ", delay($tblk, $tsyn), "\n",
      "         clear: ", delay($tsyn, $tclr), "\n",
      "           end: ", delay($tclr, $tend), "\n";
  }
}

# check count for the validation
# this simple strategy is okay because the validation does a comparison, then
# a synchronization and then checks that both tables are indeed identical.
# this check may fail if there is a hash collision.
if (defined $expect and $expect != $count) {
  if ($expect_warn) {
    warn "unexpected number of differences (got $count, expecting $expect)";
  }
  else {
    die "unexpected number of differences (got $count, expecting $expect)";
  }
}
