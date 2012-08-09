#
# $Id: test.mk 1143 2012-08-09 12:38:22Z fabien $
#
# run pg_comparator validation checks

# make AUTH=pgsql://login:password@localhost \
#      PGCOPTS='--stats-name=test --stats=csv'  test_fast_pgsql

SHELL	= /bin/bash

# rough testing, including synchronization
AUTH	= localhost
DB	= test

# test case generation
ROWS	= 100
TOTAL	= 8
KEYS	= 0
COLS	= 2
WIDTH	= 2

# algorithm parameters
FOLD	= 3
NULL	= text
AGG	= xor
CF	= ck
CS	= 8

# other options
#ENG	= INNODB ## MUCH TOO SLOW!
ENG	= MYISAM
PGCOPTS	=
# for internal use by this makefile
pgcopts	=

# generate column names
coln	:= $(shell n=$(COLS); while let n--; do echo $$n; done)
col1	:= $(shell echo $(addprefix a,$(coln))|tr ' ' ',')
col2	:= $(shell echo $(addprefix b,$(coln))|tr ' ' ',')

keyn	:= $(shell n=$(KEYS); while let n--; do echo $$n; done)
key1	:= $(shell echo id $(addprefix k,$(keyn))|tr ' ' ',')
key2	:= $(shell echo id $(addprefix l,$(keyn))|tr ' ' ',')

# use common default
AUTH1	= $(AUTH)
AUTH2	= $(AUTH)

# connections
CONN1	= $(AUTH1)/$(DB)/foo1?$(key1):$(col1)
CONN2	= $(AUTH2)/$(DB)/foo2?$(key2):$(col2)

# exected differences when validating
DIFFS	= -t $(TOTAL)

# pre/post comparison settings
PG_PRE	= :
PG_POST	= :

#
# generate a test case for pg_comparator
#
# Create, Modify, Keep
RUNOPS	= -C -M -K
run:
	./test_pg_comparator.sh \
		-1 $(AUTH1) -2 $(AUTH2) -b $(DB) \
		-k $(KEYS) -c $(COLS) -r $(ROWS) -w $(WIDTH) \
		$(DIFFS) -e $(ENG) $(RUNOPS)

#
# test comparison & synchronization
#
# make AUTH=calvin:hobbes@home DB=calvin COLS=0 ROWS=1000 test_run
test_run: pg_comparator
	# generate test case
	$(MAKE) run
	# pre-settings
	$(PG_PRE)
	# first comparison
	time ./pg_comparator -f $(FOLD) --cf=$(CF) -a $(AGG) --cs=$(CS) \
	    --null=$(NULL) -e $(TOTAL) $(PGCOPTS) $(pgcopts) $(CONN1) $(CONN2)
	# comparison & synchronize
	time ./pg_comparator -S -D -f $(FOLD) --cf=$(CF) -a $(AGG) --cs=$(CS) \
	    --null=$(NULL) -e $(TOTAL) $(PGCOPTS) $(pgcopts) $(CONN1) $(CONN2)
	# check that synchronization was okay
	time ./pg_comparator -f $(FOLD) --cf=$(CF) -a $(AGG) --cs=$(CS) \
	    --null=$(NULL) -e 0 $(PGCOPTS) $(pgcopts) $(CONN1) $(CONN2)
	# post-settings
	$(PG_POST)

#
# COMBINATORIAL VALIDATION
#
# test additionnal key & other columns
# there is always a simple primary key
test_cases:
	$(MAKE) KEYS=0 COLS=0 test_run
	$(MAKE) KEYS=0 COLS=1 test_run
	$(MAKE) KEYS=0 COLS=2 test_run
	$(MAKE) KEYS=1 COLS=0 test_run
	$(MAKE) KEYS=1 COLS=1 test_run
	$(MAKE) KEYS=1 COLS=2 test_run
	$(MAKE) KEYS=2 COLS=3 test_run

# folding log2: fold by 2, 4, 8, 128
test_foldings:
	$(MAKE) FOLD=1 test_cases
	$(MAKE) FOLD=2 test_cases
	$(MAKE) FOLD=3 test_cases
	$(MAKE) FOLD=7 test_cases

# test checksum sizes
test_cs:
	$(MAKE) CS=4 test_foldings
	$(MAKE) CS=8 test_foldings

# test checksum functions
test_cf:
	$(MAKE) CF=md5 test_cs
	$(MAKE) CF=ck test_cs

# test null handling
test_null:
	$(MAKE) NULL=text test_cf
	$(MAKE) NULL=hash test_cf

# test checksum aggregate functions
test_agg:
	$(MAKE) AGG=sum test_null
	$(MAKE) AGG=xor test_null

# test table sizes
test_sizes:
	$(MAKE) ROWS=20 test_agg
	$(MAKE) ROWS=100 test_agg
	$(MAKE) ROWS=1026 test_agg
	#$(MAKE) ROWS=10000 test_foldings

test_mysql: test_sizes
test_pgsql: test_sizes
test_mixed:
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH2) test_sizes
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH1) test_sizes

#
# FAST TESTS: just a subset of combinations
#
test_fast:
	$(MAKE) CF=md5 CS=8 AGG=sum NULL=text \
		FOLD=1 KEYS=0 COLS=0 test_run
	$(MAKE) CF=md5 CS=8 AGG=sum NULL=hash \
		FOLD=1 KEYS=0 COLS=1 test_run
	$(MAKE) CF=md5 CS=8 AGG=sum NULL=text \
		FOLD=1 KEYS=1 COLS=2 test_run
	$(MAKE) CF=md5 CS=4 AGG=sum NULL=hash \
		FOLD=3 KEYS=0 COLS=2 test_run
	$(MAKE) CF=ck CS=8 AGG=sum NULL=text \
		FOLD=2 KEYS=0 COLS=1 test_run
	$(MAKE) CF=ck CS=4 AGG=sum NULL=hash \
		FOLD=4 KEYS=1 COLS=0 test_run
	$(MAKE) CF=ck CS=8 AGG=sum NULL=hash \
		FOLD=4 KEYS=1 COLS=3 test_run
	$(MAKE) CF=md5 CS=8 AGG=xor NULL=text \
		FOLD=5 KEYS=1 COLS=1 test_run
	$(MAKE) CF=ck CS=4 AGG=xor NULL=hash \
		FOLD=7 KEYS=2 COLS=3 test_run
	$(MAKE) CF=ck CS=8 AGG=xor NULL=text \
		FOLD=6 KEYS=1 COLS=2 test_run
	$(MAKE) CF=ck CS=8 AGG=xor NULL=hash \
		FOLD=8 KEYS=2 COLS=3 test_run

# this is scripted rather than relying on dependencies
test_fast_mysql:
	$(MAKE) test_fast

test_fast_pgsql:
	$(MAKE) test_fast

test_fast_mixed:
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH2) test_fast
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH1) test_fast

# experimental feature tests
test_fast_async:
	$(MAKE) pgcopts+=' -A' test_fast

test_fast_thread:
	$(MAKE) pgcopts+=' -T --debug' test_fast

#
# VALIDATION
#
# make \
#   AUTH1=pgsql://... \
#   AUTH2=mysql://... \
#   ENG=myisam \
#   validate_fast

VALIDATE=test_fast

validate:
	[[ "$(AUTH1)" == pgsql://* ]] || exit 1
	[[ "$(AUTH2)" == mysql://* ]] || exit 2
	[[ "$(VALIDATE)" ]] || exit 3
	@echo "# VALIDATING PGSQL..."
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH1) pgcopts+=' -A -N' $(VALIDATE)_pgsql
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH1) pgcopts+=' -X -N' $(VALIDATE)_pgsql
	# threads never worked with pgsql (try with -T --debug)
	@echo "# VALIDATING MYSQL..."
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH2) pgcopts+=' -A -N' $(VALIDATE)_mysql
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH1) pgcopts+=' -X -N' $(VALIDATE)_mysql
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH2) pgcopts+=' -A -T' $(VALIDATE)_mysql
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH2) pgcopts+=' -X -T' $(VALIDATE)_mysql
	@echo "# VALIDATING MIXED..."
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH2) pgcopts+=' -A -N' $(VALIDATE)_mixed
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH2) pgcopts+=' -X -N' $(VALIDATE)_mixed
	@echo "# VALIDATION DONE"

validate_fast:
	$(MAKE) VALIDATE=test_fast validate

validate_full:
	$(MAKE) VALIDATE=test validate
#
# PERFORMANCE
#
performance:
	$(MAKE) CF=ck CS=8 AGG=sum NULL=text pgcopts+=' --stats -u' \
		ROWS=500000 KEYS=0 COLS=4 FOLD=7 WIDTH=2 test_run

performance_mixed:
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH1) performance
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH2) performance
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH1) performance
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH2) performance
