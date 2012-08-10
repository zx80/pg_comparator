#
# $Id: test.mk 1156 2012-08-10 08:18:32Z fabien $
#
# run pg_comparator validation checks

# make AUTH=pgsql://login:password@localhost \
#      PGCOPTS='--stats-name=test --stats=csv' fast_pg

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

noxor	=

#
# generate a test case for pg_comparator
#
# Create, Modify, Keep
RUNOPS	= -C -M -K
create:
	./test_pg_comparator.sh \
		-1 $(AUTH1) -2 $(AUTH2) -b $(DB) \
		-k $(KEYS) -c $(COLS) -r $(ROWS) -w $(WIDTH) \
		$(DIFFS) -e $(ENG) $(RUNOPS)

#
# test comparison & synchronization & check
#
# make AUTH=calvin:hobbes@home DB=calvin COLS=0 ROWS=1000 test_run
run: pg_comparator
	$(MAKE) create
	$(PG_PRE)
	time ./pg_comparator -f $(FOLD) --cf=$(CF) -a $(AGG) --cs=$(CS) \
	    --null=$(NULL) -e $(TOTAL) $(PGCOPTS) $(pgcopts) $(CONN1) $(CONN2)
	time ./pg_comparator -S -D -f $(FOLD) --cf=$(CF) -a $(AGG) --cs=$(CS) \
	    --null=$(NULL) -e $(TOTAL) $(PGCOPTS) $(pgcopts) $(CONN1) $(CONN2)
	time ./pg_comparator -f $(FOLD) --cf=$(CF) -a $(AGG) --cs=$(CS) \
	    --null=$(NULL) -e 0 $(PGCOPTS) $(pgcopts) $(CONN1) $(CONN2)
	$(PG_POST)

#
# "FULL" COMBINATORIAL VALIDATION
#
# test additionnal key & other columns
# there is always a simple primary key
full_cases:
	$(MAKE) KEYS=0 COLS=0 run
	$(MAKE) KEYS=0 COLS=1 run
	$(MAKE) KEYS=0 COLS=2 run
	$(MAKE) KEYS=1 COLS=0 run
	$(MAKE) KEYS=1 COLS=1 run
	$(MAKE) KEYS=1 COLS=2 run
	$(MAKE) KEYS=2 COLS=3 run

# folding log2: fold by 2, 4, 8, 128
full_foldings:
	$(MAKE) FOLD=1 full_cases
	$(MAKE) FOLD=2 full_cases
	#$(MAKE) FOLD=3 full_cases
	$(MAKE) FOLD=7 full_cases

# full checksum sizes
full_cs:
	$(MAKE) CS=4 full_foldings
	$(MAKE) CS=8 full_foldings

# full checksum functions
full_cf:
	$(MAKE) CF=md5 full_cs
	$(MAKE) CF=ck full_cs

# full null handling
full_null:
	$(MAKE) NULL=text full_cf
	$(MAKE) NULL=hash full_cf

# full checksum aggregate functions
full_agg:
	$(MAKE) AGG=sum full_null
	$(noxor) $(MAKE) AGG=xor full_null

# full table sizes
full_sizes:
	$(MAKE) ROWS=20 full_agg
	#$(MAKE) ROWS=100 full_agg
	$(MAKE) ROWS=1026 full_agg
	#$(MAKE) ROWS=10000 full_agg

# start full tests
full_my: full_sizes
full_pg: full_sizes
full_mix:
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH2) full_sizes
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH1) full_sizes

#
# FAST TESTS: 12 tests, just a subset of combinations
# run is 3 calls to pg_comparator: compare, sync, check sync
#
fast:
	$(MAKE) CF=md5 CS=8 AGG=sum NULL=text FOLD=1 KEYS=0 COLS=0 run
	$(MAKE) CF=ck  CS=8 AGG=sum NULL=text FOLD=2 KEYS=0 COLS=1 pgcopts+=' -u' run
	$(noxor) $(MAKE) CF=md5 CS=8 AGG=xor NULL=hash FOLD=1 KEYS=0 COLS=1 run
	$(MAKE) CF=md5 CS=8 AGG=sum NULL=text FOLD=1 KEYS=1 COLS=2 run
	$(MAKE) CF=md5 CS=4 AGG=sum NULL=hash FOLD=3 KEYS=0 COLS=2 run
	$(MAKE) CF=ck  CS=8 AGG=sum NULL=text FOLD=2 KEYS=0 COLS=1 run
	$(MAKE) CF=ck  CS=4 AGG=sum NULL=hash FOLD=4 KEYS=1 COLS=0 run
	$(MAKE) CF=ck  CS=8 AGG=sum NULL=hash FOLD=4 KEYS=1 COLS=3 run
	$(noxor) $(MAKE) CF=md5 CS=8 AGG=xor NULL=text FOLD=5 KEYS=1 COLS=1 run
	$(noxor) $(MAKE) CF=ck  CS=4 AGG=xor NULL=hash FOLD=7 KEYS=2 COLS=3 run
	$(noxor) $(MAKE) CF=ck  CS=8 AGG=xor NULL=text FOLD=6 KEYS=1 COLS=2 run
	$(noxor) $(MAKE) CF=ck  CS=8 AGG=xor NULL=hash FOLD=8 KEYS=2 COLS=3 run

# this is scripted rather than relying on dependencies
fast_my:
	$(MAKE) fast

fast_pg:
	$(MAKE) fast

fast_mix:
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH2) noxor=: fast
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH1) noxor=: fast

#
# VALIDATION
#
# make AUTH1=pgsql://... AUTH2=mysql://... validate_fast
#

# default to fast validation
VALIDATE=fast

check_validation_environment:
	[[ "$(AUTH1)" == pgsql://* ]] || exit 1
	[[ "$(AUTH2)" == mysql://* ]] || exit 2
	[[ "$(VALIDATE)" ]] || exit 3

# threads never worked with pgsql (try with -T --debug)
validate_pg: check_validation_environment
	@echo "# VALIDATING PGSQL ..."
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH1) pgcopts+=' -A -N' $(VALIDATE)_pg
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH1) pgcopts+=' -X -N' $(VALIDATE)_pg

validate_my: check_validation_environment
	@echo "# VALIDATING MYSQL..."
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH2) pgcopts+=' -A -N' $(VALIDATE)_my
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH1) pgcopts+=' -X -N' $(VALIDATE)_my
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH2) pgcopts+=' -A -T' $(VALIDATE)_my
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH2) pgcopts+=' -X -T' $(VALIDATE)_my

validate_mix: check_validation_environment
	@echo "# VALIDATING MIXED..."
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH2) pgcopts+=' -A -N' $(VALIDATE)_mix
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH2) pgcopts+=' -X -N' $(VALIDATE)_mix

validate: check_validation_environment
	@echo "# VALIDATING..."
	$(MAKE) validate_pg
	$(MAKE) validate_my
	$(MAKE) validate_mix
	@echo "# VALIDATION DONE"

# fast validation: 300 run in about 6 minutes on Ankh.
#    pg: 3 * 12 *     2
#    my: 3 * 12 *     4 # hmmm, seems slow...
#   mix: 3 * (12-5) * 2 * 2
# total: 72 + 144 + 84 = 300
validate_fast:
	$(MAKE) VALIDATE=fast validate

# full validate: 16128 tests, at least 5 hours?
#    pg: 3 * (7*3*2*2*2*2*2) * 2   = 3 * 672 * 2 = 4032
#    my: 3 * ~               * 4   = 8064
#   mix: 3 * ~      1        * 2*2 = 4032
# total: 16128
validate_full:
	$(MAKE) VALIDATE=full validate

#
# PERFORMANCE
#
performance:
	$(MAKE) CF=ck CS=8 AGG=sum NULL=text pgcopts+=' --stats -u' \
		ROWS=500000 KEYS=0 COLS=4 FOLD=7 WIDTH=2 run

performance_mixed:
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH1) performance
	$(MAKE) AUTH1=$(AUTH1) AUTH2=$(AUTH2) performance
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH1) performance
	$(MAKE) AUTH1=$(AUTH2) AUTH2=$(AUTH2) performance
