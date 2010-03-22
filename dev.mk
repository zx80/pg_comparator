# $Id: dev.mk 551 2010-03-21 18:17:50Z fabien $

# script distribution
dir	= $(name)
VERSION	= dev
YEAR	= $(shell date +%Y)
DATE	= $(shell date +%F)
REVISION= $(shell svnversion)

# files to distribute
F.c	= $(wildcard *.c)
F.in	= $(wildcard *.in)
F.dist 	= $(F.c) $(F.in) $(DOCS) $(DATA) $(name) INSTALL LICENSE Makefile

# derived documentations
$(name): $(name).pl
	sed -e 's/@VERSION@/$(VERSION)/g;' \
	    -e 's/@REVISION@/$(REVISION)/g;' \
	    -e 's/@YEAR@/$(YEAR)/g;' \
	    -e 's/@DATE@/$(DATE)/g' $< > $@
	chmod +x $@

%: %.src
	sed -e's/@VERSION@/$(VERSION)/g;s/@YEAR@/$(YEAR)/g' $< > $@

tar: $(name)-$(VERSION).tgz

$(name)-$(VERSION).tgz: $(F.dist)
	ln -s . $(name)-$(VERSION) ; \
	tar czf $@ $(addprefix $(name)-$(VERSION)/, $(F.dist)) ; \
	chmod a+r $@ ; \
	rm -f $(name)-$(VERSION)

clean: dev-clean
dev-clean:
	$(RM) $(name) *~ LICENSE

# rough testing, including synchronization
AUTH	= localhost
DB	= test
ROWS	= 100
TOTAL	= 8
KEYS	= 0
COLS	= 2
FOLD	= 3
PGCOPTS	=

# generate column names
SHELL	= /bin/bash
coln	:= $(shell n=$(COLS); while let n--; do echo $$n; done)
col1	:= $(shell echo $(addprefix a,$(coln))|tr ' ' ',')
col2	:= $(shell echo $(addprefix b,$(coln))|tr ' ' ',')

keyn	:= $(shell n=$(KEYS); while let n--; do echo $$n; done)
key1	:= $(shell echo id $(addprefix k,$(keyn))|tr ' ' ',')
key2	:= $(shell echo id $(addprefix l,$(keyn))|tr ' ' ',')

# connextions
CONN1	= $(AUTH)/$(DB)/foo1?$(key1):$(col1)
CONN2	= $(AUTH)/$(DB)/foo2?$(key2):$(col2)

# make AUTH=calvin:hobbes@home DB=calvin COLS=0 ROWS=1000 check
check: $(name)
	# generate test tables and perform one comparison
	./test_pg_comparator.sh \
		-a $(AUTH) -b $(DB) -k $(KEYS) -c $(COLS) -r $(ROWS) \
		-t $(TOTAL) -K -- -f $(FOLD) $(PGCOPTS)
	# synchronize
	./pg_comparator -S -D -f $(FOLD) -e $(TOTAL) $(PGCOPTS) \
		$(CONN1) $(CONN2)
	# check that synchronization was okay
	./pg_comparator -e 0 -f $(FOLD) $(PGCOPTS) $(CONN1) $(CONN2)

# validation
tests:
	$(MAKE) KEYS=0 COLS=0 check
	$(MAKE) KEYS=0 COLS=1 check
	$(MAKE) KEYS=0 COLS=2 check
	$(MAKE) KEYS=1 COLS=0 check
	$(MAKE) KEYS=1 COLS=1 check
	$(MAKE) KEYS=1 COLS=2 check
	$(MAKE) KEYS=2 COLS=3 check

fold_tests:
	$(MAKE) FOLD=1 tests
	$(MAKE) FOLD=2 tests
	$(MAKE) FOLD=3 tests
	$(MAKE) FOLD=7 tests

size_tests:
	$(MAKE) ROWS=20 fold_tests
	$(MAKE) ROWS=100 fold_tests
	$(MAKE) ROWS=1026 fold_tests
