# $Id: dev.mk 522 2010-03-21 11:47:24Z fabien $

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
	rm -f $(name)-$(VERSION)

clean: dev-clean
dev-clean:
	$(RM) $(name) *~ LICENSE

# rough testing, including synchronization
AUTH	= localhost
DB	= test
ROWS	= 100
TOTAL	= 8
COLS	= 2
PGCOPTS	=

# generate column names
SHELL	= /bin/bash
numbers	:= $(shell n=$(COLS); while let n--; do echo $$n; done)
col1	:= $(shell echo $(addprefix a,$(numbers))|tr ' ' ',')
col2	:= $(shell echo $(addprefix b,$(numbers))|tr ' ' ',')

# connextions
CONN1	= $(AUTH)/$(DB)/foo1?id:$(col1)
CONN2	= $(AUTH)/$(DB)/foo2?id:$(col2)

# make AUTH=calvin:hobbes@home DB=calvin COLS=0 ROWS=1000 check
check: $(name)
	./test_pg_comparator.sh \
		-a $(AUTH) -b $(DB) -c $(COLS) -r $(ROWS) -t $(TOTAL) -k \
		-- $(PGCOPTS)
	./pg_comparator -S -D -e $(TOTAL) $(PGCOPTS) $(CONN1) $(CONN2)
	./pg_comparator -e 0 $(PGCOPTS) $(CONN1) $(CONN2)

tests:
	$(MAKE) COLS=0 check
	$(MAKE) COLS=1 check
	$(MAKE) COLS=2 check
