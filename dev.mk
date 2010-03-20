# $Id: dev.mk 506 2010-03-20 21:43:46Z fabien $

# script distribution
dir	= $(name)
VERSION	= dev
YEAR	= 2010

# files to distribute
F.c	= $(wildcard *.c)
F.in	= $(wildcard *.in)
F.dist 	= $(F.c) $(F.in) $(DOCS) $(DATA) $(name) INSTALL LICENSE Makefile

# derived documentations
$(name): $(name).pl
	sed -e's/@VERSION@/$(VERSION)/g;s/@YEAR@/$(YEAR)/g' $< > $@
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
AUTH=localhost
DB=test
NCOLS=2
COL1=a1,a0
COL2=b1,b0
CONN1=$(AUTH)/$(DB)/foo1?id:$(COL1)
CONN2=$(AUTH)/$(DB)/foo2?id:$(COL2)

check: $(name)
	./test_pg_comparator.sh -r 100 -a $(AUTH) -b $(DB) -k -c $(NCOLS)
	./pg_comparator -S -D $(CONN1) $(CONN2)
	./pg_comparator $(CONN1) $(CONN2)
