# $Id: dev.mk 437 2008-06-03 12:59:37Z fabien $

# script distribution
dir	= $(name)
VERSION	= dev

# files to distribute
F.c	= $(wildcard *.c)
F.in	= $(wildcard *.in)
F.dist 	= $(F.c) $(F.in) $(DOCS) $(DATA) $(name) INSTALL LICENSE Makefile

# derived documentations
$(name): $(name).pl
	sed -e's/@VERSION@/$(VERSION)/' $< > $@
	chmod +x $@

tar: $(name)-$(VERSION).tgz

$(name)-$(VERSION).tgz: $(F.dist)
	cd .. ; \
	tar czf $@ $(addprefix $(dir)/, $(F.dist))

clean: dev-clean
dev-clean:
	$(RM) $(name) *~

check: $(name)
	dropdb test
	createdb test
	./test_pg_comparator.sh -r 100
