# $Id: dev.mk 1580 2017-07-07 09:12:27Z coelho $

# script distribution
dir	= $(name)
VERSION	= dev
YEAR	= $(shell date +%Y)
DATE	= $(shell date +%F)
REVISION= $(shell svnversion)

# files to distribute
F.c	= $(wildcard *.c)
F.sql	= $(wildcard *.sql)
F.in	= $(wildcard *.in)
# $(DATA): already in $(F.sql)
F.dist 	= $(F.c) $(F.sql) $(DOCS) $(name) pgcmp.control \
		INSTALL LICENSE Makefile

# default target
default: $(name)

# derived script
$(name): $(name).pl
	sed -e 's/@VERSION@/$(VERSION)/g;' \
	    -e 's/@REVISION@/$(REVISION)/g;' \
	    -e 's/@YEAR@/$(YEAR)/g;' \
	    -e 's/@DATE@/$(DATE)/g' $< > $@
	chmod a+rx $@
	perl -c $@
	touch -r $< $@

%: %.src
	sed -e's/@VERSION@/$(VERSION)/g;s/@YEAR@/$(YEAR)/g' $< > $@

tgz: $(name)-$(VERSION).tgz

# for a new version, run in the .. directory:
#   sh> make version=2.2.3 publish
# then edit pg_comparator.pl and ../local.mk to fix the version as well.
# keep a copy of the targz in ~/SAVE/SOFTS/
# distribute manually on pgfoundry.
$(name)-$(VERSION).tgz: $(F.dist)
	ln -s . $(name)-$(VERSION) ; \
	tar czf $@ $(addprefix $(name)-$(VERSION)/, $(F.dist)) ; \
	chmod a+r $@ ; \
	rm -f $(name)-$(VERSION)

clean: dev-clean
dev-clean:
	$(RM) $(name) *~ LICENSE base.db

include test.mk
