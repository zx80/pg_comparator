# $Id: dev.mk 701 2010-04-03 14:10:17Z fabien $

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
F.dist 	= $(F.c) $(F.sql) $(F.in) $(DOCS) $(DATA) $(name) \
		INSTALL LICENSE Makefile

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

# for a new version, do make version=1.5.3 publish in the .. directory
$(name)-$(VERSION).tgz: $(F.dist)
	ln -s . $(name)-$(VERSION) ; \
	tar czf $@ $(addprefix $(name)-$(VERSION)/, $(F.dist)) ; \
	chmod a+r $@ ; \
	rm -f $(name)-$(VERSION)

clean: dev-clean
dev-clean:
	$(RM) $(name) *~ LICENSE

include test.mk
