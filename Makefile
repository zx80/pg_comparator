# $Id: Makefile 202 2004-08-26 13:02:05Z coelho $

name		= pg_comparator

SCRIPTS		= $(name)
MODULES		= null_string checksum
DATA_built	= null_string.sql checksum.sql
DATA		= xor_aggregate.sql
DOCS		= README.$(name) \
			README.xor_aggregate \
			README.checksum \
			README.null_string

EXTRA_CLEAN	= $(name).1 $(name).html pod2htm?.tmp

PGXS	= $(shell pg_config --pgxs)
include $(PGXS)

# derived documentations
$(name).1: $(name); pod2man $< > $@
$(name).html: $(name); pod2html $< > $@

# distribution
dir		= $(name)
VERSION		= 1.1
dist_files 	= *.in *.c $(DOCS) $(DATA) $(name) INSTALL LICENSE Makefile

tar: $(name)-$(VERSION).tgz

$(name)-$(VERSION).tgz:
	cd .. ; \
	tar czf $(name)-$(VERSION).tgz $(addprefix $(dir)/, $(dist_files))
