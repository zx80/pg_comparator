# $Id: Makefile 434 2008-06-03 12:56:07Z fabien $

name		= pg_comparator

SCRIPTS		= $(name)
MODULES		= checksum casts
DATA_built	= checksum.sql casts.sql
DATA		= xor_aggregate.sql
DOCS		= README.$(name) \
			README.xor_aggregate \
			README.checksum

EXTRA_CLEAN	= $(name).1 $(name).html pod2htm?.tmp

# get postgresql extension infrastructure
PGXS	= $(shell pg_config --pgxs)
include $(PGXS)

# derive documentation
$(name).1: $(name); pod2man $< > $@
$(name).html: $(name); pod2html $< > $@

# development stuff is ignored by the distribution
dev.mk:; touch $@
include dev.mk
