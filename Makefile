# $Id: Makefile 512 2010-03-20 22:47:26Z fabien $

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
PG_CONFIG	= pg_config
PGXS	:= $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# derive documentation
$(name).1: $(name); pod2man --name $(name) $< > $@
$(name).html: $(name); pod2html --title $(name) $< > $@

# development stuff is ignored by the distribution
-include dev.mk
