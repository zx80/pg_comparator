# $Id: Makefile 1460 2012-11-02 18:21:27Z fabien $

#
# PostgreSQL stuff
#

name		= pg_comparator

SCRIPTS		= $(name)
MODULES		= pgc_checksum pgc_casts
DATA_built	= $(MODULES:%=%.sql)
DATA		= xor_aggregate.sql
DOCS		= README.$(name) \
		  README.xor_aggregate \
		  README.pgc_checksum \
		  README.pgc_casts

EXTRA_CLEAN	= $(name).1 $(name).html pod2htm?.tmp

# get postgresql extension infrastructure
PG_CONFIG	= pg_config
PGXS	:= $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# derive documentation
$(name).1: $(name)
	pod2man --name $(name) $< > $@
	touch -r $< $@

$(name).html: $(name)
	pod2html --title $(name) $< | \
	  sed -e '/^<body style/a<h1>$(name)</h1>' > $@
	touch -r $< $@

pgsql_install: install
pgsql_uninstall: uninstall

#
# MySQL stuff
#
MY.c	= $(wildcard mysql_*.c)
MY.so	= $(MY.c:%.c=%.so)
MY.sql	= $(MY.c:%.c=%.sql)

MY_CONFIG	= mysql_config
MYDIR	= $(shell $(MY_CONFIG) --plugindir)/
MYCC	= $(CC) $(shell $(MY_CONFIG) --cflags)

mysql_%.so: mysql_%.c
	$(MYCC) -shared -o $@ $<
	chmod a+r-x $@

mysql_install: $(MY.so) $(MY.sql)
	chmod a+r $(MY.sql)
	cp -a $^ $(MYDIR)

mysql_uninstall:
	$(RM) $(addprefix $(MYDIR),$(MY.so) $(MY.sql))

#
# SQLite stuff
#
SQLITE.libdir	= /usr/local/lib

sqlite_checksum.so: sqlite_checksum.c
	gcc -Wall -fPIC -shared $< -o $@

sqlite_install: sqlite_checksum.so
	cp $< $(SQLITE.libdir)/
	chmod a+rx $(SQLITE.libdir)/sqlite_checksum.so

sqlite_uninstall:
	$(RM) $(SQLITE.libdir)/sqlite_checksum.so

clean: sqlite-clean
sqlite-clean:
	$(RM) sqlite_checksum.so

#
# common cleanup
#
clean: local-clean
local-clean:
	$(RM) *.so

#
# development stuff is ignored by the distribution
#
-include dev.mk
