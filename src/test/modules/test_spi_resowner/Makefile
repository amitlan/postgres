# src/test/modules/test_spi_resowner/Makefile

MODULE_big = test_spi_resowner
OBJS = \
	$(WIN32RES) \
	test_spi_resowner.o
PGFILEDESC = "test_spi_resowner - SQL-callable C SPI function under a dedicated ResourceOwner"

EXTENSION = test_spi_resowner
DATA = test_spi_resowner--1.0.sql

REGRESS = ri_fastpath

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_spi_resowner
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
