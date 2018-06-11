include $(top_srcdir)/common.mk

bin_PROGRAMS = dmigrate

dmigrate_SOURCES = dmigrate.c
dmigrate_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dmigrate_LDFLAGS = $(MPI_CLDFLAGS)
dmigrate_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS) $(LUSTRE_LIBS)
