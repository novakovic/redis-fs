
#
#  Flags and stuffs.
#
CFLAGS=-ggdb -Wall -Werror  -I.
LDFLAGS=


#
#  By default make our filesystem.
#
all: link tests


#
#  Clean.
#
clean:
	rm tests *.o     || true
	rm -f pathutil.h || true
	rm -f pathutil.c || true

#
#  Symlink
#
link:
	ln -sf ../src/pathutil.c .
	ln -sf ../src/pathutil.h .

#
#  Indent & tidy.
#
tidy:
	indent --no-space-after-function-call-names --no-space-after-casts --braces-after-if-line --no-tabs --indent-level 4 -bli0 -cdb -sc pathutil_test.c
	indent --no-space-after-function-call-names --no-space-after-casts --braces-after-if-line --no-tabs --indent-level 4 -bli0 -cdb -sc zlib_test.c


#
#  Test code
#
tests: pathutil.o AllTests.o CuTest.o pathutil_test.o zlib_test.o
	gcc -o tests pathutil.o AllTests.o CuTest.o  pathutil_test.o zlib_test.o -lz
