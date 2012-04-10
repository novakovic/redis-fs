/**
 * This is a simple driver which runs each of the tests implemented in
 * our source files *_test.c
 *
 * The testing framework uses cutest:
 *
 *   http://cutest.sourceforge.net/
 *
 * Steve
 * --
 */


#include <stdlib.h>
#include <stdio.h>


#include "CuTest.h"
#include "pathutil_test.h"
#include "zlib_test.h"

/* defined in pathutil_test.c */
CuSuite *pathutil_getsuite ();
/* defined in zlib_test.c */
CuSuite *zlib_getsuite ();


/**
 * Run all the available tests, and report upon their results.
 *
 * NOTE: The reporting is minimal unless there are failures.
 *
 */
void
RunAllTests (void)
{
    CuString *output = CuStringNew ();
    CuSuite *suite = CuSuiteNew ();

    CuSuiteAddSuite (suite, pathutil_getsuite ());
    CuSuiteAddSuite (suite, zlib_getsuite ());

    CuSuiteRun (suite);
    CuSuiteSummary (suite, output);
    CuSuiteDetails (suite, output);
    printf ("%s\n", (char *) output->buffer);
}



/**
 * Entry point to our code.  Start the tests.
 */
int
main (int argc, char *argv[])
{
    RunAllTests ();

    return 0;
}
