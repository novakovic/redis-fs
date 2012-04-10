/**
 * Test cases for pathutil code.
 *
 * The testing framework uses cutest:
 *
 *   http://cutest.sourceforge.net/
 *
 * All tests are driven by the code in AllTests.c
 *
 * Steve
 * --
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include "pathutil.h"
#include "pathutil_test.h"


/**
 * Sanity check that free(NULL); works.
 */
void
TestFreeNULL(CuTest * tc)
{
    char *ptr = NULL;
    char *nul = NULL;

    CuAssertPtrEquals(tc, nul, ptr);

    free(nul);
    free(ptr);
}

/**
 * Test we can call get_basename() on a NULL pointer.
 */
void
TestBasenameNull(CuTest * tc)
{
    char *ptr = NULL;
    char *nul = NULL;

    ptr = get_basename(nul);
    CuAssertPtrEquals(tc, nul, ptr);

    free(nul);
    free(ptr);
}

/**
 * Test we can call get_parent() on a NULL pointer.
 */
void
TestParentNull(CuTest * tc)
{
    char *ptr = NULL;
    char *nul = NULL;

    ptr = get_basename(nul);
    CuAssertPtrEquals(tc, nul, ptr);

    free(nul);
    free(ptr);
}


/**
 * Test that a non-path has a NULL parent.
 */
void
TestEmptyParent(CuTest * tc)
{
    char *expected = NULL;
    char *input = "fsdfkldsjf";
    char *output = NULL;

    output = get_parent(input);
    CuAssertStrEquals(tc, output, expected);

    free(output);
}

/**
 * Test that a non-path has a sane basename.
 */
void
TestEmptyBasename(CuTest * tc)
{
    char *expected = "fsdfkldsjf";
    char *input = "fsdfkldsjf";
    char *output = NULL;

    output = get_basename(input);
    CuAssertStrEquals(tc, output, expected);
    free(output);
}

/**
 * Test that we can be sane with simple cases
 */
void
TestSimpleBasename(CuTest * tc)
{
    char *inputs[] = { "/etc/steve",
        "/steve",
        "/fsfsddddddddddddddddddddd////steve",
        "/etc/..//steve/steve",
        "./steve",
        NULL
    };

    int i = 0;

    while (inputs[i] != NULL)
    {
        char *input = inputs[i];
        char *output = NULL;
        char *expected = "steve";

        output = get_basename(input);
        CuAssertStrEquals(tc, output, expected);

        free(output);

        i += 1;
    }
}

/**
 * Test that we can be sane with simple cases
 */
void
TestSimpleParent(CuTest * tc)
{
    char *inputs[] = { "/etc/steve",
        "/etc/",
        "/etc/fdsfsteve",
        NULL
    };

    int i = 0;

    while (inputs[i] != NULL)
    {
        char *input = inputs[i];
        char *output = NULL;
        char *expected = "/etc";

        output = get_parent(input);
        CuAssertStrEquals(tc, output, expected);

        free(output);

        i += 1;
    }
}


/**
 * Test that a parent of / is still /.
 */
void
TestRootParent(CuTest * tc)
{
    char *expected = "/";
    char *input = "/";
    char *output = NULL;

    output = get_parent(input);
    CuAssertStrEquals(tc, output, expected);

    free(output);
}


CuSuite *
pathutil_getsuite()
{
    CuSuite *suite = CuSuiteNew();

    SUITE_ADD_TEST(suite, TestFreeNULL);
    SUITE_ADD_TEST(suite, TestBasenameNull);
    SUITE_ADD_TEST(suite, TestParentNull);
    SUITE_ADD_TEST(suite, TestEmptyBasename);
    SUITE_ADD_TEST(suite, TestEmptyParent);
    SUITE_ADD_TEST(suite, TestSimpleBasename);
    SUITE_ADD_TEST(suite, TestSimpleParent);
    SUITE_ADD_TEST(suite, TestRootParent);

    return suite;
}
