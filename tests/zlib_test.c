/**
 * Test cases for zlib compression code.
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

#include <zlib.h>
#include "zlib_test.h"


/**
 * Simple test.
 */
void
TestNOP(CuTest * tc)
{
    char *ptr = NULL;
    char *nul = NULL;

    CuAssertPtrEquals(tc, nul, ptr);

    free(nul);
    free(ptr);
}


void
TestCompress(CuTest * tc)
{
    char input[] =
        {
 "This is a test input string.  I like to test inputThis is a test input string.  I like to test input."
};

    uLongf output_len = 2000;
    void *output = malloc(output_len);
    uLongf input_len = (uLongf) strlen(input);

  /**
   * Attempt the compression.
   */
    int ret =
        compress2(output, &output_len, (void *)input, input_len,
                  Z_BEST_SPEED);

  /**
   * Assert it 1.  Worked.
   */
    CuAssertTrue(tc, ret == Z_OK);

  /**
   * The output size is smaller than the input size.
   */
    CuAssertTrue(tc, output_len < input_len);

    free(output);
}


/**
 * Attempt to compress and decompress.
 */
void
TestDecompress(CuTest * tc)
{
    char input[] =
        {
 "This is a test input string.  I like to test inputThis is a test input string.  I like to test input."
};
    uLongf input_len = (uLongf) strlen(input);

    uLongf compressed_len = 2000;
    void *compressed = malloc(compressed_len);

    uLongf decompressed_len = 2000;
    void *decompressed = malloc(decompressed_len);


  /**
   * Attempt the compression.
   */
    int ret =
        compress2(compressed, &compressed_len, (void *)input, input_len,
                  Z_BEST_SPEED);

  /**
   * Assert it 1.  Worked.
   */
    CuAssertTrue(tc, ret == Z_OK);

  /**
   * The output size is smaller than the input size.
   */
    CuAssertTrue(tc, compressed_len < input_len);


  /**
   * OK now we decompress.
   */
    ret =
        uncompress(decompressed, &decompressed_len, (void *)compressed,
                   compressed_len);

  /**
   * Assert: 1. it worked.
   */
    CuAssertTrue(tc, ret == Z_OK);

  /**
   * 2. Output is longer than the input.
   */
    CuAssertTrue(tc, compressed_len < decompressed_len);

  /**
   * 3. Ouptut is equal to original starting point.
   */
    CuAssertTrue(tc, decompressed_len == input_len);
    CuAssertTrue(tc, (strcmp(decompressed, input) == 0));

    free(compressed);
    free(decompressed);
}



CuSuite *
zlib_getsuite()
{
    CuSuite *suite = CuSuiteNew();

    SUITE_ADD_TEST(suite, TestNOP);
    SUITE_ADD_TEST(suite, TestCompress);
    SUITE_ADD_TEST(suite, TestDecompress);

    return suite;
}
