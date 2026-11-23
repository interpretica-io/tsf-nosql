/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief One ta_nosql operation from the command line
 *
 *   nosql_cli available
 *   nosql_cli <engine> '<config json>' <op> ['<args json>']
 *
 * Opens a connection, runs the operation, prints the result (or the
 * error class and message) and closes. For poking at a server while
 * writing a test; the contract tests are ci/core_test.c.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ta_nosql_core.h"

int
main(int argc, char **argv)
{
    ta_nosql_err err;
    char        *out = NULL;
    int          h = 0;
    int          rc;

    if (argc == 2 && strcmp(argv[1], "available") == 0)
    {
        unsigned int engines;

        if (ta_nosql_available(&engines, &out) != 0)
            return 1;
        printf("%s\n", out);
        free(out);
        return 0;
    }
    if (argc < 4)
    {
        fprintf(stderr, "usage: %s available | <engine> <config> <op> "
                        "[<args>]\n", argv[0]);
        return 2;
    }
    if (ta_nosql_open(argv[1], argv[2], &h, &out, &err) != 0)
    {
        printf("open: %s: %s\n", ta_nosql_errcls_name(err.cls), err.msg);
        return 1;
    }
    free(out);
    out = NULL;
    rc = ta_nosql_call(h, argv[3], argc > 4 ? argv[4] : "{}", &out, &err);
    if (rc == 0)
        printf("%s\n", out);
    else
        printf("%s: %s: %s\n", argv[3], ta_nosql_errcls_name(err.cls),
               err.msg);
    free(out);
    ta_nosql_close(h, &err);
    return rc == 0 ? 0 : 1;
}
