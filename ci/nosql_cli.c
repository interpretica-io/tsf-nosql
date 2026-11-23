/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief One ta_nosql operation from the command line
 *
 *   nosql_cli available
 *   nosql_cli <engine> '<config json>' <op> ['<args json>' | @file]
 *
 * Opens a connection, runs the operation, prints the result (or the
 * error class and message) and closes. For poking at a server while
 * writing a test; the contract tests are ci/core_test.c.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ta_nosql_core.h"

/* The arguments: the text, or the contents of @file (argv is short). */
static char *
args_of(const char *a)
{
    FILE  *f;
    char  *buf;
    long   n;

    if (a[0] != '@')
        return strdup(a);
    f = fopen(a + 1, "r");
    if (f == NULL)
        return NULL;
    fseek(f, 0, SEEK_END);
    n = ftell(f);
    fseek(f, 0, SEEK_SET);
    buf = malloc((size_t)n + 1);
    if (buf != NULL)
    {
        size_t got = fread(buf, 1, (size_t)n, f);

        buf[got] = '\0';
    }
    fclose(f);
    return buf;
}

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
    {
        char *a = args_of(argc > 4 ? argv[4] : "{}");

        rc = ta_nosql_call(h, argv[3], a != NULL ? a : "{}", &out, &err);
        free(a);
    }
    if (rc == 0)
        printf("%s\n", out);
    else
        printf("%s: %s: %s\n", argv[3], ta_nosql_errcls_name(err.cls),
               err.msg);
    free(out);
    ta_nosql_close(h, &err);
    return rc == 0 ? 0 : 1;
}
