/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql helpers: errors, JSON, base64, time, configuration
 */

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "ta_nosql_int.h"

/* See description in ta_nosql_int.h */
void
ta_nosql_err_set(ta_nosql_err *err, ta_nosql_errcls cls, long native,
                 const char *fmt, ...)
{
    va_list ap;

    if (err == NULL)
        return;
    err->cls = cls;
    err->native = native;
    va_start(ap, fmt);
    vsnprintf(err->msg, sizeof(err->msg), fmt, ap);
    va_end(ap);
}

/* See description in ta_nosql_core.h */
const char *
ta_nosql_errcls_name(ta_nosql_errcls cls)
{
    switch (cls)
    {
        case TA_NOSQL_OK:               return "ok";
        case TA_NOSQL_ERR_CONNECT:      return "connect";
        case TA_NOSQL_ERR_AUTH:         return "auth";
        case TA_NOSQL_ERR_PERMISSION:   return "permission";
        case TA_NOSQL_ERR_TLS:          return "tls";
        case TA_NOSQL_ERR_TIMEOUT:      return "timeout";
        case TA_NOSQL_ERR_INVALID:      return "invalid";
        case TA_NOSQL_ERR_UNSUPPORTED:  return "unsupported";
        case TA_NOSQL_ERR_UNAVAILABLE:  return "unavailable";
        case TA_NOSQL_ERR_LIMIT:        return "limit";
        case TA_NOSQL_ERR_PROTOCOL:     return "protocol";
        case TA_NOSQL_ERR_HANDLE:       return "handle";
        case TA_NOSQL_ERR_INTERNAL:     return "internal";
    }
    return "unknown";
}

/* See description in ta_nosql_int.h */
char *
ta_nosql_json_dump(const json_t *v)
{
    return json_dumps(v, JSON_COMPACT | JSON_ENCODE_ANY |
                         JSON_REAL_PRECISION(17));
}

/* See description in ta_nosql_int.h */
json_t *
ta_nosql_json_load(const char *text, size_t len, ta_nosql_err *err)
{
    json_error_t jerr;
    json_t      *v;

    v = json_loadb(text, len, JSON_DECODE_ANY, &jerr);
    if (v == NULL)
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "malformed JSON at line %d column %d: %s",
                         jerr.line, jerr.column, jerr.text);
    return v;
}

/* See description in ta_nosql_int.h */
const char *
ta_nosql_json_str(const json_t *obj, const char *name, const char *dflt)
{
    json_t *v = json_object_get(obj, name);

    return json_is_string(v) ? json_string_value(v) : dflt;
}

/* See description in ta_nosql_int.h */
long long
ta_nosql_json_int(const json_t *obj, const char *name, long long dflt)
{
    json_t *v = json_object_get(obj, name);

    if (json_is_integer(v))
        return json_integer_value(v);
    if (json_is_real(v))
        return (long long)json_real_value(v);
    return dflt;
}

/* See description in ta_nosql_int.h */
bool
ta_nosql_json_bool(const json_t *obj, const char *name, bool dflt)
{
    json_t *v = json_object_get(obj, name);

    return json_is_boolean(v) ? json_is_true(v) : dflt;
}

/* See description in ta_nosql_int.h */
char *
ta_nosql_strdupf(const char *fmt, ...)
{
    va_list ap;
    char   *s = NULL;

    va_start(ap, fmt);
    if (vasprintf(&s, fmt, ap) < 0)
        s = NULL;
    va_end(ap);
    return s;
}

static const char b64_alphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/* See description in ta_nosql_int.h */
char *
ta_nosql_b64_encode(const void *data, size_t len)
{
    const unsigned char *p = data;
    char                *out = malloc(4 * ((len + 2) / 3) + 1);
    size_t               i;
    size_t               o = 0;

    if (out == NULL)
        return NULL;
    for (i = 0; i + 2 < len; i += 3)
    {
        out[o++] = b64_alphabet[p[i] >> 2];
        out[o++] = b64_alphabet[((p[i] & 3) << 4) | (p[i + 1] >> 4)];
        out[o++] = b64_alphabet[((p[i + 1] & 15) << 2) | (p[i + 2] >> 6)];
        out[o++] = b64_alphabet[p[i + 2] & 63];
    }
    if (i < len)
    {
        out[o++] = b64_alphabet[p[i] >> 2];
        if (i + 1 < len)
        {
            out[o++] = b64_alphabet[((p[i] & 3) << 4) | (p[i + 1] >> 4)];
            out[o++] = b64_alphabet[(p[i + 1] & 15) << 2];
        }
        else
        {
            out[o++] = b64_alphabet[(p[i] & 3) << 4];
            out[o++] = '=';
        }
        out[o++] = '=';
    }
    out[o] = '\0';
    return out;
}

static int
b64_value(char c)
{
    const char *p;

    if (c == '\0')
        return -1;
    p = strchr(b64_alphabet, c);
    return p == NULL ? -1 : (int)(p - b64_alphabet);
}

/* See description in ta_nosql_int.h */
char *
ta_nosql_b64_decode(const char *text, size_t *len)
{
    size_t         n = strlen(text);
    unsigned char *out = malloc(n / 4 * 3 + 4);
    size_t         o = 0;
    unsigned int   acc = 0;
    int            bits = 0;
    size_t         i;

    if (out == NULL)
        return NULL;
    for (i = 0; i < n; i++)
    {
        int v;

        if (text[i] == '=')
            break;
        v = b64_value(text[i]);
        if (v < 0)
        {
            free(out);
            return NULL;
        }
        acc = (acc << 6) | (unsigned int)v;
        bits += 6;
        if (bits >= 8)
        {
            bits -= 8;
            out[o++] = (unsigned char)((acc >> bits) & 0xff);
        }
    }
    out[o] = '\0';
    if (len != NULL)
        *len = o;
    return (char *)out;
}

/* See description in ta_nosql_int.h */
int64_t
ta_nosql_now_us(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

/* See description in ta_nosql_int.h */
char *
ta_nosql_new_version(void)
{
    static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    static uint64_t        last;
    struct timespec        ts;
    uint64_t               v;

    /*
     * Wall-clock microseconds made strictly increasing within the
     * process: two writes of a key in one microsecond still differ.
     * Another process (another RPC server, a load thread's connection
     * in another process) cannot collide with it in a way a test would
     * see: a compare-and-set compares the token it read, and the token
     * is written in the same statement as the document.
     */
    clock_gettime(CLOCK_REALTIME, &ts);
    v = (uint64_t)ts.tv_sec * 1000000 + (uint64_t)ts.tv_nsec / 1000;
    pthread_mutex_lock(&lock);
    if (v <= last)
        v = last + 1;
    last = v;
    pthread_mutex_unlock(&lock);
    return ta_nosql_strdupf("%llu", (unsigned long long)v);
}

/* See description in ta_nosql_int.h */
bool
ta_nosql_coll_ok(const char *s)
{
    size_t i;

    if (s == NULL || s[0] < 'a' || s[0] > 'z')
        return false;
    for (i = 1; s[i] != '\0'; i++)
    {
        if (i >= 48)
            return false;
        if (!((s[i] >= 'a' && s[i] <= 'z') || (s[i] >= '0' && s[i] <= '9') ||
              s[i] == '_'))
            return false;
    }
    return true;
}

/* See description in ta_nosql_int.h */
void
ta_nosql_scrub(char *buf, const char *secret)
{
    size_t slen;
    char  *p;

    if (buf == NULL || secret == NULL || *secret == '\0')
        return;
    slen = strlen(secret);
    while ((p = strstr(buf, secret)) != NULL)
    {
        memmove(p + 3, p + slen, strlen(p + slen) + 1);
        memcpy(p, "***", 3);
    }
}

static char *
dup_or_null(const char *s)
{
    return s == NULL ? NULL : strdup(s);
}

/* See description in ta_nosql_int.h */
int
ta_nosql_conf_copy(const ta_nosql_conf *src, ta_nosql_conf *dst)
{
    *dst = *src;
    dst->host = dup_or_null(src->host);
    dst->user = dup_or_null(src->user);
    dst->password = dup_or_null(src->password);
    dst->database = dup_or_null(src->database);
    dst->tls_ca = dup_or_null(src->tls_ca);
    dst->raw = src->raw != NULL ? json_deep_copy(src->raw) : NULL;
    return 0;
}

/* See description in ta_nosql_int.h */
int
ta_nosql_conf_parse(json_t *raw, ta_nosql_conf *cfg, ta_nosql_err *err)
{
    json_t     *tls;
    long long   port;
    const char *user;
    const char *password;

    memset(cfg, 0, sizeof(*cfg));
    if (!json_is_object(raw))
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "the configuration is not a JSON object");
        return -1;
    }
    port = ta_nosql_json_int(raw, "port", 0);
    if (port < 0 || port > 65535)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0, "bad port %lld",
                         port);
        return -1;
    }
    cfg->host = strdup(ta_nosql_json_str(raw, "host", "127.0.0.1"));
    cfg->port = (int)port;
    user = ta_nosql_json_str(raw, "user", NULL);
    password = ta_nosql_json_str(raw, "password", NULL);
    cfg->user = user != NULL && *user != '\0' ? strdup(user) : NULL;
    cfg->password = password != NULL ? strdup(password) : NULL;
    cfg->database = dup_or_null(ta_nosql_json_str(raw, "database", NULL));
    cfg->timeout_ms = (int)ta_nosql_json_int(raw, "timeout_ms", 5000);
    cfg->connect_timeout_ms = (int)ta_nosql_json_int(raw,
                                                    "connect_timeout_ms",
                                                    3000);
    if (cfg->timeout_ms <= 0)
        cfg->timeout_ms = 5000;
    if (cfg->connect_timeout_ms <= 0)
        cfg->connect_timeout_ms = 3000;
    tls = json_object_get(raw, "tls");
    if (json_is_boolean(tls))
    {
        cfg->tls = json_is_true(tls);
        cfg->tls_verify = true;
    }
    else if (json_is_object(tls))
    {
        cfg->tls = ta_nosql_json_bool(tls, "enabled", true);
        cfg->tls_verify = ta_nosql_json_bool(tls, "verify", true);
        cfg->tls_ca = dup_or_null(ta_nosql_json_str(tls, "ca", NULL));
    }
    cfg->raw = json_deep_copy(raw);
    if (cfg->host == NULL || cfg->raw == NULL)
    {
        ta_nosql_conf_free(cfg);
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM,
                         "out of memory");
        return -1;
    }
    return 0;
}

/* See description in ta_nosql_int.h */
void
ta_nosql_conf_free(ta_nosql_conf *cfg)
{
    free(cfg->host);
    free(cfg->user);
    if (cfg->password != NULL)
    {
        /* Do not leave it in freed memory for the next allocation. */
        memset(cfg->password, 0, strlen(cfg->password));
        free(cfg->password);
    }
    free(cfg->database);
    free(cfg->tls_ca);
    json_decref(cfg->raw);
    memset(cfg, 0, sizeof(*cfg));
}

/* See description in ta_nosql_int.h */
void
ta_nosql_finding(json_t *findings, const char *check, int severity,
                 const char *subject, const char *fmt, ...)
{
    char    detail[512];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(detail, sizeof(detail), fmt, ap);
    va_end(ap);
    json_array_append_new(findings,
                          json_pack("{s:s, s:i, s:s, s:s}",
                                    "check", check, "severity", severity,
                                    "subject", subject, "detail", detail));
}
