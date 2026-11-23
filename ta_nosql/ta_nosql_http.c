/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql HTTP: the libcurl client of the HTTP engines
 *
 * Elasticsearch, Neo4j and etcd (its v3 JSON gateway) all speak JSON
 * over HTTP; this is the one client they share. A connection is a CURL
 * easy handle kept open, so requests reuse the TCP (and TLS) session.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <curl/curl.h>

#include "ta_nosql_int.h"

typedef struct buf {
    char  *p;
    size_t n;
    size_t cap;
} buf;

static size_t
on_body(char *data, size_t size, size_t nmemb, void *arg)
{
    buf   *b = arg;
    size_t len = size * nmemb;

    if (b->n + len + 1 > b->cap)
    {
        size_t cap = (b->cap == 0 ? 4096 : b->cap);
        char  *p;

        while (cap < b->n + len + 1)
            cap *= 2;
        if (cap > 256u * 1024 * 1024)
            return 0;
        p = realloc(b->p, cap);
        if (p == NULL)
            return 0;
        b->p = p;
        b->cap = cap;
    }
    memcpy(b->p + b->n, data, len);
    b->n += len;
    b->p[b->n] = '\0';
    return len;
}

static void
global_init_once(void)
{
    curl_global_init(CURL_GLOBAL_ALL);
}

/* curl_global_init() is not thread-safe; load threads connect at once. */
static void
global_init(void)
{
    static pthread_once_t once = PTHREAD_ONCE_INIT;

    pthread_once(&once, global_init_once);
}

/* See description in ta_nosql_int.h */
const char *
ta_nosql_http_client(void)
{
    return curl_version_info(CURLVERSION_NOW)->version;
}

/* See description in ta_nosql_int.h */
int
ta_nosql_http_init(ta_nosql_http *h, const ta_nosql_conf *cfg,
                   int default_port, ta_nosql_err *err)
{
    global_init();
    memset(h, 0, sizeof(*h));
    h->curl = curl_easy_init();
    if (h->curl == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, 0,
                         "cannot create a libcurl handle");
        return -1;
    }
    h->tls = cfg->tls;
    h->base = ta_nosql_strdupf("%s://%s%s%s:%d", cfg->tls ? "https" : "http",
                               strchr(cfg->host, ':') != NULL ? "[" : "",
                               cfg->host,
                               strchr(cfg->host, ':') != NULL ? "]" : "",
                               cfg->port != 0 ? cfg->port : default_port);
    h->user = cfg->user != NULL ? strdup(cfg->user) : NULL;
    h->password = cfg->password != NULL ? strdup(cfg->password) : NULL;
    h->ca = cfg->tls_ca != NULL ? strdup(cfg->tls_ca) : NULL;
    h->verify = cfg->tls_verify;
    h->timeout_ms = cfg->timeout_ms;
    h->connect_timeout_ms = cfg->connect_timeout_ms;
    return 0;
}

/* See description in ta_nosql_int.h */
void
ta_nosql_http_fini(ta_nosql_http *h)
{
    if (h->curl != NULL)
        curl_easy_cleanup(h->curl);
    free(h->base);
    free(h->user);
    if (h->password != NULL)
    {
        memset(h->password, 0, strlen(h->password));
        free(h->password);
    }
    free(h->bearer);
    free(h->ca);
    memset(h, 0, sizeof(*h));
}

static ta_nosql_errcls
curl_cls(CURLcode c)
{
    switch (c)
    {
        case CURLE_OPERATION_TIMEDOUT:
            return TA_NOSQL_ERR_TIMEOUT;
        case CURLE_COULDNT_RESOLVE_HOST:
        case CURLE_COULDNT_CONNECT:
        case CURLE_SEND_ERROR:
        case CURLE_RECV_ERROR:
            return TA_NOSQL_ERR_CONNECT;
        case CURLE_SSL_CONNECT_ERROR:
        case CURLE_PEER_FAILED_VERIFICATION:
        case CURLE_SSL_CERTPROBLEM:
        case CURLE_SSL_CIPHER:
        case CURLE_SSL_CACERT_BADFILE:
        case CURLE_USE_SSL_FAILED:
            return TA_NOSQL_ERR_TLS;
        case CURLE_GOT_NOTHING:
        case CURLE_WEIRD_SERVER_REPLY:
            return TA_NOSQL_ERR_PROTOCOL;
        default:
            return TA_NOSQL_ERR_CONNECT;
    }
}

/* See description in ta_nosql_int.h */
ta_nosql_errcls
ta_nosql_http_cls(long status)
{
    if (status == 401)
        return TA_NOSQL_ERR_AUTH;
    if (status == 403)
        return TA_NOSQL_ERR_PERMISSION;
    if (status == 408 || status == 504)
        return TA_NOSQL_ERR_TIMEOUT;
    if (status == 413 || status == 429)
        return TA_NOSQL_ERR_LIMIT;
    if (status == 503 || status == 502)
        return TA_NOSQL_ERR_UNAVAILABLE;
    if (status >= 400 && status < 500)
        return TA_NOSQL_ERR_INVALID;
    return TA_NOSQL_ERR_PROTOCOL;
}

/* See description in ta_nosql_int.h */
char *
ta_nosql_http_escape(const char *s)
{
    static const char hex[] = "0123456789ABCDEF";
    size_t            n = strlen(s);
    char             *out = malloc(3 * n + 1);
    size_t            o = 0;
    size_t            i;

    if (out == NULL)
        return NULL;
    for (i = 0; i < n; i++)
    {
        unsigned char c = (unsigned char)s[i];

        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' ||
            c == '~')
        {
            out[o++] = (char)c;
        }
        else
        {
            out[o++] = '%';
            out[o++] = hex[c >> 4];
            out[o++] = hex[c & 15];
        }
    }
    out[o] = '\0';
    return out;
}

/* See description in ta_nosql_int.h */
int
ta_nosql_http_req(ta_nosql_http *h, const char *method, const char *path,
                  const json_t *body, const char *content_type, long *status,
                  json_t **reply, char **raw, ta_nosql_err *err)
{
    char *text = body != NULL ? ta_nosql_json_dump(body) : NULL;
    int   rc;

    if (body != NULL && text == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, 0,
                         "cannot encode the request");
        return -1;
    }
    rc = ta_nosql_http_req_text(h, method, path, text, content_type, status,
                                reply, raw, err);
    free(text);
    return rc;
}

/* See description in ta_nosql_int.h */
int
ta_nosql_http_req_text(ta_nosql_http *h, const char *method, const char *path,
                       const char *body, const char *content_type,
                       long *status, json_t **reply, char **raw,
                       ta_nosql_err *err)
{
    struct curl_slist *hdrs = NULL;
    char              *url;
    char               ebuf[CURL_ERROR_SIZE] = "";
    char               ctype[128];
    buf                b = { NULL, 0, 0 };
    CURLcode           c;
    CURL              *curl = h->curl;

    if (reply != NULL)
        *reply = NULL;
    if (raw != NULL)
        *raw = NULL;
    *status = 0;
    url = ta_nosql_strdupf("%s%s", h->base, path);
    if (url == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, 0, "out of memory");
        return -1;
    }
    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)h->timeout_ms);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS,
                     (long)h->connect_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, ebuf);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, on_body);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &b);
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "http,https");
    if (h->tls)
    {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, h->verify ? 1L : 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, h->verify ? 2L : 0L);
        if (h->ca != NULL)
            curl_easy_setopt(curl, CURLOPT_CAINFO, h->ca);
    }
    if (h->user != NULL)
    {
        curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
        curl_easy_setopt(curl, CURLOPT_USERNAME, h->user);
        curl_easy_setopt(curl, CURLOPT_PASSWORD,
                         h->password != NULL ? h->password : "");
    }
    if (h->bearer != NULL)
    {
        char *line = ta_nosql_strdupf("Authorization: %s", h->bearer);

        hdrs = curl_slist_append(hdrs, line);
        free(line);
    }
    hdrs = curl_slist_append(hdrs, "Accept: application/json");
    if (body != NULL)
    {
        snprintf(ctype, sizeof(ctype), "Content-Type: %s",
                 content_type != NULL ? content_type : "application/json");
        hdrs = curl_slist_append(hdrs, ctype);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)strlen(body));
    }
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);

    c = curl_easy_perform(curl);
    curl_slist_free_all(hdrs);
    free(url);
    if (c != CURLE_OK)
    {
        ta_nosql_err_set(err, curl_cls(c), (long)c, "%s: %s",
                         curl_easy_strerror(c), ebuf);
        free(b.p);
        return -1;
    }
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, status);
    if (reply != NULL && b.n > 0)
    {
        json_error_t jerr;

        *reply = json_loadb(b.p, b.n, JSON_DECODE_ANY, &jerr);
    }
    if (raw != NULL)
    {
        *raw = b.p;
        b.p = NULL;
    }
    free(b.p);
    return 0;
}
