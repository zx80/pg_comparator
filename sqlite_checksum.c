/* $Id: sqlite_checksum.c 1460 2012-11-02 18:21:27Z fabien $ */
/*
 * SQLite extensions for pg_comparator.
 *
 * provide checksum functions: cksum2, cksum4 and cksum8.
 * provide integer aggregates: xor and isum.
 */

#include <stdio.h>
#include <assert.h>

#define COMPILE_SQLITE_EXTENSIONS_AS_LOADABLE_MODULE 1

#ifdef COMPILE_SQLITE_EXTENSIONS_AS_LOADABLE_MODULE
// compile as loadable module
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#else // not loadable
#include <sqlite3.h>
#endif // COMPILE_SQLITE_EXTENSIONS_AS_LOADABLE_MODULE

// plain C implementations
#include "jenkins.c"

/******************************************************* CHECKSUMS FUNCTIONS */

static void sqlite_checksum_int2(
  sqlite3_context * ctx,
  int argc,
  sqlite3_value ** argv)
{
  assert(argc==1);
  const unsigned char * txt;
  size_t len;
  switch (sqlite3_value_type(argv[0])) {
  case SQLITE_NULL:
    txt = NULL;
    len = 0;
    break;
  case SQLITE_TEXT:
    txt = sqlite3_value_text(argv[0]);
    len = sqlite3_value_bytes(argv[0]);
    break;
    // hmmm... should I do something else?
  case SQLITE_INTEGER:
  case SQLITE_FLOAT:
  case SQLITE_BLOB:
  default:
    sqlite3_result_error(ctx, "expecting TEXT or NULL", -1);
    return;
  }
  sqlite3_result_int(ctx, checksum_int2(txt, len));
}

static void sqlite_checksum_int4(
  sqlite3_context * ctx,
  int argc,
  sqlite3_value ** argv)
{
  assert(argc==1);
  const unsigned char * txt;
  size_t len;
  switch (sqlite3_value_type(argv[0])) {
  case SQLITE_NULL:
    txt = NULL;
    len = 0;
    break;
  case SQLITE_TEXT:
    txt = sqlite3_value_text(argv[0]);
    len = sqlite3_value_bytes(argv[0]);
    break;
    // hmmm... should I do something else?
  case SQLITE_INTEGER:
  case SQLITE_FLOAT:
  case SQLITE_BLOB:
  default:
    sqlite3_result_error(ctx, "expecting TEXT or NULL", -1);
    return;
  }
  sqlite3_result_int(ctx, checksum_int4(txt, len));
}

static void sqlite_checksum_int8(
  sqlite3_context * ctx,
  int argc,
  sqlite3_value ** argv)
{
  assert(argc==1);
  const unsigned char * txt;
  size_t len;
  switch (sqlite3_value_type(argv[0])) {
  case SQLITE_NULL:
    txt = NULL;
    len = 0;
    break;
  case SQLITE_TEXT:
    txt = sqlite3_value_text(argv[0]);
    len = sqlite3_value_bytes(argv[0]);
    break;
    // hmmm... should I do something else?
  case SQLITE_INTEGER:
  case SQLITE_FLOAT:
  case SQLITE_BLOB:
  default:
    sqlite3_result_error(ctx, "expecting TEXT or NULL", -1);
    return;
  }
  sqlite3_result_int64(ctx, checksum_int8(txt, len));
}

/***************************************************** INTEGER XOR AGGREGATE */

static void ixor_step(
  sqlite3_context * ctx,
  int argc,
  sqlite3_value ** argv)
{
  assert(argc==1);
  int64_t * val = sqlite3_aggregate_context(ctx, sizeof(int64_t));
  if (sqlite3_value_type(argv[0])==SQLITE_INTEGER)
    *val ^= sqlite3_value_int64(argv[0]);
  // else just ignore...
}

// integer SUM without fancy overflow handling
static void isum_step(
  sqlite3_context * ctx,
  int argc,
  sqlite3_value ** argv)
{
  assert(argc==1);
  int64_t * val = sqlite3_aggregate_context(ctx, sizeof(int64_t));
  if (sqlite3_value_type(argv[0])==SQLITE_INTEGER)
    *val += sqlite3_value_int64(argv[0]);
  // else just ignore...
}

static void int_finalize(sqlite3_context * ctx)
{
  int64_t * val = sqlite3_aggregate_context(ctx, 0);
  sqlite3_result_int64(ctx, val? *val: 0);
}

/***************************************************************** AUTO LOAD */

#ifdef COMPILE_SQLITE_EXTENSIONS_AS_LOADABLE_MODULE
// autoload checksum functions
int sqlite3_extension_init(
  sqlite3 * db,
  char ** pzErrMsg,
  const sqlite3_api_routines *pApi)
{
  SQLITE_EXTENSION_INIT2(pApi);

  sqlite3_create_function(db,
			  // name, #arg, txt, data,
			  "cksum2", 1, SQLITE_UTF8, NULL,
			  // func, step, final
			  sqlite_checksum_int2, NULL, NULL);

  sqlite3_create_function(db,
			  // name, #arg, txt, data,
			  "cksum4", 1, SQLITE_UTF8, NULL,
			  // func, step, final
			  sqlite_checksum_int4, NULL, NULL);

  sqlite3_create_function(db,
			  // name, #arg, txt, data,
			  "cksum8", 1, SQLITE_UTF8, NULL,
			  // func, step, final
			  sqlite_checksum_int8, NULL, NULL);

  sqlite3_create_function(db,
        // name, #args, txt, data,
        "xor", 1, SQLITE_UTF8, NULL,
        // func, step, final
        NULL, ixor_step, int_finalize);

  sqlite3_create_function(db,
        // name, #args, txt, data,
        "isum", 1, SQLITE_UTF8, NULL,
        // func, step, final
        NULL, isum_step, int_finalize);

 return 0;
}
#endif // COMPILE_SQLITE_EXTENSIONS_AS_LOADABLE_MODULE
