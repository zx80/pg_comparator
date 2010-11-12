/* $Id: pgc_checksum.c 1022 2010-08-06 07:28:07Z fabien $
 *
 * This function computes a simple and fast checksum of a text.
 * It is unclear to me what happends on different encodings.
 * NOT CRYPTOGRAPHICALLY SECURE.
 */

#include "postgres.h"
#include "executor/spi.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern Datum text_checksum2(PG_FUNCTION_ARGS);
extern Datum text_checksum4(PG_FUNCTION_ARGS);
extern Datum text_checksum8(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(text_checksum2);
PG_FUNCTION_INFO_V1(text_checksum4);
PG_FUNCTION_INFO_V1(text_checksum8);

#include "jenkins.c"

Datum text_checksum2(PG_FUNCTION_ARGS)
{
  unsigned char * data;
  size_t size;
  if (PG_ARGISNULL(0))
  {
    data = NULL, size = 0;
  }
  else
  {
    text *t = PG_GETARG_TEXT_P(0);
    size = VARSIZE(t) - VARHDRSZ;
    data = (unsigned char *) VARDATA(t);
  }
  PG_RETURN_INT16(checksum_int2(data, size));
}

Datum text_checksum4(PG_FUNCTION_ARGS)
{
  unsigned char * data;
  size_t size;
  if (PG_ARGISNULL(0))
  {
    data = NULL, size = 0;
  }
  else
  {
    text *t = PG_GETARG_TEXT_P(0);
    size = VARSIZE(t) - VARHDRSZ;
    data = (unsigned char *) VARDATA(t);
  }
  PG_RETURN_INT32(checksum_int4(data, size));
}

Datum text_checksum8(PG_FUNCTION_ARGS)
{
  unsigned char * data;
  size_t size;
  if (PG_ARGISNULL(0))
  {
    data = NULL, size = 0;
  }
  else
  {
    text * t = PG_GETARG_TEXT_P(0);
    data = (unsigned char *) VARDATA(t);
    size = VARSIZE(t) - VARHDRSZ;
  }
  PG_RETURN_INT64(checksum_int8(data, size));
}
