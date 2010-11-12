/* $Id: pgc_casts.c 1022 2010-08-06 07:28:07Z fabien $
 *
 * additional cast functions.
 */

#include "postgres.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"
#include "utils/varbit.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern Datum varbitfrombytea(PG_FUNCTION_ARGS);
extern Datum varbittobytea(PG_FUNCTION_ARGS);
extern Datum varbittoint2(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(varbitfrombytea);
PG_FUNCTION_INFO_V1(varbittobytea);
PG_FUNCTION_INFO_V1(varbittoint2);

/* create a bit string from a byte array.
 */
Datum varbitfrombytea(PG_FUNCTION_ARGS)
{
  bytea	*arg = PG_GETARG_BYTEA_P(0);
  int32	typmod = PG_GETARG_INT32(1); /* for ::BIT(10) syntax */
  /* bool isExplicit = PG_GETARG_BOOL(2); */
  int	datalen = VARSIZE(arg) - VARHDRSZ;
  int	bitlen = BITS_PER_BYTE * datalen;
  int	len, resbitlen, resdatalen, needlen;
  VarBit *result;

  /* truncate or expand if required */
  if (typmod>=0)
  {
    resbitlen = typmod;
    resdatalen = (resbitlen + BITS_PER_BYTE - 1) / BITS_PER_BYTE;
    needlen = datalen>resdatalen? resdatalen: datalen;
  }
  else
  {
    resbitlen = bitlen;
    resdatalen = datalen;
    needlen = datalen;
  }

  len = VARBITTOTALLEN(resbitlen);
  result = (VarBit *) palloc(len);
  SET_VARSIZE(result, len);
  VARBITLEN(result) = resbitlen;
  memcpy(VARBITS(result), VARDATA(arg), needlen);
  if (resdatalen > needlen)
  {
    unsigned char *ptr = VARBITS(result) + needlen;
    while (needlen<resdatalen)
    {
      *ptr++ = '\000';
      needlen++;
    }
  }

  PG_RETURN_VARBIT_P(result);
}

Datum varbittobytea(PG_FUNCTION_ARGS)
{
  VarBit *arg = PG_GETARG_VARBIT_P(0);
  bool	isExplicit = PG_GETARG_BOOL(2);
  int	bitlen = VARBITLEN(arg);
  int	datalen = (bitlen + BITS_PER_BYTE - 1) / BITS_PER_BYTE;
  int	len = datalen + VARHDRSZ;
  bytea	*result;

  /* no implicit cast if data size is changed */
  if (!isExplicit && (bitlen != BITS_PER_BYTE*datalen))
    ereport(ERROR,
	    (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
	     errmsg("bit length %d would be round up, use explicit cast",
		    bitlen)));

  result = (bytea *) palloc(len);
  SET_VARSIZE(result, len);
  memcpy(VARDATA(result), VARBITS(arg), datalen);

  PG_RETURN_BYTEA_P(result);
}

// hmmm... I'm quite unsure about bit order and so...
Datum varbittoint2(PG_FUNCTION_ARGS)
{
  VarBit *arg = PG_GETARG_VARBIT_P(0);
  bool	isExplicit = PG_GETARG_BOOL(2);
  int	bitlen = VARBITLEN(arg);
  int16	result = 0;

  /* no implicit cast if data size is changed */
  if (!isExplicit &&  (bitlen != BITS_PER_BYTE*2))
    ereport(ERROR,
	    (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
	     errmsg("bit length %d would be round up, use explicit cast",
		    bitlen)));

  memcpy(&result, VARBITS(arg), 2);

  PG_RETURN_INT16(result);
}
