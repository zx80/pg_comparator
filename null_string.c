/* $Id: null_string.c 144 2004-08-24 06:47:21Z coelho $ */

#include "postgres.h"
#include "executor/spi.h"

Datum null_string(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(null_string);

/* null_string(string, string_replacement_if_null) */
Datum null_string(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		/* use provided replacement value. */
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();
		else
			PG_RETURN_TEXT_P(PG_GETARG_TEXT_P(1));
	else
		/* use initial value. */
		PG_RETURN_TEXT_P(PG_GETARG_TEXT_P(0));
}
