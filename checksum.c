/* $Id: checksum.c 143 2004-08-23 13:26:01Z coelho $
 *
 * This function computes a simple and fast checksum of a text.
 * NOT CRYPTOGRAPHICALLY SECURE.
 */

#include "postgres.h"
#include "executor/spi.h"

#define ROTATE(x,n) (((x)<<(n))|((x)>>(32-(n))))

extern Datum text_checksum2(PG_FUNCTION_ARGS);
extern Datum text_checksum4(PG_FUNCTION_ARGS);
extern Datum text_checksum8(PG_FUNCTION_ARGS);

static uint32 bij[] = { 
	195, 202, 56, 230, 40, 57, 148, 222, 192, 92, 55, 76, 116, 204, 24, 90, 
	227, 15, 134, 245, 141, 10, 129, 177, 235, 197, 39, 188, 18, 214, 109, 108,
	87, 117, 73, 114, 168, 71, 216, 58, 74, 154, 130, 93, 34, 26, 221, 242, 41,
	46, 118, 207, 83, 110, 32, 6, 77, 44, 20, 172, 66, 219, 106, 145, 88, 149,
	1, 120, 194, 228, 136, 50, 155, 107, 94, 29, 211, 200, 70, 184, 16, 166,
	165, 43, 224, 132, 51, 98, 217, 64, 156, 91, 78, 201, 254, 67, 138, 89, 38,
	183, 232, 203, 96, 180, 210, 119, 60, 248, 250, 236, 208, 218, 178, 174,
	63, 11, 14, 13, 182, 82, 31, 143, 36, 212, 251, 61, 2, 68, 193, 84, 72,
	160, 229, 0, 190, 4, 80, 115, 237, 231, 186, 127, 133, 191, 142, 198, 147,
	233, 249, 137, 9, 135, 104, 179, 81, 7, 225, 246, 69, 37, 103, 33, 234,
	151, 163, 226, 175, 12, 152, 244, 52, 125, 62, 85, 124, 238, 53, 169, 139,
	153, 206, 220, 170, 213, 8, 239, 128, 187, 162, 255, 25, 247, 113, 252, 3,
	27, 100, 112, 241, 199, 22, 159, 49, 65, 173, 215, 140, 167, 158, 111, 105,
	240, 196, 161, 17, 99, 157, 23, 164, 121, 54, 45, 21, 181, 223, 150, 176,
	209, 185, 19, 42, 97, 30, 59, 102, 47, 205, 48, 101, 131, 28, 123, 189, 5,
	243, 35, 253, 122, 95, 146, 144, 171, 79, 126, 75, 86 
};

static uint32 hash(uint32 h, uint32 c, uint32 n)
{
	uint32 
		h1 = h^bij[(h+7*c+11*n) & 0xff],
		h2 = c^(3*h)^(5*c)^(13*n)^bij[c & 0xff],
		h3 = bij[(((h*(c+n)*(c-n))%104851)+h+c+n) & 0xff],
		h4 = n ^ bij[((h>>(n%17))+c) & 0xff];
	return ROTATE(h1, 24) ^ ROTATE(h2, 16) ^ ROTATE(h3, 8) ^ h4;
}

static uint64 checksum(char * data, uint32 size)
{
	uint32 cks1 = 0xfab1c0e1, cks2 = 0xca140be5;
	uint32 i;
	for (i=0; i<size; i++) {
		uint32 c = *(data+i);
		cks1 = ROTATE(cks1,7) ^ hash(cks1^cks2, c+i, i);
		cks2 = ROTATE(cks2,9) ^ hash(cks1+cks2, c^i, c);
	}
	return (((uint64)cks1)<<32)|((uint64)cks2);
}

PG_FUNCTION_INFO_V1(text_checksum2);
PG_FUNCTION_INFO_V1(text_checksum4);
PG_FUNCTION_INFO_V1(text_checksum8);

Datum text_checksum2(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		PG_RETURN_INT16(0);
	}
	else
	{
		text *t = PG_GETARG_TEXT_P(0);
		uint32 size = VARSIZE(t) - VARHDRSZ;
		char * data = VARDATA(t);
		uint64 cks = checksum(data, size);
		PG_RETURN_INT16((int16)(((cks>>48)^(cks>>32)^(cks>>16)^cks) & 0xffff));
	}
}

Datum text_checksum4(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		PG_RETURN_INT32(0);
	}
	else
	{
		text *t = PG_GETARG_TEXT_P(0);
		uint32 size = VARSIZE(t) - VARHDRSZ;
		char * data = VARDATA(t);
		uint64 cks = checksum(data, size);
		PG_RETURN_INT32((int32)(((cks>>32)^cks) & 0xffffffff));
	}
}

Datum text_checksum8(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		PG_RETURN_INT64(0);
	}
	else
	{
		text *t = PG_GETARG_TEXT_P(0);
		uint32 size = VARSIZE(t) - VARHDRSZ;
		char * data = VARDATA(t);
		uint64 cks = checksum(data, size);
		PG_RETURN_INT64(cks);
	}
}