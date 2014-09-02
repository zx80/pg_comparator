/*
 * $Id: fnv.c 1529 2014-08-04 07:09:38Z coelho $
 *
 * https://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
 * http://www.isthe.com/chongo/tech/comp/fnv/index.html
 *
 * Code adapted, simplified and slightly extended from public domain:
 *
 *   http://www.isthe.com/chongo/src/fnv/hash_64a.c
 *
 * By chongo <Landon Curt Noll>
 */

#include <stdint.h>

/* This prime is probably too small? It seems that it was chosen because it contains
 * few one bits, this allowing some optimisations on 32 bit processors which would
 * not have a hardware 64 bit multiply operation.
 */
#define FNV_64_PRIME (0x100000001b3ULL)
#define FNV1a_64_INIT (0xcbf29ce484222325ULL)

static uint64_t fnv1a_64_hash_data(const void * data, const size_t len, uint64_t hval)
{
  if (data) { // NOT NULL
    const unsigned char * bp = (unsigned char *) data;
    const unsigned char * be = bp + len;
    while (bp < be) {
      register uint64_t byte = (uint64_t) (*bp++);
#if defined(STANDARD_FNV1A_64)
      hval ^= byte;
#else
      // help tweak high bits
      hval += (byte << 11) | (byte << 31) | (byte << 53);
      hval ^= byte | (byte << 23) | (byte << 43);
#endif // STANDARD_FNV1A_64
      hval *= FNV_64_PRIME;
    }
    return hval;
  }
  else // NULL
    return 0ULL;
}

static uint64_t fnv1a_64_hash(const void * data, const size_t len)
{
  return fnv1a_64_hash_data(data, len, FNV1a_64_INIT);
}

/*
   SELECT
     (ABS(fnv8((i+1)::TEXT)) % 100) - (ABS(fnv8(i::TEXT)) % 100) AS diff,
     COUNT(*) AS nb
   FROM generate_series(1, 1000) as i
   GROUP BY diff
   ORDER BY diff;
*/

static int16_t fnv_int2(const void * data, const size_t len)
{
  uint64_t h = fnv1a_64_hash(data, len);
  return (int16_t) ((h >> 48) ^ (h >> 32) ^ (h >> 16) ^ h);
}

static int32_t fnv_int4(const void * data, const size_t len)
{
  uint64_t h = fnv1a_64_hash(data, len);
  return (int32_t) ((h >> 32) ^ h);
}

static int64_t fnv_int8(const void * data, const size_t len)
{
  return (int64_t) fnv1a_64_hash(data, len);
}
