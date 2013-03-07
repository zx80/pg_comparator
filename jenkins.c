/* $Id: jenkins.c 1462 2012-11-03 07:17:10Z fabien $ */

#include <stdint.h>

#define PN_16	15401
#define PN_32_1 433494437
#define PN_32_2 780291637
#define PN_32_3 1073676287
#define PN_32_4 1873012681

/* The following function is taken and adapted (wrt len) from
 * http://www.burtleburtle.net/bob/hash/doobs.html,
 * and is advertised public domain.
 * if hash==0, it is unchanged for the empty string.
 */
static uint32_t jenkins_one_at_a_time_hash
  (uint32_t hash, const unsigned char *key, size_t len)
{
  size_t i;
  for (i = 0; i < len; i++) {
    hash += key[i] ^ len;
    hash += (hash << 10);
    hash ^= (hash >> 6);
  }
  hash += (hash << 3);
  hash ^= (hash >> 11) + len;
  hash += (hash << 15);
  return hash;
}

/* checksum of sizes 2, 4 and 8.
 * checksum_int?(NULL) == some_predefined_value
 * checksum_int?('') == 0
 */
static int16_t checksum_int2(const unsigned char *data, size_t size)
{
  uint32_t h = PN_16; // default if NULL
  if (data) h = jenkins_one_at_a_time_hash(0, data, size);
  return (int16_t) ((h>>16)^h);
}

// many collision, eg cksum4('16667') = cksum4('53827')
static int32_t checksum_int4(const unsigned char *data, size_t size)
{
  uint32_t h = PN_32_1; // default if NULL
  if (data) h = jenkins_one_at_a_time_hash(0, data, size);
  return (int32_t) h;
}

static int64_t checksum_int8(const unsigned char *data, size_t size)
{
  uint64_t h1 = PN_32_2, h2 = PN_32_3; // default if NULL
  if (data) {
    // the 64 bit hash is based on two hashes. first one is chsum4
    h1 = jenkins_one_at_a_time_hash(0, data, size);
    // ensure that size==0 => checksum==0
    h2 = size? jenkins_one_at_a_time_hash(h1 ^ PN_32_4, data, size): 0;
  }
  return (int64_t) ((h1<<32)|h2);
}
