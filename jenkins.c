/* $Id: jenkins.c 901 2010-07-27 10:11:24Z fabien $ */

#include <stdint.h>

#define PN_16	15401
#define PN_32_1 433494437
#define PN_32_2 780291637
#define PN_32_3 1073676287
#define PN_32_4 1873012681

/* The following function is taken and adapted (wrt len) from
 * http://www.burtleburtle.net/bob/hash/doobs.html,
 * and is advertised public domain.
 */
static uint32_t jenkins_one_at_a_time_hash
  (uint32_t hash, unsigned char *key, size_t len)
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
static int16_t checksum_int2(unsigned char *data, size_t size)
{
  uint32_t h = PN_16;
  if (data) h = jenkins_one_at_a_time_hash(0, data, size);
  return (int16_t) ((h>>16)^h);
}

// many collision, eg cksum4('16667') = cksum4('53827') 
static int32_t checksum_int4(unsigned char *data, size_t size)
{
  uint32_t h = PN_32_1;
  if (data) h = jenkins_one_at_a_time_hash(0, data, size);
  return (int32_t) h;
}

static int64_t checksum_int8(unsigned char *data, size_t size)
{
  uint64_t h1 = PN_32_2, h2 = PN_32_3;
  if (data) {
    h1 = jenkins_one_at_a_time_hash(0, data, size);
    // ensure that size==0 => checksum==0
    h2 = size? jenkins_one_at_a_time_hash(PN_32_4, data, size): 0;
  }
  return (int64_t) ((h1<<32)|h2);
}
