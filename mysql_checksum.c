/* $Id: mysql_checksum.c 1525 2014-08-03 12:34:17Z coelho $
 *
 * This function computes a simple and fast checksum of a binary
 * It is unclear to me what happends on different encodings.
 * NOT CRYPTOGRAPHICALLY SECURE.
 */

// minimal headers
#include <my_global.h>
#include <mysql.h>

/* Jenkins-based hash functions
 */
/* foo_init, foo, foo_deinit */
my_bool cksum8_init(UDF_INIT *, UDF_ARGS *, char *);
longlong cksum8(UDF_INIT *, UDF_ARGS *, char *, char *);
my_bool cksum4_init(UDF_INIT *, UDF_ARGS *, char *);
longlong cksum4(UDF_INIT *, UDF_ARGS *, char *, char *);
my_bool cksum2_init(UDF_INIT *, UDF_ARGS *, char *);
longlong cksum2(UDF_INIT *, UDF_ARGS *, char *, char *);

#include "jenkins.c"

longlong cksum2(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args,
  char *is_null,
  char *error __attribute__((unused)))
{
  // if in doubt, return NULL
  if (args->arg_count!=1 || args->arg_type[0]!=STRING_RESULT)
  {
    *is_null = 1;
    return 0;
  }
  return (longlong) checksum_int2(args->args[0], args->lengths[0]);
}

my_bool cksum2_init(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args __attribute__((unused)),
  char *message __attribute__((unused)))
{
  return 0;
}

longlong cksum4(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args,
  char *is_null __attribute__((unused)),
  char *error __attribute__((unused)))
{
  // if in doubt, return NULL
  if (args->arg_count!=1 || args->arg_type[0]!=STRING_RESULT)
  {
    *is_null = 1;
    return 0;
  }
  return (longlong) checksum_int4(args->args[0], args->lengths[0]);
}

my_bool cksum4_init(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args __attribute__((unused)),
  char *message __attribute__((unused)))
{
  return 0;
}

longlong cksum8(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args,
  char *is_null __attribute__((unused)),
  char *error __attribute__((unused)))
{
  // if in doubt, return NULL
  if (args->arg_count!=1 || args->arg_type[0]!=STRING_RESULT)
  {
    *is_null = 1;
    return 0;
  }
  return (longlong) checksum_int8(args->args[0], args->lengths[0]);
}

my_bool cksum8_init(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args __attribute__((unused)),
  char *message __attribute__((unused)))
{
  return 0;
}

/* FNV-based hash functions
 */
my_bool fnv8_init(UDF_INIT *, UDF_ARGS *, char *);
longlong fnv8(UDF_INIT *, UDF_ARGS *, char *, char *);
my_bool fnv4_init(UDF_INIT *, UDF_ARGS *, char *);
longlong fnv4(UDF_INIT *, UDF_ARGS *, char *, char *);
my_bool fnv2_init(UDF_INIT *, UDF_ARGS *, char *);
longlong fnv2(UDF_INIT *, UDF_ARGS *, char *, char *);

#include "fnv.c"

longlong fnv2(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args,
  char *is_null,
  char *error __attribute__((unused)))
{
  // if in doubt, return NULL
  if (args->arg_count!=1 || args->arg_type[0]!=STRING_RESULT)
  {
    *is_null = 1;
    return 0;
  }
  return (longlong) checksum_int2(args->args[0], args->lengths[0]);
}

my_bool fnv2_init(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args __attribute__((unused)),
  char *message __attribute__((unused)))
{
  return 0;
}

longlong fnv4(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args,
  char *is_null __attribute__((unused)),
  char *error __attribute__((unused)))
{
  // if in doubt, return NULL
  if (args->arg_count!=1 || args->arg_type[0]!=STRING_RESULT)
  {
    *is_null = 1;
    return 0;
  }
  return (longlong) checksum_int4(args->args[0], args->lengths[0]);
}

my_bool fnv4_init(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args __attribute__((unused)),
  char *message __attribute__((unused)))
{
  return 0;
}

longlong fnv8(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args,
  char *is_null __attribute__((unused)),
  char *error __attribute__((unused)))
{
  // if in doubt, return NULL
  if (args->arg_count!=1 || args->arg_type[0]!=STRING_RESULT)
  {
    *is_null = 1;
    return 0;
  }
  return (longlong) checksum_int8(args->args[0], args->lengths[0]);
}

my_bool fnv8_init(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args __attribute__((unused)),
  char *message __attribute__((unused)))
{
  return 0;
}
