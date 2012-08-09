/* $Id: mysql_casts.c 1127 2012-08-08 07:49:29Z fabien $ */

// minimal headers
#include <stdint.h>
#include <my_global.h>
#include <mysql.h>

/* foo_init, foo, foo_deinit */
my_bool biginttoint8_init(UDF_INIT *, UDF_ARGS *, char *);
longlong biginttoint8(UDF_INIT *, UDF_ARGS *, char *, char *);
my_bool biginttoint4_init(UDF_INIT *, UDF_ARGS *, char *);
longlong biginttoint4(UDF_INIT *, UDF_ARGS *, char *, char *);
my_bool biginttoint2_init(UDF_INIT *, UDF_ARGS *, char *);
longlong biginttoint2(UDF_INIT *, UDF_ARGS *, char *, char *);

longlong biginttoint8(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args,
  char *is_null,
  char *error __attribute__((unused)))
{
  // if in doubt, return NULL
  if (args->arg_count!=1 || args->arg_type[0]!=INT_RESULT || !args->args[0]) {
    *is_null = 1;
    return 0;
  }
  return *((longlong*) args->args[0]);
}

my_bool biginttoint8_init(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args __attribute__((unused)),
  char *message __attribute__((unused)))
{
  return 0;
}

longlong biginttoint4(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args,
  char *is_null,
  char *error __attribute__((unused)))
{
  // if in doubt, return NULL
  if (args->arg_count!=1 || args->arg_type[0]!=INT_RESULT || !args->args[0]) {
    *is_null = 1;
    return 0;
  }
  return (longlong)
    ((int32_t) (*((longlong*) args->args[0]) & 0x00000000ffffffffLL));
}

my_bool biginttoint4_init(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args __attribute__((unused)),
  char *message __attribute__((unused)))
{
  return 0;
}

longlong biginttoint2(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args,
  char *is_null,
  char *error __attribute__((unused)))
{
  // if in doubt, return NULL
  if (args->arg_count!=1 || args->arg_type[0]!=INT_RESULT || !args->args[0]) {
    *is_null = 1;
    return 0;
  }
  return (longlong)
    ((int16_t)(*((longlong*) args->args[0]) & 0x000000000000ffffLL));
}

my_bool biginttoint2_init(
  UDF_INIT *initid __attribute__((unused)),
  UDF_ARGS *args __attribute__((unused)),
  char *message __attribute__((unused)))
{
  return 0;
}
