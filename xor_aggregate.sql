-- $Id: xor_aggregate.sql 163 2004-08-25 08:20:27Z coelho $

-- default behavior for strict functions used: NULLs are ignored...

CREATE AGGREGATE XOR(
  BASETYPE = BIT,
  SFUNC = bitxor,
  STYPE = BIT
);

CREATE AGGREGATE XOR(
  BASETYPE = INT2,
  SFUNC = int2xor,
  STYPE = INT2
);

CREATE AGGREGATE XOR(
  BASETYPE = INT4,
  SFUNC = int4xor,
  STYPE = INT4
);

CREATE AGGREGATE XOR(
  BASETYPE = INT8,
  SFUNC = int8xor,
  STYPE = INT8
);
