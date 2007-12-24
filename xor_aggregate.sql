-- $Id: xor_aggregate.sql 247 2005-03-13 09:31:52Z coelho $

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
