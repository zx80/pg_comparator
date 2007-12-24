-- test bit and varbit to bytea

-- 80
SELECT 
  (VARBIT '1')::BYTEA, 
  (BIT '1')::BYTEA, 
  (VARBIT '1000')::BYTEA, 
  (BIT '1000')::BYTEA,
  (VARBIT '10000000')::BYTEA,
  (BIT '10000000')::BYTEA;

SELECT 
  (VARBIT '0100001101000001010011')::BYTEA AS "CAL",
  (VARBIT '0101011')::BYTEA AS "V",
  (VARBIT '010010010100111')::BYTEA AS "IN";

-- test bytea to bit and varbit
SELECT (BYTEA 'hello')::VARBIT AS "hello";
SELECT (BYTEA 'he')::VARBIT;
SELECT (BYTEA 'he')::BIT(4) AS "restricted bit(4)";
SELECT (BYTEA 'he')::BIT(12) AS "restricted bit(12)";
SELECT (BYTEA 'he')::BIT(16) AS "bit(16)";
SELECT (BYTEA 'he')::BIT(20) AS "expanded bit(20)";
SELECT (BYTEA 'he')::BIT(33) AS "expanded bit(33)";

-- bitwise operations
SELECT (BYTEA 'hello')::VARBIT & (BYTEA 'world')::VARBIT AS "hello & world";
SELECT (BYTEA 'hello')::BIT(40) & (BYTEA 'foo')::BIT(40) AS "hello & foo";
SELECT 
  DECODE(MD5('hello'),'hex')::VARBIT # DECODE(MD5('world'),'hex')::VARBIT
  AS "md5('hello') xor md5('world')";

-- should be found with implicit?
-- SELECT BYTEA 'hello' & BYTEA 'world';

-- no
-- SELECT (BYTEA 'he')::VARBIT(12) AS "varbit(12)";
