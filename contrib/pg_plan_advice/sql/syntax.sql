LOAD 'pg_plan_advice';

-- An empty string is allowed, and so is an empty target list.
SET pg_plan_advice.advice = '';
SET pg_plan_advice.advice = 'SEQ_SCAN()';

-- Test assorted variations in capitalization, whitespace, and which parts of
-- the relation identifier are included. These should all work.
SET pg_plan_advice.advice = 'SEQ_SCAN(x)';
SET pg_plan_advice.advice = 'seq_scan(x@y)';
SET pg_plan_advice.advice = 'SEQ_scan(x#2)';
SET pg_plan_advice.advice = 'SEQ_SCAN (x/y)';
SET pg_plan_advice.advice = '  SEQ_SCAN ( x / y . z )  ';
SET pg_plan_advice.advice = 'SEQ_SCAN("x"#2/"y"."z"@"t")';

-- Syntax errors.
SET pg_plan_advice.advice = 'SEQUENTIAL_SCAN(x)';
SET pg_plan_advice.advice = 'SEQ_SCAN';
SET pg_plan_advice.advice = 'SEQ_SCAN(';
SET pg_plan_advice.advice = 'SEQ_SCAN("';
SET pg_plan_advice.advice = 'SEQ_SCAN(#';
SET pg_plan_advice.advice = '()';
SET pg_plan_advice.advice = '123';

-- Legal comments.
SET pg_plan_advice.advice = '/**/';
SET pg_plan_advice.advice = 'HASH_JOIN(_)/***/';
SET pg_plan_advice.advice = '/* comment */ HASH_JOIN(/*x*/y)';
SET pg_plan_advice.advice = '/* comment */ HASH_JOIN(y//*x*/z)';

-- Unterminated comments.
SET pg_plan_advice.advice = '/*';
SET pg_plan_advice.advice = 'JOIN_ORDER("fOO") /* oops';

-- Nested comments are not supported, so the first of these is legal and
-- the second is not.
SET pg_plan_advice.advice = '/*/*/';
SET pg_plan_advice.advice = '/*/* stuff */*/';

-- Foreign join requires multiple relation identifiers.
SET pg_plan_advice.advice = 'FOREIGN_JOIN(a)';
SET pg_plan_advice.advice = 'FOREIGN_JOIN((a))';
