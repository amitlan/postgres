

setup
{
 CREATE TABLE pk (a int PRIMARY KEY);
 CREATE TABLE fk (a int REFERENCES pk ON DELETE CASCADE ON UPDATE CASCADE);
 INSERT INTO pk VALUES (1);
}

teardown
{
 DROP TABLE fk, pk;
}

session "s1"

setup	{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "s1spk" { TABLE fk; }
step "s1dpk" { DELETE FROM pk; }
step "s1upk" { UPDATE pk SET a = a + 1; }
step "s1c"	{ COMMIT; }


session "s2"

step "s2ifk" { INSERT INTO fk VALUES (1); }

permutation "s1spk" "s2ifk" "s1dpk" "s1c"
permutation "s1spk" "s2ifk" "s1upk" "s1c"
