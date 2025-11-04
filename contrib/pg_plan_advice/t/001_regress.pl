# Copyright (c) 2021-2025, PostgreSQL Global Development Group

# Run the core regression tests under pg_plan_advice to check for problems.
use strict;
use warnings FATAL => 'all';

use Cwd            qw(abs_path);
use File::Basename qw(dirname);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize the primary node
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init();

# Set up our desired configuration.
#
# We run with pg_plan_advice.shared_collection_limit set to ensure that the
# plan tree walker code runs against every query in the regression tests. If
# we're unable to properly analyze any of those plan trees, this test should fail.
#
# We set pg_plan_advice.advice to an advice string that will cause the advice
# trove to be populated with a few entries of various sorts, but which we do
# not expect to match anything in the regression test queries. This way, the
# planner hooks will be called, improving code coverage, but no plans should
# actually change.
#
# pg_plan_advice.always_explain_supplied_advice=false is needed to avoid breaking
# regression test queries that use EXPLAIN. In the real world, it seems like
# users will want EXPLAIN output to show supplied advice so that it's clear
# whether normal planner behavior has been altered, but here that's undesirable.
$node->append_conf('postgresql.conf', <<EOM);
pg_plan_advice.shared_collection_limit=1000000
shared_preload_libraries=pg_plan_advice
pg_plan_advice.advice='SEQ_SCAN(entirely_fictitious) HASH_JOIN(total_fabrication) GATHER(completely_imaginary)'
pg_plan_advice.always_explain_supplied_advice=false
EOM
$node->start;

my $srcdir = abs_path("../..");

# --dlpath is needed to be able to find the location of regress.so
# and any libraries the regression tests require.
my $dlpath = dirname($ENV{REGRESS_SHLIB});

# --outputdir points to the path where to place the output files.
my $outputdir = $PostgreSQL::Test::Utils::tmp_check;

# --inputdir points to the path of the input files.
my $inputdir = "$srcdir/src/test/regress";

# Run the tests.
my $rc =
  system($ENV{PG_REGRESS} . " "
	  . "--bindir= "
	  . "--dlpath=\"$dlpath\" "
	  . "--host=" . $node->host . " "
	  . "--port=" . $node->port . " "
	  . "--schedule=$srcdir/src/test/regress/parallel_schedule "
	  . "--max-concurrent-tests=20 "
	  . "--inputdir=\"$inputdir\" "
	  . "--outputdir=\"$outputdir\"");

# Dump out the regression diffs file, if there is one
if ($rc != 0)
{
	my $diffs = "$outputdir/regression.diffs";
	if (-e $diffs)
	{
		print "=== dumping $diffs ===\n";
		print slurp_file($diffs);
		print "=== EOF ===\n";
	}
}

# Report results
is($rc, 0, 'regression tests pass');

# Create the extension so we can access the collector
$node->safe_psql('postgres', 'CREATE EXTENSION pg_plan_advice');

# Verify that a large amount of advice was collected
my $all_query_count = $node->safe_psql('postgres', <<EOM);
SELECT COUNT(*) FROM pg_get_collected_shared_advice();
EOM
cmp_ok($all_query_count, '>', 35000, "copious advice collected");

# Verify that lots of different advice strings were collected
my $distinct_query_count = $node->safe_psql('postgres', <<EOM);
SELECT COUNT(*) FROM
	(SELECT DISTINCT advice FROM pg_get_collected_shared_advice());
EOM
cmp_ok($distinct_query_count, '>', 3000, "diverse advice collected");

# We want to test for the presence of our known tags in the collected advice.
# Put all tags into the hash that follows; map any tags that aren't tested
# by the core regression tests to 0, and others to 1.
my %tag_map = (
	BITMAP_HEAP_SCAN => 1,
	FOREIGN_JOIN => 0,
	GATHER => 1,
	GATHER_MERGE => 1,
	HASH_JOIN => 1,
	INDEX_ONLY_SCAN => 1,
	INDEX_SCAN => 1,
	JOIN_ORDER => 1,
	MERGE_JOIN_MATERIALIZE => 1,
	MERGE_JOIN_PLAIN => 1,
	NESTED_LOOP_MATERIALIZE => 1,
	NESTED_LOOP_MEMOIZE => 1,
	NESTED_LOOP_PLAIN => 1,
	NO_GATHER => 1,
	PARTITIONWISE => 1,
	SEMIJOIN_NON_UNIQUE => 1,
	SEMIJOIN_UNIQUE => 1,
	SEQ_SCAN => 1,
	TID_SCAN => 1,
);
for my $tag (sort keys %tag_map)
{
	my $checkit = $tag_map{$tag};

	# Search for the given tag. This is not entirely robust: it could get thrown
	# off by a table alias such as "FOREIGN_JOIN(", but that probably won't
	# happen in the core regression tests.
	my $tag_count = $node->safe_psql('postgres', <<EOM);
SELECT COUNT(*) FROM pg_get_collected_shared_advice()
	WHERE advice LIKE '%$tag(%'
EOM

	# Check that the tag got a non-trivial amount of use, unless told otherwise.
	cmp_ok($tag_count, '>', 10, "multiple uses of $tag") if $checkit;

	# Regardless, note the exact count in the log, for human consumption.
	note("found $tag_count advice strings containing $tag");
}

# Trigger a partial cleanup of the shared advice collector, and then a full
# cleanup.
$node->safe_psql('postgres', <<EOM);
SET pg_plan_advice.shared_collection_limit=500;
SELECT * FROM pg_clear_collected_shared_advice();
EOM

done_testing();
