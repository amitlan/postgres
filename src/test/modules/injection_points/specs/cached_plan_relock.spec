# Exercise the fallback in PortalRunMulti() taken when a reused generic
# plan's Bind-time partition-pruning survivor set may be stale (the
# command counter advanced between Bind and Execute).  That fallback
# discards the prepared state and re-locks conservatively; if the plan
# was invalidated in the meantime the statement fails with
# serialization_failure.
#
# The fallback is normally only reachable in a pipelined transaction
# (named portals executed out of Bind order).  The "cached-plan-relock"
# injection point stands in for that: while it is attached, an ordinary
# EXECUTE is diverted into the fallback, and the point also provides a
# wait so a concurrent invalidation can be placed precisely in the
# re-lock window.

setup
{
	CREATE EXTENSION injection_points;

	CREATE TABLE cpr (a int, b int) PARTITION BY LIST (a);
	CREATE TABLE cpr1 PARTITION OF cpr FOR VALUES IN (1);
	CREATE TABLE cpr2 PARTITION OF cpr FOR VALUES IN (2);
	CREATE TABLE cpr3 PARTITION OF cpr FOR VALUES IN (3);
	INSERT INTO cpr SELECT g, 0 FROM generate_series(1, 3) g;
}

teardown
{
	DROP TABLE cpr;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SET plan_cache_mode = force_generic_plan;
	-- session setup runs at the start of every permutation on the same
	-- connection, so clear any prepared statement left by a prior one
	DEALLOCATE ALL;
	PREPARE p (int) AS UPDATE cpr SET b = b + 1 WHERE a = $1 RETURNING a;
	-- first EXECUTE builds the generic plan (not yet "reused", so it does
	-- not take the prune-before-lock path); the plan is reused afterwards
	EXECUTE p (1);
	SELECT injection_points_set_local();
}
# attach as 'notice': force the fallback but let it run through
step arm_notice	{ SELECT injection_points_attach('cached-plan-relock', 'notice'); }
# attach as 'wait': force the fallback and park in the re-lock window
step arm_wait	{ SELECT injection_points_attach('cached-plan-relock', 'wait'); }
step exec		{ EXECUTE p (1); }
step noop		{ }
teardown		{ SELECT injection_points_detach('cached-plan-relock'); }

session s2
# invalidate the cached plan on a partition s1 does not hold locked at
# Bind (it pruned to cpr1), so this does not block on s1
step invalidate	{ CREATE INDEX cpr3_a_idx ON cpr3 (a); }
step wakeup		{ SELECT injection_points_wakeup('cached-plan-relock'); }

# Fallback runs to completion when the plan is still valid: the reused
# generic plan is discarded, re-locked conservatively, and executed,
# returning the correct row (a = 1).
permutation arm_notice exec

# Fallback meets a concurrent invalidation: s1 parks at the re-lock
# point, s2 invalidates the plan and wakes s1, whose conservative
# AcquireExecutorLocks() then reports the plan invalid, raising
# serialization_failure.  The noop keeps detach ordered after the wait.
permutation arm_wait exec invalidate wakeup noop
