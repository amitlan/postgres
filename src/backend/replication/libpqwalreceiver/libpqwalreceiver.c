/*-------------------------------------------------------------------------
 *
 * libpqwalreceiver.c
 *
 * This file contains the libpq-specific parts of walreceiver. It's
 * loaded as a dynamic module to avoid linking the main server binary with
 * libpq.
 *
 * Apart from walreceiver, the libpq-specific routines are now being used by
 * logical replication workers and slot synchronization.
 *
 * Portions Copyright (c) 2010-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/libpqwalreceiver/libpqwalreceiver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <sys/time.h>

#include "common/connect.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "libpq/libpq-be-fe-helpers.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pqexpbuffer.h"
#include "replication/walreceiver.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC_EXT(
					.name = "libpqwalreceiver",
					.version = PG_VERSION
);

struct WalReceiverConn
{
	/* Current connection to the primary, if any */
	PGconn	   *streamConn;
	/* Used to remember if the connection is logical or physical */
	bool		logical;
	/* Buffer for currently read records */
	char	   *recvBuf;
};

/* Prototypes for interface functions */
static WalReceiverConn *libpqrcv_connect(const char *conninfo,
										 bool replication, bool logical,
										 bool must_use_password,
										 const char *appname, char **err);
static void libpqrcv_check_conninfo(const char *conninfo,
									bool must_use_password);
static char *libpqrcv_get_conninfo(WalReceiverConn *conn);
static void libpqrcv_get_senderinfo(WalReceiverConn *conn,
									char **sender_host, int *sender_port);
static char *libpqrcv_identify_system(WalReceiverConn *conn,
									  TimeLineID *primary_tli);
static char *libpqrcv_get_dbname_from_conninfo(const char *connInfo);
static int	libpqrcv_server_version(WalReceiverConn *conn);
static void libpqrcv_readtimelinehistoryfile(WalReceiverConn *conn,
											 TimeLineID tli, char **filename,
											 char **content, int *len);
static bool libpqrcv_startstreaming(WalReceiverConn *conn,
									const WalRcvStreamOptions *options);
static void libpqrcv_endstreaming(WalReceiverConn *conn,
								  TimeLineID *next_tli);
static int	libpqrcv_receive(WalReceiverConn *conn, char **buffer,
							 pgsocket *wait_fd);
static void libpqrcv_send(WalReceiverConn *conn, const char *buffer,
						  int nbytes);
static char *libpqrcv_create_slot(WalReceiverConn *conn,
								  const char *slotname,
								  bool temporary,
								  bool two_phase,
								  bool failover,
								  CRSSnapshotAction snapshot_action,
								  XLogRecPtr *lsn);
static void libpqrcv_alter_slot(WalReceiverConn *conn, const char *slotname,
								const bool *failover, const bool *two_phase);
static pid_t libpqrcv_get_backend_pid(WalReceiverConn *conn);
static WalRcvExecResult *libpqrcv_exec(WalReceiverConn *conn,
									   const char *query,
									   const int nRetTypes,
									   const Oid *retTypes);
static void libpqrcv_disconnect(WalReceiverConn *conn);

static WalReceiverFunctionsType PQWalReceiverFunctions = {
	.walrcv_connect = libpqrcv_connect,
	.walrcv_check_conninfo = libpqrcv_check_conninfo,
	.walrcv_get_conninfo = libpqrcv_get_conninfo,
	.walrcv_get_senderinfo = libpqrcv_get_senderinfo,
	.walrcv_identify_system = libpqrcv_identify_system,
	.walrcv_server_version = libpqrcv_server_version,
	.walrcv_readtimelinehistoryfile = libpqrcv_readtimelinehistoryfile,
	.walrcv_startstreaming = libpqrcv_startstreaming,
	.walrcv_endstreaming = libpqrcv_endstreaming,
	.walrcv_receive = libpqrcv_receive,
	.walrcv_send = libpqrcv_send,
	.walrcv_create_slot = libpqrcv_create_slot,
	.walrcv_alter_slot = libpqrcv_alter_slot,
	.walrcv_get_dbname_from_conninfo = libpqrcv_get_dbname_from_conninfo,
	.walrcv_get_backend_pid = libpqrcv_get_backend_pid,
	.walrcv_exec = libpqrcv_exec,
	.walrcv_disconnect = libpqrcv_disconnect
};

/* Prototypes for private functions */
static char *stringlist_to_identifierstr(PGconn *conn, List *strings);

/*
 * Module initialization function
 */
void
_PG_init(void)
{
	if (WalReceiverFunctions != NULL)
		elog(ERROR, "libpqwalreceiver already loaded");
	WalReceiverFunctions = &PQWalReceiverFunctions;
}

/*
 * Establish the connection to the primary server.
 *
 * This function can be used for both replication and regular connections.
 * If it is a replication connection, it could be either logical or physical
 * based on input argument 'logical'.
 *
 * If an error occurs, this function will normally return NULL and set *err
 * to a palloc'ed error message. However, if must_use_password is true and
 * the connection fails to use the password, this function will ereport(ERROR).
 * We do this because in that case the error includes a detail and a hint for
 * consistency with other parts of the system, and it's not worth adding the
 * machinery to pass all of those back to the caller just to cover this one
 * case.
 */
static WalReceiverConn *
libpqrcv_connect(const char *conninfo, bool replication, bool logical,
				 bool must_use_password, const char *appname, char **err)
{
	WalReceiverConn *conn;
	const char *keys[6];
	const char *vals[6];
	int			i = 0;

	/*
	 * Re-validate connection string. The validation already happened at DDL
	 * time, but the subscription owner may have changed. If we don't recheck
	 * with the correct must_use_password, it's possible that the connection
	 * will obtain the password from a different source, such as PGPASSFILE or
	 * PGPASSWORD.
	 */
	libpqrcv_check_conninfo(conninfo, must_use_password);

	/*
	 * We use the expand_dbname parameter to process the connection string (or
	 * URI), and pass some extra options.
	 */
	keys[i] = "dbname";
	vals[i] = conninfo;

	/* We can not have logical without replication */
	Assert(replication || !logical);

	if (replication)
	{
		keys[++i] = "replication";
		vals[i] = logical ? "database" : "true";

		if (logical)
		{
			/* Tell the publisher to translate to our encoding */
			keys[++i] = "client_encoding";
			vals[i] = GetDatabaseEncodingName();

			/*
			 * Force assorted GUC parameters to settings that ensure that the
			 * publisher will output data values in a form that is unambiguous
			 * to the subscriber.  (We don't want to modify the subscriber's
			 * GUC settings, since that might surprise user-defined code
			 * running in the subscriber, such as triggers.)  This should
			 * match what pg_dump does.
			 */
			keys[++i] = "options";
			vals[i] = "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3";
		}
		else
		{
			/*
			 * The database name is ignored by the server in replication mode,
			 * but specify "replication" for .pgpass lookup.
			 */
			keys[++i] = "dbname";
			vals[i] = "replication";
		}
	}

	keys[++i] = "fallback_application_name";
	vals[i] = appname;

	keys[++i] = NULL;
	vals[i] = NULL;

	Assert(i < lengthof(keys));

	conn = palloc0(sizeof(WalReceiverConn));
	conn->streamConn =
		libpqsrv_connect_params(keys, vals,
								 /* expand_dbname = */ true,
								WAIT_EVENT_LIBPQWALRECEIVER_CONNECT);

	if (PQstatus(conn->streamConn) != CONNECTION_OK)
		goto bad_connection_errmsg;

	if (must_use_password && !PQconnectionUsedPassword(conn->streamConn))
	{
		libpqsrv_disconnect(conn->streamConn);
		pfree(conn);

		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				 errmsg("password is required"),
				 errdetail("Non-superuser cannot connect if the server does not request a password."),
				 errhint("Target server's authentication method must be changed, or set password_required=false in the subscription parameters.")));
	}

	PQsetNoticeReceiver(conn->streamConn, libpqsrv_notice_receiver,
						gettext_noop("received message via replication"));

	/*
	 * Set always-secure search path for the cases where the connection is
	 * used to run SQL queries, so malicious users can't get control.
	 */
	if (!replication || logical)
	{
		PGresult   *res;

		res = libpqsrv_exec(conn->streamConn,
							ALWAYS_SECURE_SEARCH_PATH_SQL,
							WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			*err = psprintf(_("could not clear search path: %s"),
							pchomp(PQerrorMessage(conn->streamConn)));
			goto bad_connection;
		}
		PQclear(res);
	}

	conn->logical = logical;

	return conn;

	/* error path, using libpq's error message */
bad_connection_errmsg:
	*err = pchomp(PQerrorMessage(conn->streamConn));

	/* error path, error already set */
bad_connection:
	libpqsrv_disconnect(conn->streamConn);
	pfree(conn);
	return NULL;
}

/*
 * Validate connection info string.
 *
 * If the connection string can't be parsed, this function will raise
 * an error. If must_use_password is true, the function raises an error
 * if no password is provided in the connection string. In any other case
 * it successfully completes.
 */
static void
libpqrcv_check_conninfo(const char *conninfo, bool must_use_password)
{
	PQconninfoOption *opts = NULL;
	PQconninfoOption *opt;
	char	   *err = NULL;

	opts = PQconninfoParse(conninfo, &err);
	if (opts == NULL)
	{
		/* The error string is malloc'd, so we must free it explicitly */
		char	   *errcopy = err ? pstrdup(err) : "out of memory";

		PQfreemem(err);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid connection string syntax: %s", errcopy)));
	}

	if (must_use_password)
	{
		bool		uses_password = false;

		for (opt = opts; opt->keyword != NULL; ++opt)
		{
			/* Ignore connection options that are not present. */
			if (opt->val == NULL)
				continue;

			if (strcmp(opt->keyword, "password") == 0 && opt->val[0] != '\0')
			{
				uses_password = true;
				break;
			}
		}

		if (!uses_password)
		{
			/* malloc'd, so we must free it explicitly */
			PQconninfoFree(opts);

			ereport(ERROR,
					(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
					 errmsg("password is required"),
					 errdetail("Non-superusers must provide a password in the connection string.")));
		}
	}

	PQconninfoFree(opts);
}

/*
 * Return a user-displayable conninfo string.  Any security-sensitive fields
 * are obfuscated.
 */
static char *
libpqrcv_get_conninfo(WalReceiverConn *conn)
{
	PQconninfoOption *conn_opts;
	PQconninfoOption *conn_opt;
	PQExpBufferData buf;
	char	   *retval;

	Assert(conn->streamConn != NULL);

	initPQExpBuffer(&buf);
	conn_opts = PQconninfo(conn->streamConn);

	if (conn_opts == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not parse connection string: %s",
						_("out of memory"))));

	/* build a clean connection string from pieces */
	for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
	{
		bool		obfuscate;

		/* Skip debug and empty options */
		if (strchr(conn_opt->dispchar, 'D') ||
			conn_opt->val == NULL ||
			conn_opt->val[0] == '\0')
			continue;

		/* Obfuscate security-sensitive options */
		obfuscate = strchr(conn_opt->dispchar, '*') != NULL;

		appendPQExpBuffer(&buf, "%s%s=%s",
						  buf.len == 0 ? "" : " ",
						  conn_opt->keyword,
						  obfuscate ? "********" : conn_opt->val);
	}

	PQconninfoFree(conn_opts);

	retval = PQExpBufferDataBroken(buf) ? NULL : pstrdup(buf.data);
	termPQExpBuffer(&buf);
	return retval;
}

/*
 * Provides information of sender this WAL receiver is connected to.
 */
static void
libpqrcv_get_senderinfo(WalReceiverConn *conn, char **sender_host,
						int *sender_port)
{
	char	   *ret = NULL;

	*sender_host = NULL;
	*sender_port = 0;

	Assert(conn->streamConn != NULL);

	ret = PQhost(conn->streamConn);
	if (ret && strlen(ret) != 0)
		*sender_host = pstrdup(ret);

	ret = PQport(conn->streamConn);
	if (ret && strlen(ret) != 0)
		*sender_port = atoi(ret);
}

/*
 * Check that primary's system identifier matches ours, and fetch the current
 * timeline ID of the primary.
 */
static char *
libpqrcv_identify_system(WalReceiverConn *conn, TimeLineID *primary_tli)
{
	PGresult   *res;
	char	   *primary_sysid;

	/*
	 * Get the system identifier and timeline ID as a DataRow message from the
	 * primary server.
	 */
	res = libpqsrv_exec(conn->streamConn,
						"IDENTIFY_SYSTEM",
						WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not receive database system identifier and timeline ID from "
						"the primary server: %s",
						pchomp(PQerrorMessage(conn->streamConn)))));
	}

	/*
	 * IDENTIFY_SYSTEM returns 3 columns in 9.3 and earlier, and 4 columns in
	 * 9.4 and onwards.
	 */
	if (PQnfields(res) < 3 || PQntuples(res) != 1)
	{
		int			ntuples = PQntuples(res);
		int			nfields = PQnfields(res);

		PQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid response from primary server"),
				 errdetail("Could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields.",
						   ntuples, nfields, 1, 3)));
	}
	primary_sysid = pstrdup(PQgetvalue(res, 0, 0));
	*primary_tli = pg_strtoint32(PQgetvalue(res, 0, 1));
	PQclear(res);

	return primary_sysid;
}

/*
 * Thin wrapper around libpq to obtain server version.
 */
static int
libpqrcv_server_version(WalReceiverConn *conn)
{
	return PQserverVersion(conn->streamConn);
}

/*
 * Get database name from the primary server's conninfo.
 *
 * If dbname is not found in connInfo, return NULL value.
 */
static char *
libpqrcv_get_dbname_from_conninfo(const char *connInfo)
{
	PQconninfoOption *opts;
	char	   *dbname = NULL;
	char	   *err = NULL;

	opts = PQconninfoParse(connInfo, &err);
	if (opts == NULL)
	{
		/* The error string is malloc'd, so we must free it explicitly */
		char	   *errcopy = err ? pstrdup(err) : "out of memory";

		PQfreemem(err);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid connection string syntax: %s", errcopy)));
	}

	for (PQconninfoOption *opt = opts; opt->keyword != NULL; ++opt)
	{
		/*
		 * If multiple dbnames are specified, then the last one will be
		 * returned
		 */
		if (strcmp(opt->keyword, "dbname") == 0 && opt->val &&
			*opt->val)
		{
			if (dbname)
				pfree(dbname);

			dbname = pstrdup(opt->val);
		}
	}

	PQconninfoFree(opts);
	return dbname;
}

/*
 * Start streaming WAL data from given streaming options.
 *
 * Returns true if we switched successfully to copy-both mode. False
 * means the server received the command and executed it successfully, but
 * didn't switch to copy-mode.  That means that there was no WAL on the
 * requested timeline and starting point, because the server switched to
 * another timeline at or before the requested starting point. On failure,
 * throws an ERROR.
 */
static bool
libpqrcv_startstreaming(WalReceiverConn *conn,
						const WalRcvStreamOptions *options)
{
	StringInfoData cmd;
	PGresult   *res;

	Assert(options->logical == conn->logical);
	Assert(options->slotname || !options->logical);

	initStringInfo(&cmd);

	/* Build the command. */
	appendStringInfoString(&cmd, "START_REPLICATION");
	if (options->slotname != NULL)
		appendStringInfo(&cmd, " SLOT \"%s\"",
						 options->slotname);

	if (options->logical)
		appendStringInfoString(&cmd, " LOGICAL");

	appendStringInfo(&cmd, " %X/%08X", LSN_FORMAT_ARGS(options->startpoint));

	/*
	 * Additional options are different depending on if we are doing logical
	 * or physical replication.
	 */
	if (options->logical)
	{
		char	   *pubnames_str;
		List	   *pubnames;
		char	   *pubnames_literal;

		appendStringInfoString(&cmd, " (");

		appendStringInfo(&cmd, "proto_version '%u'",
						 options->proto.logical.proto_version);

		if (options->proto.logical.streaming_str)
			appendStringInfo(&cmd, ", streaming '%s'",
							 options->proto.logical.streaming_str);

		if (options->proto.logical.twophase &&
			PQserverVersion(conn->streamConn) >= 150000)
			appendStringInfoString(&cmd, ", two_phase 'on'");

		if (options->proto.logical.origin &&
			PQserverVersion(conn->streamConn) >= 160000)
			appendStringInfo(&cmd, ", origin '%s'",
							 options->proto.logical.origin);

		pubnames = options->proto.logical.publication_names;
		pubnames_str = stringlist_to_identifierstr(conn->streamConn, pubnames);
		if (!pubnames_str)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),	/* likely guess */
					 errmsg("could not start WAL streaming: %s",
							pchomp(PQerrorMessage(conn->streamConn)))));
		pubnames_literal = PQescapeLiteral(conn->streamConn, pubnames_str,
										   strlen(pubnames_str));
		if (!pubnames_literal)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),	/* likely guess */
					 errmsg("could not start WAL streaming: %s",
							pchomp(PQerrorMessage(conn->streamConn)))));
		appendStringInfo(&cmd, ", publication_names %s", pubnames_literal);
		PQfreemem(pubnames_literal);
		pfree(pubnames_str);

		if (options->proto.logical.binary &&
			PQserverVersion(conn->streamConn) >= 140000)
			appendStringInfoString(&cmd, ", binary 'true'");

		appendStringInfoChar(&cmd, ')');
	}
	else
		appendStringInfo(&cmd, " TIMELINE %u",
						 options->proto.physical.startpointTLI);

	/* Start streaming. */
	res = libpqsrv_exec(conn->streamConn,
						cmd.data,
						WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
	pfree(cmd.data);

	if (PQresultStatus(res) == PGRES_COMMAND_OK)
	{
		PQclear(res);
		return false;
	}
	else if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		PQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not start WAL streaming: %s",
						pchomp(PQerrorMessage(conn->streamConn)))));
	}
	PQclear(res);
	return true;
}

/*
 * Stop streaming WAL data. Returns the next timeline's ID in *next_tli, as
 * reported by the server, or 0 if it did not report it.
 */
static void
libpqrcv_endstreaming(WalReceiverConn *conn, TimeLineID *next_tli)
{
	PGresult   *res;

	/*
	 * Send copy-end message.  As in libpqsrv_exec, this could theoretically
	 * block, but the risk seems small.
	 */
	if (PQputCopyEnd(conn->streamConn, NULL) <= 0 ||
		PQflush(conn->streamConn))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not send end-of-streaming message to primary: %s",
						pchomp(PQerrorMessage(conn->streamConn)))));

	*next_tli = 0;

	/*
	 * After COPY is finished, we should receive a result set indicating the
	 * next timeline's ID, or just CommandComplete if the server was shut
	 * down.
	 *
	 * If we had not yet received CopyDone from the backend, PGRES_COPY_OUT is
	 * also possible in case we aborted the copy in mid-stream.
	 */
	res = libpqsrv_get_result(conn->streamConn,
							  WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		/*
		 * Read the next timeline's ID. The server also sends the timeline's
		 * starting point, but it is ignored.
		 */
		if (PQnfields(res) < 2 || PQntuples(res) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected result set after end-of-streaming")));
		*next_tli = pg_strtoint32(PQgetvalue(res, 0, 0));
		PQclear(res);

		/* the result set should be followed by CommandComplete */
		res = libpqsrv_get_result(conn->streamConn,
								  WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
	}
	else if (PQresultStatus(res) == PGRES_COPY_OUT)
	{
		PQclear(res);

		/* End the copy */
		if (PQendcopy(conn->streamConn))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("error while shutting down streaming COPY: %s",
							pchomp(PQerrorMessage(conn->streamConn)))));

		/* CommandComplete should follow */
		res = libpqsrv_get_result(conn->streamConn,
								  WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
	}

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("error reading result of streaming command: %s",
						pchomp(PQerrorMessage(conn->streamConn)))));
	PQclear(res);

	/* Verify that there are no more results */
	res = libpqsrv_get_result(conn->streamConn,
							  WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
	if (res != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected result after CommandComplete: %s",
						pchomp(PQerrorMessage(conn->streamConn)))));
}

/*
 * Fetch the timeline history file for 'tli' from primary.
 */
static void
libpqrcv_readtimelinehistoryfile(WalReceiverConn *conn,
								 TimeLineID tli, char **filename,
								 char **content, int *len)
{
	PGresult   *res;
	char		cmd[64];

	Assert(!conn->logical);

	/*
	 * Request the primary to send over the history file for given timeline.
	 */
	snprintf(cmd, sizeof(cmd), "TIMELINE_HISTORY %u", tli);
	res = libpqsrv_exec(conn->streamConn,
						cmd,
						WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not receive timeline history file from "
						"the primary server: %s",
						pchomp(PQerrorMessage(conn->streamConn)))));
	}
	if (PQnfields(res) != 2 || PQntuples(res) != 1)
	{
		int			ntuples = PQntuples(res);
		int			nfields = PQnfields(res);

		PQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid response from primary server"),
				 errdetail("Expected 1 tuple with 2 fields, got %d tuples with %d fields.",
						   ntuples, nfields)));
	}
	*filename = pstrdup(PQgetvalue(res, 0, 0));

	*len = PQgetlength(res, 0, 1);
	*content = palloc(*len);
	memcpy(*content, PQgetvalue(res, 0, 1), *len);
	PQclear(res);
}

/*
 * Disconnect connection to primary, if any.
 */
static void
libpqrcv_disconnect(WalReceiverConn *conn)
{
	libpqsrv_disconnect(conn->streamConn);
	PQfreemem(conn->recvBuf);
	pfree(conn);
}

/*
 * Receive a message available from XLOG stream.
 *
 * Returns:
 *
 *	 If data was received, returns the length of the data. *buffer is set to
 *	 point to a buffer holding the received message. The buffer is only valid
 *	 until the next libpqrcv_* call.
 *
 *	 If no data was available immediately, returns 0, and *wait_fd is set to a
 *	 socket descriptor which can be waited on before trying again.
 *
 *	 -1 if the server ended the COPY.
 *
 * ereports on error.
 */
static int
libpqrcv_receive(WalReceiverConn *conn, char **buffer,
				 pgsocket *wait_fd)
{
	int			rawlen;

	PQfreemem(conn->recvBuf);
	conn->recvBuf = NULL;

	/* Try to receive a CopyData message */
	rawlen = PQgetCopyData(conn->streamConn, &conn->recvBuf, 1);
	if (rawlen == 0)
	{
		/* Try consuming some data. */
		if (PQconsumeInput(conn->streamConn) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not receive data from WAL stream: %s",
							pchomp(PQerrorMessage(conn->streamConn)))));

		/* Now that we've consumed some input, try again */
		rawlen = PQgetCopyData(conn->streamConn, &conn->recvBuf, 1);
		if (rawlen == 0)
		{
			/* Tell caller to try again when our socket is ready. */
			*wait_fd = PQsocket(conn->streamConn);
			return 0;
		}
	}
	if (rawlen == -1)			/* end-of-streaming or error */
	{
		PGresult   *res;

		res = libpqsrv_get_result(conn->streamConn,
								  WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
		if (PQresultStatus(res) == PGRES_COMMAND_OK)
		{
			PQclear(res);

			/* Verify that there are no more results. */
			res = libpqsrv_get_result(conn->streamConn,
									  WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
			if (res != NULL)
			{
				PQclear(res);

				/*
				 * If the other side closed the connection orderly (otherwise
				 * we'd seen an error, or PGRES_COPY_IN) don't report an error
				 * here, but let callers deal with it.
				 */
				if (PQstatus(conn->streamConn) == CONNECTION_BAD)
					return -1;

				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("unexpected result after CommandComplete: %s",
								PQerrorMessage(conn->streamConn))));
			}

			return -1;
		}
		else if (PQresultStatus(res) == PGRES_COPY_IN)
		{
			PQclear(res);
			return -1;
		}
		else
		{
			PQclear(res);
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("could not receive data from WAL stream: %s",
							pchomp(PQerrorMessage(conn->streamConn)))));
		}
	}
	if (rawlen < -1)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not receive data from WAL stream: %s",
						pchomp(PQerrorMessage(conn->streamConn)))));

	/* Return received messages to caller */
	*buffer = conn->recvBuf;
	return rawlen;
}

/*
 * Send a message to XLOG stream.
 *
 * ereports on error.
 */
static void
libpqrcv_send(WalReceiverConn *conn, const char *buffer, int nbytes)
{
	if (PQputCopyData(conn->streamConn, buffer, nbytes) <= 0 ||
		PQflush(conn->streamConn))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not send data to WAL stream: %s",
						pchomp(PQerrorMessage(conn->streamConn)))));
}

/*
 * Create new replication slot.
 * Returns the name of the exported snapshot for logical slot or NULL for
 * physical slot.
 */
static char *
libpqrcv_create_slot(WalReceiverConn *conn, const char *slotname,
					 bool temporary, bool two_phase, bool failover,
					 CRSSnapshotAction snapshot_action, XLogRecPtr *lsn)
{
	PGresult   *res;
	StringInfoData cmd;
	char	   *snapshot;
	int			use_new_options_syntax;

	use_new_options_syntax = (PQserverVersion(conn->streamConn) >= 150000);

	initStringInfo(&cmd);

	appendStringInfo(&cmd, "CREATE_REPLICATION_SLOT \"%s\"", slotname);

	if (temporary)
		appendStringInfoString(&cmd, " TEMPORARY");

	if (conn->logical)
	{
		appendStringInfoString(&cmd, " LOGICAL pgoutput ");
		if (use_new_options_syntax)
			appendStringInfoChar(&cmd, '(');
		if (two_phase)
		{
			appendStringInfoString(&cmd, "TWO_PHASE");
			if (use_new_options_syntax)
				appendStringInfoString(&cmd, ", ");
			else
				appendStringInfoChar(&cmd, ' ');
		}

		if (failover)
		{
			appendStringInfoString(&cmd, "FAILOVER");
			if (use_new_options_syntax)
				appendStringInfoString(&cmd, ", ");
			else
				appendStringInfoChar(&cmd, ' ');
		}

		if (use_new_options_syntax)
		{
			switch (snapshot_action)
			{
				case CRS_EXPORT_SNAPSHOT:
					appendStringInfoString(&cmd, "SNAPSHOT 'export'");
					break;
				case CRS_NOEXPORT_SNAPSHOT:
					appendStringInfoString(&cmd, "SNAPSHOT 'nothing'");
					break;
				case CRS_USE_SNAPSHOT:
					appendStringInfoString(&cmd, "SNAPSHOT 'use'");
					break;
			}
		}
		else
		{
			switch (snapshot_action)
			{
				case CRS_EXPORT_SNAPSHOT:
					appendStringInfoString(&cmd, "EXPORT_SNAPSHOT");
					break;
				case CRS_NOEXPORT_SNAPSHOT:
					appendStringInfoString(&cmd, "NOEXPORT_SNAPSHOT");
					break;
				case CRS_USE_SNAPSHOT:
					appendStringInfoString(&cmd, "USE_SNAPSHOT");
					break;
			}
		}

		if (use_new_options_syntax)
			appendStringInfoChar(&cmd, ')');
	}
	else
	{
		if (use_new_options_syntax)
			appendStringInfoString(&cmd, " PHYSICAL (RESERVE_WAL)");
		else
			appendStringInfoString(&cmd, " PHYSICAL RESERVE_WAL");
	}

	res = libpqsrv_exec(conn->streamConn,
						cmd.data,
						WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
	pfree(cmd.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not create replication slot \"%s\": %s",
						slotname, pchomp(PQerrorMessage(conn->streamConn)))));
	}

	if (lsn)
		*lsn = DatumGetLSN(DirectFunctionCall1Coll(pg_lsn_in, InvalidOid,
												   CStringGetDatum(PQgetvalue(res, 0, 1))));

	if (!PQgetisnull(res, 0, 2))
		snapshot = pstrdup(PQgetvalue(res, 0, 2));
	else
		snapshot = NULL;

	PQclear(res);

	return snapshot;
}

/*
 * Change the definition of the replication slot.
 */
static void
libpqrcv_alter_slot(WalReceiverConn *conn, const char *slotname,
					const bool *failover, const bool *two_phase)
{
	StringInfoData cmd;
	PGresult   *res;

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "ALTER_REPLICATION_SLOT %s ( ",
					 quote_identifier(slotname));

	if (failover)
		appendStringInfo(&cmd, "FAILOVER %s",
						 *failover ? "true" : "false");

	if (failover && two_phase)
		appendStringInfoString(&cmd, ", ");

	if (two_phase)
		appendStringInfo(&cmd, "TWO_PHASE %s",
						 *two_phase ? "true" : "false");

	appendStringInfoString(&cmd, " );");

	res = libpqsrv_exec(conn->streamConn, cmd.data,
						WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
	pfree(cmd.data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not alter replication slot \"%s\": %s",
						slotname, pchomp(PQerrorMessage(conn->streamConn)))));

	PQclear(res);
}

/*
 * Return PID of remote backend process.
 */
static pid_t
libpqrcv_get_backend_pid(WalReceiverConn *conn)
{
	return PQbackendPID(conn->streamConn);
}

/*
 * Convert tuple query result to tuplestore.
 */
static void
libpqrcv_processTuples(PGresult *pgres, WalRcvExecResult *walres,
					   const int nRetTypes, const Oid *retTypes)
{
	int			tupn;
	int			coln;
	int			nfields = PQnfields(pgres);
	HeapTuple	tuple;
	AttInMetadata *attinmeta;
	MemoryContext rowcontext;
	MemoryContext oldcontext;

	/* Make sure we got expected number of fields. */
	if (nfields != nRetTypes)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid query response"),
				 errdetail("Expected %d fields, got %d fields.",
						   nRetTypes, nfields)));

	walres->tuplestore = tuplestore_begin_heap(true, false, work_mem);

	/* Create tuple descriptor corresponding to expected result. */
	walres->tupledesc = CreateTemplateTupleDesc(nRetTypes);
	for (coln = 0; coln < nRetTypes; coln++)
		TupleDescInitEntry(walres->tupledesc, (AttrNumber) coln + 1,
						   PQfname(pgres, coln), retTypes[coln], -1, 0);
	attinmeta = TupleDescGetAttInMetadata(walres->tupledesc);

	/* No point in doing more here if there were no tuples returned. */
	if (PQntuples(pgres) == 0)
		return;

	/* Create temporary context for local allocations. */
	rowcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "libpqrcv query result context",
									   ALLOCSET_DEFAULT_SIZES);

	/* Process returned rows. */
	for (tupn = 0; tupn < PQntuples(pgres); tupn++)
	{
		char	   *cstrs[MaxTupleAttributeNumber];

		CHECK_FOR_INTERRUPTS();

		/* Do the allocations in temporary context. */
		oldcontext = MemoryContextSwitchTo(rowcontext);

		/*
		 * Fill cstrs with null-terminated strings of column values.
		 */
		for (coln = 0; coln < nfields; coln++)
		{
			if (PQgetisnull(pgres, tupn, coln))
				cstrs[coln] = NULL;
			else
				cstrs[coln] = PQgetvalue(pgres, tupn, coln);
		}

		/* Convert row to a tuple, and add it to the tuplestore */
		tuple = BuildTupleFromCStrings(attinmeta, cstrs);
		tuplestore_puttuple(walres->tuplestore, tuple);

		/* Clean up */
		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(rowcontext);
	}

	MemoryContextDelete(rowcontext);
}

/*
 * Public interface for sending generic queries (and commands).
 *
 * This can only be called from process connected to database.
 */
static WalRcvExecResult *
libpqrcv_exec(WalReceiverConn *conn, const char *query,
			  const int nRetTypes, const Oid *retTypes)
{
	PGresult   *pgres = NULL;
	WalRcvExecResult *walres = palloc0(sizeof(WalRcvExecResult));
	char	   *diag_sqlstate;

	if (MyDatabaseId == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("the query interface requires a database connection")));

	pgres = libpqsrv_exec(conn->streamConn,
						  query,
						  WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);

	switch (PQresultStatus(pgres))
	{
		case PGRES_TUPLES_OK:
		case PGRES_SINGLE_TUPLE:
		case PGRES_TUPLES_CHUNK:
			walres->status = WALRCV_OK_TUPLES;
			libpqrcv_processTuples(pgres, walres, nRetTypes, retTypes);
			break;

		case PGRES_COPY_IN:
			walres->status = WALRCV_OK_COPY_IN;
			break;

		case PGRES_COPY_OUT:
			walres->status = WALRCV_OK_COPY_OUT;
			break;

		case PGRES_COPY_BOTH:
			walres->status = WALRCV_OK_COPY_BOTH;
			break;

		case PGRES_COMMAND_OK:
			walres->status = WALRCV_OK_COMMAND;
			break;

			/* Empty query is considered error. */
		case PGRES_EMPTY_QUERY:
			walres->status = WALRCV_ERROR;
			walres->err = _("empty query");
			break;

		case PGRES_PIPELINE_SYNC:
		case PGRES_PIPELINE_ABORTED:
			walres->status = WALRCV_ERROR;
			walres->err = _("unexpected pipeline mode");
			break;

		case PGRES_NONFATAL_ERROR:
		case PGRES_FATAL_ERROR:
		case PGRES_BAD_RESPONSE:
			walres->status = WALRCV_ERROR;
			walres->err = pchomp(PQerrorMessage(conn->streamConn));
			diag_sqlstate = PQresultErrorField(pgres, PG_DIAG_SQLSTATE);
			if (diag_sqlstate)
				walres->sqlstate = MAKE_SQLSTATE(diag_sqlstate[0],
												 diag_sqlstate[1],
												 diag_sqlstate[2],
												 diag_sqlstate[3],
												 diag_sqlstate[4]);
			break;
	}

	PQclear(pgres);

	return walres;
}

/*
 * Given a List of strings, return it as single comma separated
 * string, quoting identifiers as needed.
 *
 * This is essentially the reverse of SplitIdentifierString.
 *
 * The caller should free the result.
 */
static char *
stringlist_to_identifierstr(PGconn *conn, List *strings)
{
	ListCell   *lc;
	StringInfoData res;
	bool		first = true;

	initStringInfo(&res);

	foreach(lc, strings)
	{
		char	   *val = strVal(lfirst(lc));
		char	   *val_escaped;

		if (first)
			first = false;
		else
			appendStringInfoChar(&res, ',');

		val_escaped = PQescapeIdentifier(conn, val, strlen(val));
		if (!val_escaped)
		{
			free(res.data);
			return NULL;
		}
		appendStringInfoString(&res, val_escaped);
		PQfreemem(val_escaped);
	}

	return res.data;
}
