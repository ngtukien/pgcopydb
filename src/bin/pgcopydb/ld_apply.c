/*
 * src/bin/pgcopydb/ld_apply.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/select.h>
#include <sys/wait.h>
#include <unistd.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"

#include "parson.h"

#include "catalog.h"
#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "env_utils.h"
#include "ld_depkeys.h"
#include "ld_store.h"
#include "ld_stream.h"
#include "lock_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pidfile.h"
#include "pg_utils.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"

#if defined(LIBPQ_HAS_PIPELINING) && LIBPQ_HAS_PIPELINING
#define pqPipelineModeEnabled(conn) (PQpipelineStatus(conn) == PQ_PIPELINE_ON)
#else
#define pqPipelineModeEnabled(conn) (false)
#endif

GUC applySettingsSync[] = {
	COMMON_GUC_SETTINGS,
	{ "synchronous_commit", "on" },
	{ "session_replication_role", "'replica'" },
	{ NULL, NULL },
};

GUC applySettings[] = {
	COMMON_GUC_SETTINGS,
	{ "synchronous_commit", "off" },
	{ "session_replication_role", "'replica'" },
	{ NULL, NULL },
};

static bool readTxnCommitLSN(LogicalMessageMetadata *metadata, const char *dir,
							 bool *txnCommitLSNFound);
static bool parseTxnMetadataFile(const char *filename, LogicalMessageMetadata *metadata);

static bool computeTxnMetadataFilename(uint32_t xid, const char *dir, char *filename);

static bool writeTxnCommitMetadata(LogicalMessageMetadata *mesg, const char *dir);

static bool setupConnection(PGSQL *pgsql, StreamApplyContext *context);
static bool stream_apply_load_target_relkinds(StreamApplyContext *context);

static bool stream_apply_dml(StreamApplyContext *context, ReplayDBStmt *s);
static bool stream_apply_transaction(StreamApplyContext *context,
									 uint32_t xid,
									 uint64_t begin_id,
									 uint64_t commitLSN,
									 bool sync);

/*
 * Parallel apply.
 *
 * The apply process is a scheduler: it reads the apply queue that transform
 * writes (the txn table of the replay.db files, in source commit order) and
 * hands each transaction to one of --apply-jobs worker processes, each with
 * its own connection to the target.
 *
 * Two transactions run at the same time only when their dependency keys do
 * not intersect (see ld_depkeys.c): a transaction waits for every earlier
 * transaction that touched one of its keys, and a barrier transaction runs
 * alone. Independent transactions may then commit on the target in another
 * order than on the source, never two changes of a same row or a same
 * table-level key.
 *
 * Progress is the watermark: the commit LSN up to which every transaction is
 * applied. With a single worker the worker commits in source order and the
 * replication origin tracks the watermark as before. With several workers,
 * each applied transaction also inserts its commit LSN into a small table on
 * the target, pgcopydb.apply_txn, in the same target transaction; the
 * scheduler advances the replication origin to the watermark and deletes the
 * rows below it. After a crash, the transactions above the origin position
 * that have a row there are known to be applied and are skipped: a target
 * commit and its progress record are never separated.
 */

#define APPLY_WINDOW 4096
#define APPLY_FETCH_BATCH 256
#define APPLY_PERSIST_INTERVAL_MS 1000

#define APPLY_PROGRESS_SCHEMA "pgcopydb"
#define APPLY_PROGRESS_TABLE "pgcopydb.apply_txn"

typedef struct ApplyJob
{
	uint64_t seq;
	uint32_t xid;
	uint64_t begin_id;
	uint64_t commitLSN;
	bool sync;
	char replayFile[MAXPGPATH];
} ApplyJob;

typedef struct ApplyResult
{
	int worker;
	uint64_t seq;
	bool success;
} ApplyResult;

typedef enum
{
	APPLY_ENTRY_PENDING = 0,
	APPLY_ENTRY_RUNNING,
	APPLY_ENTRY_DONE
} ApplyEntryState;

typedef struct ApplyEntry
{
	uint64_t seq;
	StreamAction kind;
	uint32_t xid;
	uint64_t begin_id;
	uint64_t lsn;
	uint64_t endlsn;
	bool barrier;
	int file;                   /* index in ApplyScheduler.files */
	ApplyEntryState state;
	bool sent;                  /* handed to a worker */

	int nkeys;
	uint64_t *keys;             /* malloc'ed */

	int ndeps;
	uint64_t *deps;             /* malloc'ed: seq of entries to wait for */
} ApplyEntry;

typedef struct ApplyKeyOwner
{
	uint64_t key;
	uint64_t seq;               /* last entry that has this key */
	UT_hash_handle hh;
} ApplyKeyOwner;

typedef struct ApplySkip
{
	uint64_t lsn;
	UT_hash_handle hh;
} ApplySkip;

typedef struct ApplyWorker
{
	pid_t pid;
	int jobFd;
	bool busy;
} ApplyWorker;

typedef struct ApplyScheduler
{
	StreamSpecs *specs;
	StreamApplyContext *context;

	int jobs;
	ApplyWorker *workers;
	int resultFd;
	bool tableMode;

	/* entries with seq in [head, tail), at ring[seq % APPLY_WINDOW] */
	ApplyEntry *ring;
	uint64_t head;
	uint64_t tail;
	uint64_t lastBarrier;       /* seq + 1 of the last barrier, 0 when none */
	uint64_t limit;             /* entries from this seq are after endpos */
	ApplyKeyOwner *owners;

	/* the replay.db being read, and the files entries refer to */
	DatabaseCatalog replay;
	int64_t fileId;
	uint64_t lastTxnId;
	int nfiles;
	char (*files)[MAXPGPATH];

	bool fetchDone;

	uint64_t watermark;         /* applied up to this commit LSN */
	uint64_t persisted;
	uint64_t lastPersistMs;
	ApplySkip *skip;

	uint64_t applied;
	uint64_t barriers;
	uint64_t lastLog;
	bool failed;
} ApplyScheduler;

static bool apply_scheduler_setup(StreamSpecs *specs,
								  StreamApplyContext *context,
								  ApplyScheduler *s);
static bool apply_scheduler_run(ApplyScheduler *s);
static void apply_scheduler_stop_workers(ApplyScheduler *s);
static bool apply_start_workers(ApplyScheduler *s);
static void apply_worker_main(ApplyScheduler *s, int k, int jobFd, int resultFd);
static bool apply_fetch(ApplyScheduler *s, bool *fetched);
static bool apply_open_queue(ApplyScheduler *s, bool *ready);
static bool apply_enqueue(ApplyScheduler *s, ReplayDBTxn *txn);
static void apply_check_endpos(ApplyScheduler *s);
static bool apply_dispatch(ApplyScheduler *s, bool *dispatched);
static bool apply_collect(ApplyScheduler *s, int timeoutMs, bool *collected);
static void apply_advance(ApplyScheduler *s);
static bool apply_persist(ApplyScheduler *s, bool force);
static bool apply_transform_done(ApplyScheduler *s);
static bool apply_tables_need_barrier(ApplyScheduler *s, const char *tables);
static bool apply_load_progress_table(ApplyScheduler *s);
static uint64_t apply_now_ms(void);
static bool apply_read_full(int fd, void *buf, size_t len, bool *eof);
static bool apply_write_full(int fd, const void *buf, size_t len);

#define ENTRY(s, seq) (&((s)->ring[(seq) % APPLY_WINDOW]))


/*
 * stream_apply_catchup applies the CDC changes queued in the replayDB files
 * to the target database, from the replication origin position, with
 * specs->applyJobs worker processes.
 *
 * When no transform process runs beside us (pgcopydb stream catchup), the
 * changes that receive has written are transformed first.
 */
bool
stream_apply_catchup(StreamSpecs *specs)
{
	StreamApplyContext context = { 0 };
	ApplyScheduler s = { 0 };

	if (specs->transform.pid <= 0)
	{
		if (!stream_transform_drain(specs))
		{
			log_error("Failed to transform the CDC changes, "
					  "see above for details");
			return false;
		}
	}

	if (!apply_scheduler_setup(specs, &context, &s))
	{
		log_error("Failed to setup for catchup, see above for details");
		apply_scheduler_stop_workers(&s);
		(void) stream_apply_cleanup(&context);
		return false;
	}

	if (!context.apply)
	{
		log_notice("Apply mode is still disabled, quitting now");
		(void) stream_apply_cleanup(&context);
		return true;
	}

	(void) pipeline_state_start(specs->sourceDB, "apply", context.previousLSN);

	bool success = apply_scheduler_run(&s);

	apply_scheduler_stop_workers(&s);

	/* the last progress, once every worker is done */
	if (!apply_persist(&s, true))
	{
		success = false;
	}

	(void) pipeline_state_end(specs->sourceDB, "apply",
							  context.previousLSN, success);

	log_info("Applied %llu transactions with %d worker%s (%llu barriers), "
			 "up to LSN %X/%X",
			 (unsigned long long) s.applied,
			 s.jobs,
			 s.jobs > 1 ? "s" : "",
			 (unsigned long long) s.barriers,
			 LSN_FORMAT_ARGS(context.previousLSN));

	ld_store_close_raw(&(s.replay));
	(void) stream_apply_cleanup(&context);

	return success;
}


/*
 * apply_scheduler_setup waits until apply is enabled, finds the starting
 * position on the target and starts the workers.
 */
static bool
apply_scheduler_setup(StreamSpecs *specs,
					  StreamApplyContext *context,
					  ApplyScheduler *s)
{
	s->specs = specs;
	s->context = context;
	s->jobs = specs->applyJobs > 0 ? specs->applyJobs : 1;
	s->resultFd = -1;

	if (!stream_apply_init_context(context,
								   specs->sourceDB,
								   NULL,
								   &(specs->paths),
								   specs->connStrings,
								   specs->origin,
								   specs->endpos))
	{
		/* errors have already been logged */
		return false;
	}

	context->logSQL = specs->logSQL;

	if (!stream_apply_wait_for_sentinel(specs, context))
	{
		/* errors have already been logged */
		return false;
	}

	if (!context->apply)
	{
		return true;
	}

	context->targetDB = specs->targetDB;

	PGSQL *control = &(context->controlPgConn);

	if (!setupConnection(control, context))
	{
		/* errors have already been logged */
		return false;
	}

	uint32_t oid = 0;

	if (!pgsql_replication_origin_oid(control, context->origin, &oid))
	{
		/* errors have already been logged */
		return false;
	}

	if (oid == 0)
	{
		log_error("Failed to fetch progress for replication origin \"%s\": "
				  "replication origin not found on target database",
				  context->origin);
		return false;
	}

	uint64_t originLSN = InvalidXLogRecPtr;

	if (!pgsql_replication_origin_progress(control, context->origin, true,
										   &originLSN))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Setting up previous LSN from replication origin \"%s\" "
			 "progress at %X/%X",
			 context->origin,
			 LSN_FORMAT_ARGS(originLSN));

	context->previousLSN = originLSN;
	s->watermark = originLSN;
	s->persisted = originLSN;

	/*
	 * Several workers need the progress table; so does a single worker when
	 * a previous run with several workers left rows above the origin.
	 */
	if (!apply_load_progress_table(s))
	{
		/* errors have already been logged */
		return false;
	}

	/* build the target relkind cache (matview detection) */
	if (!stream_apply_load_target_relkinds(context))
	{
		log_warn("Failed to load target relkind cache; "
				 "CDC against materialized views will fail if encountered");
	}

	s->ring = (ApplyEntry *) calloc(APPLY_WINDOW, sizeof(ApplyEntry));

	if (s->ring == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!apply_start_workers(s))
	{
		/* errors have already been logged */
		return false;
	}

	if (context->endpos != InvalidXLogRecPtr)
	{
		log_info("Catchup-up with changes from LSN %X/%X up to endpos LSN %X/%X",
				 LSN_FORMAT_ARGS(context->previousLSN),
				 LSN_FORMAT_ARGS(context->endpos));
	}
	else
	{
		log_info("Catchup-up with changes from LSN %X/%X",
				 LSN_FORMAT_ARGS(context->previousLSN));
	}

	log_info("Applying with %d worker%s (%s progress)",
			 s->jobs,
			 s->jobs > 1 ? "s" : "",
			 s->tableMode ? APPLY_PROGRESS_TABLE : "replication origin");

	return true;
}


/*
 * apply_load_progress_table decides how progress is recorded, prepares the
 * progress table when needed and loads the transactions it lists above the
 * replication origin position.
 */
static bool
apply_load_progress_table(ApplyScheduler *s)
{
	StreamApplyContext *context = s->context;
	PGSQL *control = &(context->controlPgConn);
	char sql[BUFSIZE] = { 0 };
	char *origin = context->origin;

	if (strchr(origin, '\'') != NULL)
	{
		log_error("Replication origin name \"%s\" contains a quote", origin);
		return false;
	}

	SingleValueResultContext exists = { { 0 }, PGSQL_RESULT_BOOL, false };

	if (!pgsql_execute_with_params(control,
								   "select to_regclass($1) is not null",
								   1, (const Oid[]) { TEXTOID },
								   (const char *[]) { APPLY_PROGRESS_TABLE },
								   &exists, &parseSingleValueResult))
	{
		/* errors have already been logged */
		return false;
	}

	s->tableMode = s->jobs > 1;

	if (exists.boolVal)
	{
		char lsn[PG_LSN_MAXLENGTH] = { 0 };

		sformat(lsn, sizeof(lsn), "%X/%X", LSN_FORMAT_ARGS(s->watermark));
		sformat(sql, sizeof(sql),
				"select string_agg(commit_lsn::text, ' ') "
				"  from " APPLY_PROGRESS_TABLE
				" where origin = '%s' and commit_lsn > '%s'",
				origin, lsn);

		SingleValueResultContext rows = { { 0 }, PGSQL_RESULT_STRING, false };

		if (!pgsql_execute_with_params(control, sql, 0, NULL, NULL,
									   &rows, &parseSingleValueResult))
		{
			/* errors have already been logged */
			return false;
		}

		if (!rows.isNull && rows.strVal != NULL && rows.strVal[0] != '\0')
		{
			int count = 0;
			char *saveptr = NULL;

			for (char *tok = strtok_r(rows.strVal, " ", &saveptr);
				 tok != NULL;
				 tok = strtok_r(NULL, " ", &saveptr))
			{
				uint64_t applied = InvalidXLogRecPtr;

				if (!parseLSN(tok, &applied))
				{
					log_error("Failed to parse LSN \"%s\" from %s",
							  tok, APPLY_PROGRESS_TABLE);
					return false;
				}

				ApplySkip *skip = (ApplySkip *) calloc(1, sizeof(ApplySkip));

				if (skip == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				skip->lsn = applied;
				HASH_ADD(hh, s->skip, lsn, sizeof(skip->lsn), skip);
				++count;
			}

			log_info("Found %d transactions applied above LSN %X/%X in %s, "
					 "they are skipped",
					 count, LSN_FORMAT_ARGS(s->watermark), APPLY_PROGRESS_TABLE);

			/* keep recording there until the watermark passes them */
			s->tableMode = true;
		}
	}

	if (s->tableMode && !exists.boolVal)
	{
		char *ddl =
			"create schema if not exists " APPLY_PROGRESS_SCHEMA "; "
			"create table if not exists " APPLY_PROGRESS_TABLE
			"(origin text not null, commit_lsn pg_lsn not null, "
			" primary key(origin, commit_lsn))";

		if (!pgsql_execute(control, ddl))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * apply_start_workers forks the workers, each with its own pipe for jobs and
 * a shared pipe for the results.
 */
static bool
apply_start_workers(ApplyScheduler *s)
{
	int resultPipe[2] = { -1, -1 };
	int (*jobPipes)[2] = calloc(s->jobs, sizeof(int[2]));

	s->workers = (ApplyWorker *) calloc(s->jobs, sizeof(ApplyWorker));

	if (jobPipes == NULL || s->workers == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (pipe(resultPipe) != 0)
	{
		log_error("Failed to create the apply result pipe: %m");
		return false;
	}

	for (int k = 0; k < s->jobs; k++)
	{
		if (pipe(jobPipes[k]) != 0)
		{
			log_error("Failed to create the apply job pipe: %m");
			return false;
		}
	}

	fflush(stdout);
	fflush(stderr);

	for (int k = 0; k < s->jobs; k++)
	{
		pid_t pid = fork();

		if (pid == -1)
		{
			log_error("Failed to fork apply worker %d: %m", k);
			return false;
		}

		if (pid == 0)
		{
			/* keep only our job pipe read end and the result write end */
			for (int j = 0; j < s->jobs; j++)
			{
				close(jobPipes[j][1]);

				if (j != k)
				{
					close(jobPipes[j][0]);
				}
			}

			close(resultPipe[0]);

			apply_worker_main(s, k, jobPipes[k][0], resultPipe[1]);

			/* not reached */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		s->workers[k].pid = pid;
		s->workers[k].jobFd = jobPipes[k][1];
		close(jobPipes[k][0]);
	}

	close(resultPipe[1]);
	s->resultFd = resultPipe[0];

	free(jobPipes);

	return true;
}


/*
 * apply_worker_main applies the transactions sent by the scheduler, one at a
 * time, until the job pipe is closed.
 */
static void
apply_worker_main(ApplyScheduler *s, int k, int jobFd, int resultFd)
{
	StreamApplyContext *context = s->context;

	char psTitle[BUFSIZE] = { 0 };

	sformat(psTitle, sizeof(psTitle), "pgcopydb: follow apply worker %d", k);
	(void) set_ps_title(psTitle);

	/*
	 * The scheduler connection and SQLite handles belong to the parent: leave
	 * them alone, without closing them.
	 */
	context->controlPgConn.connection = NULL;
	context->applyPgConn.connection = NULL;
	context->progressTable = s->tableMode;

	PGSQL *apply = &(context->applyPgConn);

	if (!setupConnection(apply, context))
	{
		log_error("Apply worker %d failed to connect to the target", k);
		exit(EXIT_CODE_TARGET);
	}

	/* with a single worker, the replication origin follows each commit */
	if (!s->tableMode &&
		!pgsql_replication_origin_session_setup(apply, context->origin))
	{
		exit(EXIT_CODE_TARGET);
	}

	if (!pgsql_enable_pipeline_mode(apply))
	{
		exit(EXIT_CODE_TARGET);
	}

	DatabaseCatalog replay = { 0 };
	int exitCode = EXIT_CODE_QUIT;

	for (;;)
	{
		ApplyJob job = { 0 };
		bool eof = false;

		if (!apply_read_full(jobFd, &job, sizeof(job), &eof))
		{
			exitCode = EXIT_CODE_INTERNAL_ERROR;
			break;
		}

		if (eof)
		{
			break;
		}

		ApplyResult result = { .worker = k, .seq = job.seq, .success = false };

		if (ld_store_open_raw(&replay, job.replayFile,
							  DATABASE_CATALOG_TYPE_REPLAY, false))
		{
			context->replayDB = &replay;

			result.success = stream_apply_transaction(context,
													  job.xid,
													  job.begin_id,
													  job.commitLSN,
													  job.sync);
		}

		if (!apply_write_full(resultFd, &result, sizeof(result)))
		{
			exitCode = EXIT_CODE_INTERNAL_ERROR;
			break;
		}

		if (!result.success)
		{
			exitCode = EXIT_CODE_TARGET;
			break;
		}
	}

	ld_store_close_raw(&replay);
	(void) pgsql_finish(apply);

	exit(exitCode);
}


/*
 * apply_scheduler_run is the scheduler main loop.
 */
static bool
apply_scheduler_run(ApplyScheduler *s)
{
	StreamApplyContext *context = s->context;
	bool stopping = false;

	s->lastPersistMs = apply_now_ms();
	s->lastLog = time(NULL);

	for (;;)
	{
		bool collected = false;
		bool fetched = false;
		bool dispatched = false;

		if (!stopping && (asked_to_stop || asked_to_stop_fast || asked_to_quit))
		{
			log_info("Apply process received a shutdown signal, "
					 "waiting for the running transactions");
			stopping = true;
		}

		if (!apply_collect(s, 0, &collected))
		{
			s->failed = true;
			stopping = true;
		}

		apply_advance(s);

		int running = 0;

		for (int k = 0; k < s->jobs; k++)
		{
			running += s->workers[k].busy ? 1 : 0;
		}

		if (stopping || s->failed)
		{
			if (running == 0)
			{
				break;
			}
		}
		else
		{
			/* every queued transaction up to endpos is applied */
			bool atLimit = s->limit > 0 && s->head == s->limit;
			bool atEndpos = context->endpos != InvalidXLogRecPtr &&
							s->watermark >= context->endpos;
			bool drained = s->fetchDone && s->head == s->tail;

			if (running == 0 && (atLimit || atEndpos || drained))
			{
				context->reachedEndPos = true;

				/* a keepalive past endpos, like before: report endpos */
				if (atLimit && s->head < s->tail &&
					ENTRY(s, s->head)->kind == STREAM_ACTION_KEEPALIVE &&
					s->watermark < context->endpos)
				{
					s->watermark = context->endpos;
				}

				log_notice("Apply reached end position %X/%X at LSN %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(s->watermark));
				break;
			}

			if (!apply_fetch(s, &fetched))
			{
				s->failed = true;
				continue;
			}

			if (!apply_dispatch(s, &dispatched))
			{
				s->failed = true;
				continue;
			}
		}

		if (!apply_persist(s, false))
		{
			s->failed = true;
			continue;
		}

		if (time(NULL) - s->lastLog >= 10)
		{
			log_info("Apply: %llu transactions applied, at LSN %X/%X, "
					 "%llu queued",
					 (unsigned long long) s->applied,
					 LSN_FORMAT_ARGS(s->watermark),
					 (unsigned long long) (s->tail - s->head));
			s->lastLog = time(NULL);
		}

		/* wait for a result, or for more work to show up */
		if (!collected && !fetched && !dispatched)
		{
			if (!apply_collect(s, 50, &collected))
			{
				s->failed = true;
				stopping = true;
			}
		}
	}

	return !s->failed;
}


/*
 * apply_open_queue opens the replay.db file of the current position, once
 * transform has created it.
 */
static bool
apply_open_queue(ApplyScheduler *s, bool *ready)
{
	DatabaseCatalog *sourceDB = s->specs->sourceDB;
	char lsn[PG_LSN_MAXLENGTH] = { 0 };
	char outputFile[MAXPGPATH] = { 0 };

	*ready = false;

	sformat(lsn, sizeof(lsn), "%08X/%08X", LSN_FORMAT_ARGS(s->watermark));

	char *sql =
		s->watermark != InvalidXLogRecPtr
		? "select id, filename from cdc_files where startpos <= $1 "
		  "order by id desc limit 1"
		: "select id, filename from cdc_files where $1 = $1 "
		  "order by id limit 1";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(sourceDB->db, sql, &query))
	{
		return false;
	}

	sqlite3_bind_text(query.ppStmt, 1, lsn, -1, SQLITE_TRANSIENT);

	int64_t id = 0;
	int rc = catalog_sql_step(&query);

	if (rc == SQLITE_ROW)
	{
		id = sqlite3_column_int64(query.ppStmt, 0);
		strlcpy(outputFile, (char *) sqlite3_column_text(query.ppStmt, 1),
				sizeof(outputFile));
	}

	(void) catalog_sql_finalize(&query);

	if (id == 0)
	{
		/* when there is no CDC file and transform is done, nothing to do */
		if (apply_transform_done(s))
		{
			s->fetchDone = true;
		}

		return true;
	}

	char replayFile[MAXPGPATH] = { 0 };

	ld_store_replaydb_filename(outputFile, replayFile, sizeof(replayFile));

	if (!file_exists(replayFile))
	{
		return true;
	}

	if (!ld_store_open_raw(&(s->replay), replayFile,
						   DATABASE_CATALOG_TYPE_REPLAY, false))
	{
		return false;
	}

	/* transform may still be creating the schema */
	uint64_t endlsn = 0;
	bool switched = false;

	if (!ld_store_replay_txn_last(&(s->replay), &endlsn, &switched))
	{
		ld_store_close_raw(&(s->replay));
		return true;
	}

	s->fileId = id;
	s->lastTxnId = 0;
	*ready = true;

	log_info("Apply reads its queue from \"%s\"", replayFile);

	return true;
}


/*
 * apply_fetch reads more of the apply queue into the scheduler window.
 */
static bool
apply_fetch(ApplyScheduler *s, bool *fetched)
{
	*fetched = false;

	if (s->fetchDone)
	{
		return true;
	}

	if (s->replay.db == NULL)
	{
		bool ready = false;

		if (!apply_open_queue(s, &ready))
		{
			return false;
		}

		if (!ready)
		{
			return true;
		}
	}

	uint64_t room = APPLY_WINDOW - (s->tail - s->head);

	if (room == 0)
	{
		return true;
	}

	/* transform done before the read: then an empty read means the end */
	bool transformDone = apply_transform_done(s);

	ReplayDBTxn batch[APPLY_FETCH_BATCH];
	int count = 0;
	int limit = room < APPLY_FETCH_BATCH ? (int) room : APPLY_FETCH_BATCH;

	if (!ld_store_replay_txn_fetch_batch(&(s->replay), s->lastTxnId, limit,
										 batch, &count))
	{
		return false;
	}

	bool success = true;

	for (int i = 0; i < count; i++)
	{
		ReplayDBTxn *txn = &(batch[i]);

		if (success && !s->fetchDone)
		{
			s->lastTxnId = txn->id;

			if (txn->kind == REPLAY_TXN_KIND_SWITCH)
			{
				/* this file pair is complete, the next one exists */
				char *next =
					"select id, filename from cdc_files where id > $1 "
					"order by id limit 1";

				SQLiteQuery query = { 0 };
				int64_t id = 0;
				char outputFile[MAXPGPATH] = { 0 };

				if (!catalog_sql_prepare(s->specs->sourceDB->db, next, &query))
				{
					success = false;
				}
				else
				{
					sqlite3_bind_int64(query.ppStmt, 1, s->fileId);

					if (catalog_sql_step(&query) == SQLITE_ROW)
					{
						id = sqlite3_column_int64(query.ppStmt, 0);
						strlcpy(outputFile,
								(char *) sqlite3_column_text(query.ppStmt, 1),
								sizeof(outputFile));
					}

					(void) catalog_sql_finalize(&query);
				}

				char replayFile[MAXPGPATH] = { 0 };

				if (id > 0)
				{
					ld_store_replaydb_filename(outputFile, replayFile,
											   sizeof(replayFile));
				}

				if (id == 0 ||
					!ld_store_open_raw(&(s->replay), replayFile,
									   DATABASE_CATALOG_TYPE_REPLAY, false))
				{
					log_error("Failed to open the CDC file pair after %lld",
							  (long long) s->fileId);
					success = false;
				}
				else
				{
					log_info("Apply moved to \"%s\"", replayFile);
					s->fileId = id;
					s->lastTxnId = 0;
					*fetched = true;
				}
			}
			else if (!apply_enqueue(s, txn))
			{
				success = false;
			}
			else
			{
				*fetched = true;
			}
		}

		ld_store_replay_txn_free(txn);
	}

	if (success && count == 0 && transformDone)
	{
		s->fetchDone = true;
	}

	return success;
}


/*
 * apply_enqueue adds a row of the apply queue to the window, and computes
 * the entries it has to wait for.
 */
static bool
apply_enqueue(ApplyScheduler *s, ReplayDBTxn *txn)
{
	ApplyEntry *e = ENTRY(s, s->tail);

	bzero(e, sizeof(ApplyEntry));

	e->seq = s->tail;
	e->kind = txn->kind;
	e->xid = txn->xid;
	e->begin_id = txn->begin_id;
	e->lsn = txn->lsn;
	e->endlsn = txn->endlsn;
	e->state = APPLY_ENTRY_PENDING;

	/* where workers read this transaction */
	if (s->nfiles == 0 || !streq(s->files[s->nfiles - 1], s->replay.dbfile))
	{
		char (*files)[MAXPGPATH] =
			realloc(s->files, (s->nfiles + 1) * sizeof(*s->files));

		if (files == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		s->files = files;
		strlcpy(s->files[s->nfiles++], s->replay.dbfile, MAXPGPATH);
	}

	e->file = s->nfiles - 1;

	bool applied = false;

	if (txn->kind == REPLAY_TXN_KIND_TXN)
	{
		ApplySkip *skip = NULL;

		HASH_FIND(hh, s->skip, &(txn->endlsn), sizeof(txn->endlsn), skip);

		/* applied before a restart, or nothing to apply */
		applied = txn->endlsn <= s->watermark || skip != NULL || txn->keys == NULL;
	}
	else
	{
		/* keepalive and rollback: nothing to apply */
		applied = true;
	}

	if (applied)
	{
		e->state = APPLY_ENTRY_DONE;
	}
	else
	{
		bool barrier = false;

		if (!depkeys_parse(txn->keys, &(e->keys), &(e->nkeys), &barrier))
		{
			/* errors have already been logged */
			return false;
		}

		e->barrier = barrier || apply_tables_need_barrier(s, txn->tables);

		/* at most one dependency per key, plus the last barrier */
		e->deps = (uint64_t *) calloc(e->nkeys + 1, sizeof(uint64_t));

		if (e->deps == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		if (s->lastBarrier > 0)
		{
			uint64_t b = s->lastBarrier - 1;

			if (b >= s->head && ENTRY(s, b)->state != APPLY_ENTRY_DONE)
			{
				e->deps[e->ndeps++] = b;
			}
		}

		for (int i = 0; i < e->nkeys; i++)
		{
			ApplyKeyOwner *owner = NULL;

			HASH_FIND(hh, s->owners, &(e->keys[i]), sizeof(uint64_t), owner);

			if (owner == NULL)
			{
				owner = (ApplyKeyOwner *) calloc(1, sizeof(ApplyKeyOwner));

				if (owner == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				owner->key = e->keys[i];
				HASH_ADD(hh, s->owners, key, sizeof(owner->key), owner);
			}
			else if (owner->seq >= s->head &&
					 ENTRY(s, owner->seq)->state != APPLY_ENTRY_DONE)
			{
				e->deps[e->ndeps++] = owner->seq;
			}

			owner->seq = e->seq;
		}

		if (e->barrier)
		{
			s->lastBarrier = e->seq + 1;
		}
	}

	++s->tail;

	apply_check_endpos(s);

	return true;
}


/*
 * apply_check_endpos stops the queue at endpos: transactions committed up to
 * endpos are applied, then the first one committed after endpos when it
 * began before endpos (endpos in the middle of it), and none after that.
 */
static void
apply_check_endpos(ApplyScheduler *s)
{
	uint64_t endpos = s->context->endpos;

	if (endpos == InvalidXLogRecPtr || s->limit > 0)
	{
		return;
	}

	for (uint64_t seq = s->head; seq < s->tail; seq++)
	{
		ApplyEntry *e = ENTRY(s, seq);

		/* sent to a worker before endpos was known: it cannot be held back */
		if (e->endlsn <= endpos || e->sent)
		{
			continue;
		}

		bool midTxn = e->kind == REPLAY_TXN_KIND_TXN && e->lsn < endpos;

		s->limit = midTxn ? seq + 1 : seq;
		s->fetchDone = true;

		log_notice("Apply stops at endpos %X/%X, before the changes "
				   "committed at %X/%X",
				   LSN_FORMAT_ARGS(endpos),
				   LSN_FORMAT_ARGS(midTxn && seq + 1 < s->tail
								   ? ENTRY(s, seq + 1)->endlsn
								   : e->endlsn));
		return;
	}
}


/*
 * apply_entry_ready is true when every entry this one waits for is done.
 */
static bool
apply_entry_ready(ApplyScheduler *s, ApplyEntry *e)
{
	/* a barrier runs alone, after everything before it */
	if (e->barrier)
	{
		return e->seq == s->head;
	}

	for (int i = 0; i < e->ndeps; i++)
	{
		uint64_t dep = e->deps[i];

		if (dep >= s->head && ENTRY(s, dep)->state != APPLY_ENTRY_DONE)
		{
			return false;
		}
	}

	return true;
}


/*
 * apply_dispatch sends the ready entries to the idle workers, oldest first.
 */
static bool
apply_dispatch(ApplyScheduler *s, bool *dispatched)
{
	StreamApplyContext *context = s->context;

	*dispatched = false;

	int idle = 0;

	for (int k = 0; k < s->jobs; k++)
	{
		idle += s->workers[k].busy ? 0 : 1;
	}

	uint64_t end = s->limit > 0 ? Min(s->limit, s->tail) : s->tail;

	for (uint64_t seq = s->head; seq < end && idle > 0; seq++)
	{
		ApplyEntry *e = ENTRY(s, seq);

		if (e->state != APPLY_ENTRY_PENDING)
		{
			continue;
		}

		if (!apply_entry_ready(s, e))
		{
			/* nothing runs beside a barrier waiting for its turn */
			if (e->barrier)
			{
				break;
			}

			continue;
		}

		int k = 0;

		while (s->workers[k].busy)
		{
			++k;
		}

		ApplyJob job = {
			.seq = e->seq,
			.xid = e->xid,
			.begin_id = e->begin_id,
			.commitLSN = e->endlsn,
			.sync = context->endpos != InvalidXLogRecPtr &&
					context->endpos <= e->endlsn
		};

		strlcpy(job.replayFile, s->files[e->file], sizeof(job.replayFile));

		if (!apply_write_full(s->workers[k].jobFd, &job, sizeof(job)))
		{
			log_error("Failed to send transaction %u to apply worker %d",
					  e->xid, k);
			return false;
		}

		e->state = APPLY_ENTRY_RUNNING;
		e->sent = true;
		s->workers[k].busy = true;
		--idle;
		*dispatched = true;

		if (e->barrier)
		{
			++s->barriers;

			/* nothing else starts until the barrier is done */
			break;
		}
	}

	return true;
}


/*
 * apply_collect reads the results the workers sent, waiting at most
 * timeoutMs for one.
 */
static bool
apply_collect(ApplyScheduler *s, int timeoutMs, bool *collected)
{
	*collected = false;

	for (;;)
	{
		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(s->resultFd, &rfds);

		struct timeval tv = {
			.tv_sec = timeoutMs / 1000,
			.tv_usec = (timeoutMs % 1000) * 1000
		};

		int nready = select(s->resultFd + 1, &rfds, NULL, NULL, &tv);

		if (nready < 0)
		{
			if (errno == EINTR)
			{
				return true;
			}

			log_error("Failed to wait for the apply workers: %m");
			return false;
		}

		if (nready == 0)
		{
			break;
		}

		ApplyResult result = { 0 };
		bool eof = false;

		if (!apply_read_full(s->resultFd, &result, sizeof(result), &eof) || eof)
		{
			log_error("Apply workers have exited");
			return false;
		}

		*collected = true;
		timeoutMs = 0;

		if (result.worker < 0 || result.worker >= s->jobs ||
			result.seq < s->head || result.seq >= s->tail)
		{
			log_error("BUG: unexpected apply result for seq %llu from worker %d",
					  (unsigned long long) result.seq, result.worker);
			return false;
		}

		s->workers[result.worker].busy = false;

		ApplyEntry *e = ENTRY(s, result.seq);

		if (!result.success)
		{
			log_error("Apply worker %d failed to apply transaction %u "
					  "committed at %X/%X, see above for details",
					  result.worker, e->xid, LSN_FORMAT_ARGS(e->endlsn));

			/* the transaction is not applied: do not move past it */
			s->failed = true;
			continue;
		}

		e->state = APPLY_ENTRY_DONE;
		++s->applied;
	}

	/* a worker that died without a word */
	for (int k = 0; k < s->jobs; k++)
	{
		int status = 0;

		if (s->workers[k].pid > 0 &&
			waitpid(s->workers[k].pid, &status, WNOHANG) == s->workers[k].pid)
		{
			log_error("Apply worker %d (pid %d) has exited unexpectedly",
					  k, s->workers[k].pid);
			s->workers[k].pid = -1;
			s->workers[k].busy = false;
			return false;
		}
	}

	return true;
}


/*
 * apply_advance moves the head of the window past the done entries, and the
 * watermark to the commit LSN of the last of them.
 */
static void
apply_advance(ApplyScheduler *s)
{
	while (s->head < s->tail)
	{
		ApplyEntry *e = ENTRY(s, s->head);

		if (e->state != APPLY_ENTRY_DONE)
		{
			break;
		}

		if (s->limit > 0 && s->head >= s->limit)
		{
			break;
		}

		/* a rollback has no commit to report */
		if (e->kind != REPLAY_TXN_KIND_ROLLBACK && e->endlsn > s->watermark)
		{
			s->watermark = e->endlsn;
		}

		for (int i = 0; i < e->nkeys; i++)
		{
			ApplyKeyOwner *owner = NULL;

			HASH_FIND(hh, s->owners, &(e->keys[i]), sizeof(uint64_t), owner);

			if (owner != NULL && owner->seq == e->seq)
			{
				HASH_DEL(s->owners, owner);
				free(owner);
			}
		}

		free(e->keys);
		free(e->deps);
		e->keys = NULL;
		e->deps = NULL;

		++s->head;
	}
}


/*
 * apply_persist records the watermark: in the replication origin (with
 * several workers, the scheduler advances it), and in the sentinel, which
 * receive reports to the source as the flush position.
 */
static bool
apply_persist(ApplyScheduler *s, bool force)
{
	StreamApplyContext *context = s->context;
	uint64_t now = apply_now_ms();

	/* not set up: apply was never enabled */
	if (context->controlPgConn.connection == NULL)
	{
		return true;
	}

	if (!force && now - s->lastPersistMs < APPLY_PERSIST_INTERVAL_MS)
	{
		return true;
	}

	s->lastPersistMs = now;

	if (s->tableMode && s->watermark > s->persisted)
	{
		char lsn[PG_LSN_MAXLENGTH] = { 0 };
		char sql[BUFSIZE] = { 0 };

		sformat(lsn, sizeof(lsn), "%X/%X", LSN_FORMAT_ARGS(s->watermark));

		/*
		 * A synchronous commit also flushes every commit of the workers
		 * before it: the origin never runs ahead of the applied data.
		 */
		sformat(sql, sizeof(sql),
				"begin; "
				"set local synchronous_commit to on; "
				"select pg_replication_origin_advance('%s', '%s'); "
				"delete from " APPLY_PROGRESS_TABLE
				" where origin = '%s' and commit_lsn <= '%s'; "
				"commit",
				context->origin, lsn, context->origin, lsn);

		if (!pgsql_execute(&(context->controlPgConn), sql))
		{
			log_error("Failed to record apply progress at %s", lsn);
			return false;
		}

		/* forget the skipped transactions below the watermark */
		ApplySkip *skip, *tmp;

		HASH_ITER(hh, s->skip, skip, tmp)
		{
			if (skip->lsn <= s->watermark)
			{
				HASH_DEL(s->skip, skip);
				free(skip);
			}
		}
	}

	s->persisted = s->watermark;

	if (context->previousLSN < s->watermark)
	{
		context->previousLSN = s->watermark;
	}

	uint64_t endpos = context->endpos;

	if (!stream_apply_sync_sentinel(context, false))
	{
		log_warn("Failed to sync sentinel at LSN %X/%X",
				 LSN_FORMAT_ARGS(context->previousLSN));
	}

	/* endpos can be set while we run */
	if (context->endpos != endpos)
	{
		log_info("Apply endpos is now %X/%X", LSN_FORMAT_ARGS(context->endpos));
		apply_check_endpos(s);
	}

	return true;
}


/*
 * apply_scheduler_stop_workers closes the job pipes, which the workers read
 * as the end of their work, and waits for them.
 */
static void
apply_scheduler_stop_workers(ApplyScheduler *s)
{
	if (s->workers == NULL)
	{
		return;
	}

	for (int k = 0; k < s->jobs; k++)
	{
		if (s->workers[k].jobFd > 0)
		{
			close(s->workers[k].jobFd);
			s->workers[k].jobFd = -1;
		}
	}

	for (int k = 0; k < s->jobs; k++)
	{
		int status = 0;

		if (s->workers[k].pid > 0)
		{
			(void) waitpid(s->workers[k].pid, &status, 0);
			s->workers[k].pid = -1;
		}
	}

	if (s->resultFd > 0)
	{
		close(s->resultFd);
		s->resultFd = -1;
	}
}


/*
 * apply_transform_done is true when the transform process of this follow run
 * has finished, or when there is none (it ran before us).
 */
static bool
apply_transform_done(ApplyScheduler *s)
{
	StreamSpecs *specs = s->specs;

	if (specs->transform.pid <= 0)
	{
		return true;
	}

	PipelineStateEntry ts = { 0 };

	return pipeline_state_get(specs->sourceDB, "transform", &ts) &&
		   ts.process_name[0] != '\0' &&
		   strcmp(ts.run_state, "done") == 0 &&
		   ts.pid == specs->transform.pid;
}


/*
 * apply_tables_need_barrier is true when a transaction changes a relation
 * that is a materialized view on the target: apply refreshes it, which reads
 * other tables, so it runs alone.
 */
static bool
apply_tables_need_barrier(ApplyScheduler *s, const char *tables)
{
	TargetRelkind *cache = s->context->targetRelkindCache;

	if (tables == NULL || cache == NULL)
	{
		return false;
	}

	const char *p = tables;

	while (*p != '\0')
	{
		const char *eol = strchr(p, '\n');
		size_t len = eol != NULL ? (size_t) (eol - p) : strlen(p);

		/* split nspname.relname on the first dot outside of quotes */
		bool quoted = false;
		size_t dot = len;

		for (size_t i = 0; i < len; i++)
		{
			if (p[i] == '"')
			{
				quoted = !quoted;
			}
			else if (p[i] == '.' && !quoted)
			{
				dot = i;
				break;
			}
		}

		if (dot < len)
		{
			TargetRelkind lookup = { 0 };

			sformat(lookup.nspname, sizeof(lookup.nspname), "%.*s",
					(int) dot, p);
			sformat(lookup.relname, sizeof(lookup.relname), "%.*s",
					(int) (len - dot - 1), p + dot + 1);

			unsigned keylen =
				offsetof(TargetRelkind, relname) +
				sizeof(lookup.relname) -
				offsetof(TargetRelkind, nspname);

			TargetRelkind *found = NULL;

			HASH_FIND(hh, cache, lookup.nspname, keylen, found);

			if (found != NULL && found->relkind == 'm')
			{
				return true;
			}
		}

		p += len;

		if (*p == '\n')
		{
			++p;
		}
	}

	return false;
}


static uint64_t
apply_now_ms(void)
{
	struct timespec ts = { 0 };

	clock_gettime(CLOCK_MONOTONIC, &ts);

	return (uint64_t) ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}


static bool
apply_read_full(int fd, void *buf, size_t len, bool *eof)
{
	size_t done = 0;

	*eof = false;

	while (done < len)
	{
		ssize_t n = read(fd, (char *) buf + done, len - done);

		if (n == 0)
		{
			*eof = true;
			return done == 0;
		}

		if (n < 0)
		{
			if (errno == EINTR)
			{
				continue;
			}

			log_error("Failed to read from the apply pipe: %m");
			return false;
		}

		done += n;
	}

	return true;
}


static bool
apply_write_full(int fd, const void *buf, size_t len)
{
	size_t done = 0;

	while (done < len)
	{
		ssize_t n = write(fd, (const char *) buf + done, len - done);

		if (n < 0)
		{
			if (errno == EINTR)
			{
				continue;
			}

			log_error("Failed to write to the apply pipe: %m");
			return false;
		}

		done += n;
	}

	return true;
}


/*
 * stream_apply_transaction applies a single committed transaction to the
 * target database.  It opens a Postgres transaction, iterates all DML rows
 * for xid in replay order (starting from begin_id), and commits with the
 * replication origin set to commitLSN.
 *
 * When endpos <= commitLSN, synchronous_commit = on is enabled so the origin
 * position is durably flushed before the caller signals endpos reached.
 *
 * Returns false on error; on success context->previousLSN = commitLSN.
 */
static bool
stream_apply_transaction(StreamApplyContext *context,
						 uint32_t xid,
						 uint64_t begin_id,
						 uint64_t commitLSN,
						 bool sync)
{
	PGSQL *applyPgConn = &(context->applyPgConn);
	DatabaseCatalog *replayDB = context->replayDB;

	if (!pgsql_begin(applyPgConn))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_set_gucs(applyPgConn,
						sync ? applySettingsSync : applySettings))
	{
		/* errors have already been logged */
		(void) pgsql_execute(applyPgConn, "ROLLBACK");
		return false;
	}

	context->transactionInProgress = true;
	context->reachedStartPos = true;
	context->continuedTxn = false;

	ReplayDBReplayTxnIterator iter = { 0 };

	iter.catalog = replayDB;
	iter.xid = xid;
	iter.begin_id = begin_id;

	if (!ld_store_iter_replay_txn_init(&iter))
	{
		/* errors have already been logged */
		(void) pgsql_execute(applyPgConn, "ROLLBACK");
		context->transactionInProgress = false;
		return false;
	}

	bool success = true;
	char commitTimestamp[PG_MAX_TIMESTAMP] = { 0 };

	/*
	 * Per-transaction sent-REFRESH set: when the first DML row targeting a
	 * matview is detected we immediately queue REFRESH on the pipeline so the
	 * server can start executing it while the client continues reading the
	 * remaining DML rows from SQLite.  The hash deduplicates: subsequent DML
	 * rows for the same matview are skipped without sending another REFRESH.
	 */
	TargetRelkind *sentRefresh = NULL;

	for (;;)
	{
		if (!ld_store_iter_replay_txn_next(&iter))
		{
			/* errors have already been logged */
			success = false;
			break;
		}

		ReplayDBStmt *s = iter.current;

		if (s == NULL)
		{
			log_error("BUG: stream_apply_transaction: no COMMIT/ROLLBACK row "
					  "found for xid %u (begin_id %lld commitLSN %X/%X)",
					  xid, (long long) begin_id,
					  LSN_FORMAT_ARGS(commitLSN));
			success = false;
			break;
		}

		if (s->action == STREAM_ACTION_BEGIN)
		{
			/* skip: the outer loop already processed this */
			continue;
		}

		if (s->action == STREAM_ACTION_COMMIT)
		{
			strlcpy(commitTimestamp, s->timestamp, sizeof(commitTimestamp));
			break;
		}

		if (s->action == STREAM_ACTION_ROLLBACK)
		{
			/*
			 * Rolled-back transactions are normally not emitted by logical
			 * decoding, but handle defensively.  We abort the target
			 * transaction and leave previousLSN unchanged.
			 */
			log_notice("Rolling back transaction %u (rollback LSN %X/%X)",
					   xid, LSN_FORMAT_ARGS(s->lsn));

			(void) ld_store_iter_replay_txn_finish(&iter);

			TargetRelkind *pr, *prTmp;
			HASH_ITER(hh, sentRefresh, pr, prTmp)
			{
				HASH_DEL(sentRefresh, pr);
				free(pr);
			}

			if (!pgsql_execute(applyPgConn, "ROLLBACK"))
			{
				/* errors have already been logged */
				return false;
			}

			context->transactionInProgress = false;
			return true;
		}

		/* DML row (INSERT / UPDATE / DELETE / TRUNCATE) */

		/*
		 * If the target relation is a materialized view, skip the DML and
		 * issue REFRESH MATERIALIZED VIEW on the first occurrence.  REFRESH
		 * is queued into the pipeline immediately (no CONCURRENTLY, so it
		 * runs inside the open transaction) — the server starts executing it
		 * while the client continues reading the rest of the DML rows from
		 * SQLite.  Subsequent DML rows for the same matview are silently
		 * skipped using the sentRefresh deduplication hash.
		 */
		if (s->action != STREAM_ACTION_TRUNCATE &&
			context->targetRelkindCache != NULL &&
			!IS_EMPTY_STRING_BUFFER(s->nspname))
		{
			TargetRelkind lookupKey = { 0 };
			strlcpy(lookupKey.nspname, s->nspname, sizeof(lookupKey.nspname));
			strlcpy(lookupKey.relname, s->relname, sizeof(lookupKey.relname));

			unsigned keylen =
				offsetof(TargetRelkind, relname) +
				sizeof(lookupKey.relname) -
				offsetof(TargetRelkind, nspname);

			TargetRelkind *found = NULL;

			HASH_FIND(hh, context->targetRelkindCache,
					  lookupKey.nspname, keylen, found);

			if (found != NULL && found->relkind == 'm')
			{
				TargetRelkind *sent = NULL;

				HASH_FIND(hh, sentRefresh, lookupKey.nspname, keylen, sent);

				if (sent == NULL)
				{
					/*
					 * First DML row for this matview in this transaction:
					 * send REFRESH now into the pipeline so the server starts
					 * working on it immediately.
					 */
					char refreshSQL[BUFSIZE] = { 0 };

					sformat(refreshSQL, sizeof(refreshSQL),
							"REFRESH MATERIALIZED VIEW %s.%s",
							s->nspname, s->relname);

					log_info("Refreshing materialized view %s.%s (xid %u)",
							 s->nspname, s->relname, xid);

					if (!pgsql_execute(applyPgConn, refreshSQL))
					{
						log_error("Failed to queue REFRESH for %s.%s",
								  s->nspname, s->relname);
						success = false;
						break;
					}

					sent = (TargetRelkind *) calloc(1, sizeof(TargetRelkind));

					if (sent == NULL)
					{
						log_error(ALLOCATION_FAILED_ERROR);
						success = false;
						break;
					}

					strlcpy(sent->nspname, s->nspname, sizeof(sent->nspname));
					strlcpy(sent->relname, s->relname, sizeof(sent->relname));
					sent->relkind = 'm';

					HASH_ADD(hh, sentRefresh, nspname, keylen, sent);
				}

				/* skip the DML row — REFRESH covers the matview update */
				continue;
			}
		}

		if (!stream_apply_dml(context, s))
		{
			/* errors have already been logged */
			success = false;
			break;
		}
	}

	(void) ld_store_iter_replay_txn_finish(&iter);

	/* free the per-transaction sent-REFRESH dedup set */
	{
		TargetRelkind *pr, *prTmp;

		HASH_ITER(hh, sentRefresh, pr, prTmp)
		{
			HASH_DEL(sentRefresh, pr);
			free(pr);
		}
	}

	if (!success)
	{
		(void) pgsql_execute(applyPgConn, "ROLLBACK");
		context->transactionInProgress = false;
		return false;
	}

	/* set replication origin to commitLSN before committing */
	char lsn[PG_LSN_MAXLENGTH] = { 0 };

	sformat(lsn, sizeof(lsn), "%X/%X", LSN_FORMAT_ARGS(commitLSN));

	if (IS_EMPTY_STRING_BUFFER(commitTimestamp))
	{
		TimestampTz now = feGetCurrentTimestamp();

		(void) pgsql_timestamptz_to_string(now,
										   commitTimestamp,
										   sizeof(commitTimestamp));
	}

	if (context->progressTable)
	{
		/*
		 * Several workers commit out of source order: record this commit in
		 * the same target transaction, the scheduler moves the replication
		 * origin once every transaction before it is applied too.
		 */
		char progressSQL[BUFSIZE] = { 0 };

		sformat(progressSQL, sizeof(progressSQL),
				"insert into pgcopydb.apply_txn(origin, commit_lsn) "
				"values('%s', '%s')",
				context->origin, lsn);

		if (!pgsql_execute(applyPgConn, progressSQL))
		{
			log_error("Failed to record the apply progress of xid %u at %X/%X",
					  xid, LSN_FORMAT_ARGS(commitLSN));
			(void) pgsql_execute(applyPgConn, "ROLLBACK");
			context->transactionInProgress = false;
			return false;
		}
	}
	else if (!pgsql_replication_origin_xact_setup(applyPgConn, lsn,
												  commitTimestamp))
	{
		log_error("Failed to setup replication origin for xid %u at %X/%X",
				  xid, LSN_FORMAT_ARGS(commitLSN));
		(void) pgsql_execute(applyPgConn, "ROLLBACK");
		context->transactionInProgress = false;
		return false;
	}

	log_debug("COMMIT xid %u LSN %X/%X", xid, LSN_FORMAT_ARGS(commitLSN));

	if (!pgsql_execute(applyPgConn, "COMMIT"))
	{
		/* errors have already been logged */
		context->transactionInProgress = false;
		return false;
	}

	/*
	 * In pipeline mode the statements above are only sent: wait for the
	 * results up to the COMMIT, so that the transaction is reported applied
	 * only once the target has committed it, and any error is seen here.
	 */
	if (pqPipelineModeEnabled(applyPgConn->connection) &&
		!pgsql_sync_pipeline(applyPgConn))
	{
		log_error("Failed to apply transaction %u committed at %X/%X on the "
				  "source, see above for details",
				  xid, LSN_FORMAT_ARGS(commitLSN));
		context->transactionInProgress = false;
		return false;
	}

	context->transactionInProgress = false;
	context->previousLSN = commitLSN;

	/* the scheduler reports progress, once the transactions before are done */
	return true;
}


/*
 * stream_apply_dml applies a single DML row (INSERT/UPDATE/DELETE/TRUNCATE)
 * to the target database.  The transaction must already be open.
 *
 * INSERT/UPDATE/DELETE use the PREPARE-once-per-hash + EXECUTE pattern.
 * TRUNCATE is executed directly (with TRUNCATE ONLY → TRUNCATE rewrite for
 * partitioned target tables).
 *
 * Rows with a NULL stmt and zero hash (generated-column degenerate rows) are
 * silently skipped.
 */
static bool
stream_apply_dml(StreamApplyContext *context, ReplayDBStmt *s)
{
	PGSQL *applyPgConn = &(context->applyPgConn);

	if (s->action == STREAM_ACTION_TRUNCATE)
	{
		const char *sql = (s->stmt != NULL) ? s->stmt : "";
		int len = strlen(sql);
		char truncateSQL[BUFSIZE] = { 0 };

		strlcpy(truncateSQL, sql, sizeof(truncateSQL));

		/* chomp trailing semicolon */
		if (len > 0 && truncateSQL[len - 1] == ';')
		{
			truncateSQL[len - 1] = '\0';
		}

		const char *execSQL = truncateSQL;
		char rewritten[BUFSIZE] = { 0 };
		const char onlyPrefix[] = "TRUNCATE ONLY ";
		size_t prefixLen = sizeof(onlyPrefix) - 1;

		if (strncmp(truncateSQL, onlyPrefix, prefixLen) == 0)
		{
			const char *qname = truncateSQL + prefixLen;
			bool isPartitioned = false;

			if (context->targetRelkindCache != NULL &&
				!IS_EMPTY_STRING_BUFFER(s->nspname))
			{
				TargetRelkind lookupKey = { 0 };
				strlcpy(lookupKey.nspname, s->nspname, sizeof(lookupKey.nspname));
				strlcpy(lookupKey.relname, s->relname, sizeof(lookupKey.relname));

				unsigned keylen =
					offsetof(TargetRelkind, relname) +
					sizeof(lookupKey.relname) -
					offsetof(TargetRelkind, nspname);

				TargetRelkind *found = NULL;

				HASH_FIND(hh, context->targetRelkindCache,
						  lookupKey.nspname, keylen, found);

				isPartitioned = (found != NULL && found->relkind == 'p');
			}
			else
			{
				/* cache unavailable: fall back to live lookup on controlPgConn */
				char relkind = '\0';

				if (pgsql_get_table_relkind(&(context->controlPgConn),
											qname, &relkind))
				{
					isPartitioned = (relkind == 'p');
				}
				else
				{
					log_warn("Could not resolve relkind for replicated "
							 "TRUNCATE on \"%s\"; replaying as TRUNCATE ONLY.",
							 qname);
				}
			}

			if (isPartitioned)
			{
				sformat(rewritten, sizeof(rewritten), "TRUNCATE %s", qname);
				execSQL = rewritten;
			}
		}

		if (!pgsql_execute(applyPgConn, execSQL))
		{
			/* errors have already been logged */
			return false;
		}

		return true;
	}

	/* INSERT / UPDATE / DELETE */

	if (s->stmt == NULL && s->hash == 0)
	{
		/* degenerate row (e.g. UPDATE on a generated column) — skip */
		return true;
	}

	uint32_t hash = s->hash;
	PreparedStmt *stmtHashTable = context->preparedStmt;
	PreparedStmt *stmt = NULL;

	HASH_FIND(hh, stmtHashTable, &hash, sizeof(hash), stmt);

	if (stmt == NULL)
	{
		char name[NAMEDATALEN] = { 0 };
		sformat(name, sizeof(name), "%x", hash);

		if (!pgsql_prepare(applyPgConn, name, s->stmt, 0, NULL))
		{
			/* errors have already been logged */
			return false;
		}

		stmt = (PreparedStmt *) calloc(1, sizeof(PreparedStmt));

		if (stmt == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		stmt->hash = hash;
		stmt->prepared = true;

		HASH_ADD(hh, stmtHashTable, hash, sizeof(hash), stmt);

		/* HASH_ADD can change the pointer in place, update */
		context->preparedStmt = stmtHashTable;
	}

	if (s->data != NULL)
	{
		char name[NAMEDATALEN] = { 0 };
		sformat(name, sizeof(name), "%x", hash);

		JSON_Value *js = json_parse_string(s->data);

		if (json_value_get_type(js) != JSONArray)
		{
			log_error("Failed to parse DML args array: %s", s->data);
			json_value_free(js);
			return false;
		}

		JSON_Array *jsArray = json_value_get_array(js);
		int count = json_array_get_count(jsArray);

		if (count > 0)
		{
			const char **paramValues =
				(const char **) calloc(count, sizeof(char *));

			if (paramValues == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				json_value_free(js);
				return false;
			}

			for (int i = 0; i < count; i++)
			{
				paramValues[i] = json_array_get_string(jsArray, i);
			}

			if (!pgsql_execute_prepared(applyPgConn, name,
										count, paramValues,
										NULL, NULL))
			{
				/* errors have already been logged */
				free(paramValues);
				json_value_free(js);
				return false;
			}

			free(paramValues);
		}

		json_value_free(js);
	}

	return true;
}


/*
 * stream_apply_setup does the required setup for then starting to catchup or
 * to replay changes from the SQL input (files or Unix PIPE) to the target
 * database.
 */
bool
stream_apply_setup(StreamSpecs *specs, StreamApplyContext *context)
{
	/* init our context */
	if (!stream_apply_init_context(context,
								   specs->sourceDB,
								   specs->replayDB,
								   &(specs->paths),
								   specs->connStrings,
								   specs->origin,
								   specs->endpos))
	{
		/* errors have already been logged */
		return false;
	}

	context->logSQL = specs->logSQL;

	/* wait until the sentinel enables the apply process */
	if (!stream_apply_wait_for_sentinel(specs, context))
	{
		/* errors have already been logged */
		return false;
	}

	if (!context->apply)
	{
		log_notice("Apply mode is still disabled, quitting now");
		return true;
	}

	/* propagate targetDB from specs to context */
	context->targetDB = specs->targetDB;

	/*
	 * Determine previousLSN (the apply cursor) before opening the CDC files
	 * so we can select the correct starting file when file rotation has
	 * produced multiple output.db files during prefetch.
	 */
	if (!setupReplicationOrigin(context))
	{
		log_error("Failed to setup replication origin on the target database");
		return false;
	}

	/* build the target relkind cache (matview detection) */
	if (!stream_apply_load_target_relkinds(context))
	{
		/* non-fatal: log warning and continue without matview interception */
		log_warn("Failed to load target relkind cache; "
				 "CDC against materialized views will fail if encountered");
	}

	/*
	 * Pre-populate specs->outputDB->dbfile with the correct starting file:
	 *
	 *  - previousLSN > 0: the origin already has progress; open the file
	 *    whose LSN range contains that LSN.
	 *  - previousLSN == 0: fresh start; open the oldest file (smallest id)
	 *    so we process every file in order when rotation has already happened.
	 *
	 * ld_store_open_outputdb skips its own set_current call when the
	 * filename is already set.
	 */
	if (context->previousLSN != InvalidXLogRecPtr)
	{
		if (!ld_store_set_cdc_filename_at_lsn(specs, context->previousLSN))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		if (!ld_store_set_first_cdc_filename(specs))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!ld_store_open_outputdb(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!ld_store_open_replaydb(specs))
	{
		/* errors have already been logged */
		return false;
	}

	char *process =
		specs->mode == STREAM_MODE_CATCHUP ? "Catchup-up with" : "Replaying";

	if (context->endpos != InvalidXLogRecPtr)
	{
		if (context->endpos <= context->previousLSN)
		{
			log_info("Current endpos %X/%X was previously reached at %X/%X",
					 LSN_FORMAT_ARGS(context->endpos),
					 LSN_FORMAT_ARGS(context->previousLSN));

			return true;
		}

		log_info("%s changes from LSN %X/%X up to endpos LSN %X/%X",
				 process,
				 LSN_FORMAT_ARGS(context->previousLSN),
				 LSN_FORMAT_ARGS(context->endpos));
	}
	else
	{
		log_info("%s changes from LSN %X/%X",
				 process,
				 LSN_FORMAT_ARGS(context->previousLSN));
	}

	return true;
}


/*
 * stream_apply_cleanup cleans up the resources used by the apply process.
 */
bool
stream_apply_cleanup(StreamApplyContext *context)
{
	/* make sure we close the connection on the way out */
	(void) pgsql_finish(&(context->controlPgConn));

	(void) pgsql_finish(&(context->applyPgConn));

	/* free the target relkind in-memory cache */
	TargetRelkind *entry, *tmp;

	HASH_ITER(hh, context->targetRelkindCache, entry, tmp)
	{
		HASH_DEL(context->targetRelkindCache, entry);
		free(entry);
	}

	return true;
}


/*
 * stream_apply_wait_for_sentinel fetches the current pgcopydb sentinel values:
 * the catchup processing only gets to start when the sentinel "apply" column
 * has been set to true.
 */
bool
stream_apply_wait_for_sentinel(StreamSpecs *specs, StreamApplyContext *context)
{
	bool firstLoop = true;
	CopyDBSentinel sentinel = { 0 };

	/* make sure context->apply is false before entering the loop */
	context->apply = false;

	while (!context->apply)
	{
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_info("Apply process received a shutdown signal "
					 "while waiting for apply mode, "
					 "quitting now");
			return true;
		}

		/* this reconnects on each loop iteration, every 10s by default */
		if (!sentinel_get(specs->sourceDB, &sentinel))
		{
			log_warn("Retrying to fetch pgcopydb sentinel values in %ds",
					 CATCHINGUP_SLEEP_MS / 10);
			pg_usleep(CATCHINGUP_SLEEP_MS * 1000);

			continue;
		}

		/*
		 * Now grab the current sentinel values.
		 *
		 * The pgcopydb sentinel table contains an endpos. The --endpos command
		 * line option (found in specs->endpos) prevails, but when it's not
		 * been used, we have a look at the sentinel value.
		 */
		context->startpos = sentinel.startpos;
		context->apply = sentinel.apply;

		if (specs->endpos == InvalidXLogRecPtr)
		{
			context->endpos = sentinel.endpos;
		}
		else if (context->endpos != sentinel.endpos)
		{
			log_warn("Sentinel endpos is %X/%X, overriden by --endpos %X/%X",
					 LSN_FORMAT_ARGS(sentinel.endpos),
					 LSN_FORMAT_ARGS(specs->endpos));
		}

		if (context->previousLSN == InvalidXLogRecPtr)
		{
			context->previousLSN = sentinel.replay_lsn;
		}
		else
		{
			log_debug("stream_apply_wait_for_sentinel: "
					  "previous lsn %X/%X, replay_lsn %X/%X",
					  LSN_FORMAT_ARGS(context->previousLSN),
					  LSN_FORMAT_ARGS(sentinel.replay_lsn));
		}

		log_debug("startpos %X/%X endpos %X/%X apply %s",
				  LSN_FORMAT_ARGS(context->startpos),
				  LSN_FORMAT_ARGS(context->endpos),
				  context->apply ? "enabled" : "disabled");

		if (context->apply)
		{
			break;
		}

		if (firstLoop)
		{
			firstLoop = false;

			log_info("Waiting until the pgcopydb sentinel apply is enabled");
		}

		/* avoid buzy looping and avoid hammering the source database */
		pg_usleep(CATCHINGUP_SLEEP_MS * 1000);
	}

	/* when apply was already set on first loop, don't even mention it */
	if (!firstLoop)
	{
		log_info("The pgcopydb sentinel has enabled applying changes");
	}

	return true;
}


/*
 * stream_apply_sync_sentinel sync with the pgcopydb sentinel table, sending
 * the current replay LSN position and fetching the maybe new endpos and apply
 * values.
 */
bool
stream_apply_sync_sentinel(StreamApplyContext *context, bool findDurableLSN)
{
	uint64_t durableLSN = InvalidXLogRecPtr;

	/*
	 * If we know we reached endpos, then publish that as the replay_lsn.
	 */
	if (context->reachedEndPos || !findDurableLSN)
	{
		durableLSN = context->previousLSN;
	}
	else
	{
		if (!stream_apply_find_durable_lsn(context, &durableLSN))
		{
			log_warn("Skipping sentinel replay_lsn update: "
					 "failed to find a durable LSN matching current flushLSN");
			return true;
		}
	}

	CopyDBSentinel sentinel = { 0 };

	if (!sentinel_sync_apply(context->sourceDB, durableLSN, &sentinel))
	{
		log_warn("Failed to sync progress with the pgcopydb sentinel");
		return true;
	}

	context->apply = sentinel.apply;
	context->endpos = sentinel.endpos;
	context->startpos = sentinel.startpos;
	context->sentinelSyncTime = time(NULL);

	log_debug("stream_apply_sync_sentinel: "
			  "write_lsn %X/%X flush_lsn %X/%X replay_lsn %X/%X "
			  "startpos %X/%X endpos %X/%X apply %s",
			  LSN_FORMAT_ARGS(sentinel.write_lsn),
			  LSN_FORMAT_ARGS(sentinel.flush_lsn),
			  LSN_FORMAT_ARGS(sentinel.replay_lsn),
			  LSN_FORMAT_ARGS(context->startpos),
			  LSN_FORMAT_ARGS(context->endpos),
			  context->apply ? "enabled" : "disabled");

	return true;
}


/*
 * stream_apply_sql connects to the target database system and applies the
 * given SQL command as prepared by the stream_transform_file or
 * stream_transform_stream function.
 */
bool
stream_apply_sql(StreamApplyContext *context,
				 LogicalMessageMetadata *metadata,
				 const char *sql)
{
	PGSQL *applyPgConn = &(context->applyPgConn);

	switch (metadata->action)
	{
		case STREAM_ACTION_SWITCH:
		{
			log_debug("SWITCH from %X/%X to %X/%X",
					  LSN_FORMAT_ARGS(context->switchLSN),
					  LSN_FORMAT_ARGS(metadata->lsn));

			/*
			 * Track the SWITCH LSN, it helps to determine the next
			 * .sql file to apply.
			 */
			context->switchLSN = metadata->lsn;

			/*
			 * Advance previousLSN and sync sentinel.replay_lsn to the SWITCH
			 * position.  Without this, follow_reached_endpos() compares
			 * sentinel.replay_lsn (last COMMIT) against sentinel.endpos and
			 * finds them unequal, causing follow_main_loop() to restart the
			 * CATCHUP→REPLAY cycle forever even though all WAL has been applied.
			 *
			 * A SWITCH message marks a WAL-segment boundary and is always the
			 * last record in a replayDB segment; its LSN is the endpos that the
			 * inject/sentinel process sets.  Syncing here allows
			 * follow_reached_endpos() to detect completion correctly.
			 */
			context->previousLSN = metadata->lsn;

			bool findDurableLSN =
				context->reachedEOF ||
				(context->endpos != InvalidXLogRecPtr &&
				 context->endpos <= metadata->lsn);

			if (!stream_apply_sync_sentinel(context, findDurableLSN))
			{
				log_warn("Failed to sync sentinel at SWITCH LSN %X/%X, "
						 "will retry on next iteration",
						 LSN_FORMAT_ARGS(metadata->lsn));
			}

			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= metadata->lsn)
			{
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X at SWITCH %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(metadata->lsn));
			}

			break;
		}

		case STREAM_ACTION_BEGIN:
		{
			if (metadata->lsn == InvalidXLogRecPtr ||
				IS_EMPTY_STRING_BUFFER(metadata->timestamp))
			{
				log_fatal("Failed to parse BEGIN message: %s", sql);
				return false;
			}

			bool txnCommitLSNFound = false;

			if (!readTxnCommitLSN(metadata,
								  context->paths.dir,
								  &txnCommitLSNFound))
			{
				log_error("Failed to read transaction metadata file");
				return false;
			}

			/*
			 * Few a time, BEGIN won't have a txnCommitLSN for the txn which
			 * spread across multiple WAL segments. We call that txn as
			 * a continuedTxn and allow it to be replayed until we encounter
			 * a COMMIT message.
			 *
			 * The lsn of a COMMIT message determines whether to keep txn or
			 * abort.
			 */
			context->continuedTxn = !txnCommitLSNFound;

			/* did we reach the starting LSN positions now? */
			if (!context->reachedStartPos)
			{
				/*
				 * compare previousLSN with COMMIT LSN to safely include
				 * complete transactions while skipping already applied
				 * changes.
				 *
				 * this is particularly useful at the beginnig where
				 * BEGIN LSN of some transactions could be less than
				 * `consistent_point`, but COMMIT LSN of those transactions
				 * is guaranteed to be greater.
				 *
				 * in case of interruption and this is the first
				 * transaction to be applied, previousLSN should be equal
				 * to the last transaction's COMMIT LSN or the LSN of
				 * non-transaction action. Therefore, this condition will
				 * still hold true.
				 */
				context->reachedStartPos =
					context->previousLSN < metadata->txnCommitLSN;
			}

			bool skip = !context->reachedStartPos && !context->continuedTxn;

			log_debug("BEGIN %lld LSN %X/%X @%s, previous LSN %X/%X, COMMIT LSN %X/%X %s",
					  (long long) metadata->xid,
					  LSN_FORMAT_ARGS(metadata->lsn),
					  metadata->timestamp,
					  LSN_FORMAT_ARGS(context->previousLSN),
					  LSN_FORMAT_ARGS(metadata->txnCommitLSN),
					  skip ? "[skipping]" : "");

			/*
			 * Check if we reached the endpos LSN already.
			 */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= metadata->lsn)
			{
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X at BEGIN %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(metadata->lsn));

				return true;
			}

			/* actually skip this one if we didn't reach start pos yet */
			if (skip)
			{
				return true;
			}

			/*
			 * We're all good to replay that transaction, let's BEGIN and
			 * register our origin tracking on the target database.
			 */
			if (!pgsql_begin(applyPgConn))
			{
				/* errors have already been logged */
				return false;
			}

			/*
			 * If this transaction is going to reach the endpos, then we're
			 * happy to wait until it's been sync'ed on-disk by Postgres on the
			 * target.
			 *
			 * In other words, use synchronous_commit = on.
			 */
			bool commitLSNreachesEndPos =
				context->endpos != InvalidXLogRecPtr &&
				!context->continuedTxn &&
				context->endpos <= metadata->txnCommitLSN;

			GUC *settings =
				commitLSNreachesEndPos || context->reachedEOF
				? applySettingsSync
				: applySettings;

			if (commitLSNreachesEndPos)
			{
				log_notice("BEGIN transaction with COMMIT LSN %X/%X which is "
						   "reaching endpos %X/%X, synchronous_commit is on",
						   LSN_FORMAT_ARGS(metadata->txnCommitLSN),
						   LSN_FORMAT_ARGS(context->endpos));
			}

			if (!pgsql_set_gucs(applyPgConn, settings))
			{
				log_error("Failed to set the apply GUC settings, "
						  "see above for details");
				return false;
			}

			context->transactionInProgress = true;

			break;
		}

		case STREAM_ACTION_ROLLBACK:
		{
			/* Rollback the transaction */
			if (!pgsql_execute(applyPgConn, "ROLLBACK"))
			{
				/* errors have already been logged */
				return false;
			}

			/* Reset the transactionInProgress after abort */
			context->transactionInProgress = false;

			/* Reevaluate reachedStartPos after rollback */
			context->reachedStartPos = false;

			break;
		}

		case STREAM_ACTION_COMMIT:
		{
			context->reachedStartPos = context->previousLSN < metadata->lsn;

			if (context->continuedTxn)
			{
				/*
				 * Write the transaction metadata file for continuedTxn.
				 * This file will be used for the resumed transaction
				 * to determine whether allow the transaction to be
				 * replayed or not.
				 * Without this, executing the same continuedTxn twice
				 * will result in duplicate key errors if the table has
				 * unique constraints.
				 */
				if (!writeTxnCommitMetadata(metadata, context->paths.dir))
				{
					log_error("Failed to write transaction metadata file, "
							  "see above for details");
					return false;
				}
			}

			if (!context->reachedStartPos)
			{
				/*
				 * Abort if we are not yet reachedStartPos and txn is a
				 * continuedTxn.
				 */
				if (context->continuedTxn)
				{
					log_notice("Skip(abort) applied transaction %lld LSN %X/%X "
							   "@%s, previous LSN %X/%X",
							   (long long) metadata->xid,
							   LSN_FORMAT_ARGS(metadata->lsn),
							   metadata->timestamp,
							   LSN_FORMAT_ARGS(context->previousLSN));

					/* Rollback the transaction */
					if (!pgsql_execute(applyPgConn, "ROLLBACK"))
					{
						/* errors have already been logged */
						return false;
					}

					/* Reset the transactionInProgress after abort */
					context->transactionInProgress = false;
					context->continuedTxn = false;
				}

				return true;
			}

			/*
			 * update replication progress with metadata->lsn, that is,
			 * transaction COMMIT LSN
			 */
			char lsn[PG_LSN_MAXLENGTH] = { 0 };

			sformat(lsn, sizeof(lsn), "%X/%X",
					LSN_FORMAT_ARGS(metadata->lsn));

			if (!pgsql_replication_origin_xact_setup(applyPgConn,
													 lsn,
													 metadata->timestamp))
			{
				log_error("Failed to setup apply transaction, "
						  "see above for details");
				return false;
			}

			log_trace("COMMIT %lld LSN %X/%X",
					  (long long) metadata->xid,
					  LSN_FORMAT_ARGS(metadata->lsn));


			/* calling pgsql_commit() would finish the connection, avoid */
			if (!pgsql_execute(applyPgConn, "COMMIT"))
			{
				/* errors have already been logged */
				return false;
			}

			context->transactionInProgress = false;
			context->previousLSN = metadata->lsn;

			/*
			 * At COMMIT time we might have reached the endpos: we know
			 * that already when endpos <= lsn. It's important to check
			 * that at COMMIT record time, because that record might be the
			 * last entry of the file we're applying.
			 */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= context->previousLSN)
			{
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X at COMMIT %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(context->previousLSN));
				return true;
			}

			break;
		}

		case STREAM_ACTION_ENDPOS:
		{
			if (!context->reachedStartPos && !context->continuedTxn)
			{
				return true;
			}

			log_debug("ENDPOS %X/%X found at %X/%X",
					  LSN_FORMAT_ARGS(metadata->lsn),
					  LSN_FORMAT_ARGS(context->previousLSN));

			/*
			 * It could be the current endpos, or the endpos of a previous
			 * run.
			 */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= metadata->lsn)
			{
				context->previousLSN = metadata->lsn;
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X at ENDPOS %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(context->previousLSN));

				if (context->transactionInProgress)
				{
					if (!pgsql_execute(applyPgConn, "ROLLBACK"))
					{
						/* errors have already been logged */
						return false;
					}

					context->transactionInProgress = false;
				}

				return true;
			}

			break;
		}

		/*
		 * A KEEPALIVE message is replayed as its own transaction where the
		 * only thgin we do is call into the replication origin tracking
		 * API to advance our position on the target database.
		 */
		case STREAM_ACTION_KEEPALIVE:
		{
			/* did we reach the starting LSN positions now? */
			if (!context->reachedStartPos && !context->continuedTxn)
			{
				context->reachedStartPos =
					context->previousLSN < metadata->lsn;
			}

			/* in a transaction only the COMMIT LSN is tracked */
			if (context->transactionInProgress)
			{
				return true;
			}

			log_trace("KEEPALIVE LSN %X/%X @%s, previous LSN %X/%X %s",
					  LSN_FORMAT_ARGS(metadata->lsn),
					  metadata->timestamp,
					  LSN_FORMAT_ARGS(context->previousLSN),
					  context->reachedStartPos ? "" : "[skipping]");

			if (metadata->lsn == InvalidXLogRecPtr)
			{
				log_fatal("Failed to parse KEEPALIVE message: %s", sql);
				return false;
			}

			/*
			 * When the timestamp is empty (e.g. from a stale SQL file
			 * written before the empty-timestamp fix), use the current
			 * time as a fallback. The timestamp is only used for
			 * replication origin tracking, so a local timestamp is safe.
			 */
			if (IS_EMPTY_STRING_BUFFER(metadata->timestamp))
			{
				TimestampTz now = feGetCurrentTimestamp();

				if (!pgsql_timestamptz_to_string(now,
												 metadata->timestamp,
												 sizeof(metadata->timestamp)))
				{
					log_fatal("Failed to generate fallback timestamp "
							  "for KEEPALIVE message: %s", sql);
					return false;
				}

				log_debug("KEEPALIVE at LSN %X/%X has empty timestamp, "
						  "using current time \"%s\" as fallback",
						  LSN_FORMAT_ARGS(metadata->lsn),
						  metadata->timestamp);
			}

			/*
			 * Check if we reached the endpos LSN already. If the keepalive
			 * message is the endpos, still apply it: its only purpose is
			 * to maintain our replication origin tracking on the target
			 * database.
			 */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos < metadata->lsn)
			{
				context->reachedEndPos = true;
				context->previousLSN = metadata->lsn;

				log_notice("Apply reached end position %X/%X at KEEPALIVE %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(context->previousLSN));

				return true;
			}

			/* actually skip this one if we didn't reach start pos yet */
			if (!context->reachedStartPos)
			{
				return true;
			}

			/* skip KEEPALIVE message that won't make progress */
			if (metadata->lsn == context->previousLSN)
			{
				return true;
			}

			if (!pgsql_begin(applyPgConn))
			{
				/* errors have already been logged */
				return false;
			}

			/*
			 * Replication origin is handled differently by the postgres
			 * backend to avoid database bloat and runtime overhead[1].
			 * This optimization leads to persist origin progress only when
			 * the txn modifies the state of the database. So, an empty txn
			 * created to update KEEPALIVE LSN effectively ignored by the
			 * backend leading to not updating the origin progress.
			 *
			 * To workaround this, we execute `SELECT txid_current()` query to
			 * force the backend to update the origin progress.
			 *
			 * [1] https://www.postgresql.org/docs/current/replication-origins.html
			 */
			char *sql = "SELECT txid_current()";

			if (!pgsql_execute(applyPgConn, sql))
			{
				/* errors have already been logged */
				return false;
			}

			/*
			 * Use context->previousLSN (the last committed data-transaction
			 * LSN) rather than metadata->lsn (the KEEPALIVE position) for the
			 * replication origin advancement.
			 *
			 * A KEEPALIVE fires at cur_record_lsn which may be *inside* an
			 * uncommitted transaction (the mid-transaction endpos scenario).
			 * If we advance the origin to that position, the next apply
			 * session starts its replay cursor at the KEEPALIVE lsn, which
			 * skips the straddling transaction's BEGIN and DML rows (their
			 * lsn < KEEPALIVE lsn).
			 *
			 * By recording only the last durably committed transaction's LSN,
			 * the replay cursor on the next session will start from
			 * previousLSN and naturally include any straddling transaction.
			 *
			 * Fall back to metadata->lsn when previousLSN is zero (origin
			 * has never been advanced in this session).
			 */
			uint64_t originLSN =
				(context->previousLSN != InvalidXLogRecPtr)
				? context->previousLSN
				: metadata->lsn;

			char lsn[PG_LSN_MAXLENGTH] = { 0 };

			sformat(lsn, sizeof(lsn), "%X/%X",
					LSN_FORMAT_ARGS(originLSN));

			if (!pgsql_replication_origin_xact_setup(applyPgConn,
													 lsn,
													 metadata->timestamp))
			{
				/* errors have already been logged */
				return false;
			}

			/* calling pgsql_commit() would finish the connection, avoid */
			if (!pgsql_execute(applyPgConn, "COMMIT"))
			{
				/* errors have already been logged */
				return false;
			}

			context->previousLSN = metadata->lsn;

			/*
			 * At COMMIT time we might have reached the endpos: we know
			 * that already when endpos <= lsn. It's important to check
			 * that at COMMIT record time, because that record might be the
			 * last entry of the file we're applying.
			 */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= context->previousLSN)
			{
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X at KEEPALIVE %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(context->previousLSN));
				break;
			}

			break;
		}

		case STREAM_ACTION_INSERT:
		case STREAM_ACTION_UPDATE:
		case STREAM_ACTION_DELETE:
		{
			/*
			 * We still allow continuedTxn, COMMIT message determines whether
			 * to keep the transaction or abort it.
			 */
			if (!context->reachedStartPos && !context->continuedTxn)
			{
				return true;
			}

			uint32_t hash = metadata->hash;
			PreparedStmt *stmtHashTable = context->preparedStmt;
			PreparedStmt *stmt = NULL;

			HASH_FIND(hh, stmtHashTable, &hash, sizeof(hash), stmt);

			if (stmt == NULL)
			{
				char name[NAMEDATALEN] = { 0 };
				sformat(name, sizeof(name), "%x", metadata->hash);

				if (!pgsql_prepare(applyPgConn, name, metadata->stmt, 0, NULL))
				{
					/* errors have already been logged */
					return false;
				}

				stmt = (PreparedStmt *) calloc(1, sizeof(PreparedStmt));
				stmt->hash = hash;
				stmt->prepared = true;

				HASH_ADD(hh, stmtHashTable, hash, sizeof(hash), stmt);

				/* HASH_ADD can change the pointer in place, update */
				context->preparedStmt = stmtHashTable;
			}

			/*
			 * In the SQLite pipeline the replay row carries the parameter
			 * array in the same row (passed here via metadata->jsonBuffer).
			 * Execute the statement immediately after preparing it.
			 */
			if (metadata->jsonBuffer != NULL)
			{
				char name[NAMEDATALEN] = { 0 };
				sformat(name, sizeof(name), "%x", metadata->hash);

				JSON_Value *js = json_parse_string(metadata->jsonBuffer);

				if (json_value_get_type(js) != JSONArray)
				{
					log_error("Failed to parse DML args array: %s",
							  metadata->jsonBuffer);
					return false;
				}

				JSON_Array *jsArray = json_value_get_array(js);
				int count = json_array_get_count(jsArray);

				if (0 < count)
				{
					const char **paramValues =
						(const char **) calloc(count, sizeof(char *));

					if (paramValues == NULL)
					{
						log_error(ALLOCATION_FAILED_ERROR);
						return false;
					}

					for (int i = 0; i < count; i++)
					{
						paramValues[i] = json_array_get_string(jsArray, i);
					}

					if (!pgsql_execute_prepared(applyPgConn, name,
												count, paramValues,
												NULL, NULL))
					{
						/* errors have already been logged */
						return false;
					}

					free(paramValues);
				}

				json_value_free(js);
			}

			break;
		}

		case STREAM_ACTION_EXECUTE:
		{
			/*
			 * We still allow continuedTxn, COMMIT message determines whether
			 * to keep the transaction or abort it.
			 */
			if (!context->reachedStartPos && !context->continuedTxn)
			{
				return true;
			}

			uint32_t hash = metadata->hash;
			PreparedStmt *stmtHashTable = context->preparedStmt;
			PreparedStmt *stmt = NULL;

			HASH_FIND(hh, stmtHashTable, &hash, sizeof(hash), stmt);

			if (stmt == NULL)
			{
				log_warn("BUG: Failed to find statement %x in stmtHashTable",
						 hash);
			}

			char name[NAMEDATALEN] = { 0 };
			sformat(name, sizeof(name), "%x", metadata->hash);

			JSON_Value *js = json_parse_string(metadata->jsonBuffer);

			if (json_value_get_type(js) != JSONArray)
			{
				log_error("Failed to parse EXECUTE array: %s",
						  metadata->jsonBuffer);
				return false;
			}

			JSON_Array *jsArray = json_value_get_array(js);

			int count = json_array_get_count(jsArray);

			if (0 < count)
			{
				const char **paramValues =
					(const char **) calloc(count, sizeof(char *));

				if (paramValues == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				for (int i = 0; i < count; i++)
				{
					const char *value = json_array_get_string(jsArray, i);
					paramValues[i] = value;
				}

				if (!pgsql_execute_prepared(applyPgConn, name,
											count, paramValues,
											NULL, NULL))
				{
					/* errors have already been logged */
					return false;
				}
			}


			break;
		}

		case STREAM_ACTION_TRUNCATE:
		{
			/*
			 * We still allow continuedTxn, COMMIT message determines whether
			 * to keep the transaction or abort it.
			 */
			if (!context->reachedStartPos && !context->continuedTxn)
			{
				return true;
			}

			/* chomp the final semi-colon that we added */
			int len = strlen(sql);

			if (sql[len - 1] == ';')
			{
				char *ptr = (char *) sql + len - 1;
				*ptr = '\0';
			}

			/*
			 * Postgres rejects TRUNCATE ONLY on partitioned tables. Mirror
			 * the runtime fix in pgsql_truncate: when the target relation is
			 * partitioned, drop the ONLY keyword. The lookup runs on the
			 * non-pipeline controlPgConn because applyPgConn is in pipeline
			 * mode (which forbids parseFun callbacks; see pgsql_execute_with_params).
			 *
			 * Invariant: stream_write_truncate emits exactly one relation per
			 * statement (LogicalMessageTruncate.table is singular), so the
			 * single-relation regclass lookup below is sufficient. If a future
			 * change ever makes this multi-relation, the regclass cast will
			 * fail, the rewrite will be skipped, and Postgres will surface a
			 * loud error from the original TRUNCATE ONLY.
			 */
			char rewritten[BUFSIZE] = { 0 };
			const char *truncateSQL = sql;
			const char onlyPrefix[] = "TRUNCATE ONLY ";
			size_t prefixLen = sizeof(onlyPrefix) - 1;

			if (strncmp(sql, onlyPrefix, prefixLen) == 0)
			{
				char relkind = '\0';
				const char *qname = sql + prefixLen;

				if (pgsql_get_table_relkind(&(context->controlPgConn),
											qname, &relkind))
				{
					if (relkind == 'p')
					{
						sformat(rewritten, sizeof(rewritten),
								"TRUNCATE %s", qname);
						truncateSQL = rewritten;
					}
				}
				else
				{
					log_warn("Could not resolve relkind for replicated "
							 "TRUNCATE on \"%s\"; replaying as TRUNCATE ONLY. "
							 "If the target is partitioned, Postgres will "
							 "reject this statement.",
							 qname);
				}
			}

			if (!pgsql_execute(applyPgConn, truncateSQL))
			{
				/* errors have already been logged */
				return false;
			}
			break;
		}

		default:
		{
			log_error("Failed to parse action %c for SQL query: %s",
					  metadata->action,
					  sql);

			return false;
		}
	}

	return true;
}


/*
 * TargetRelkindContext is the callback context for the pg_class query that
 * loads matviews ('m') and partitioned tables ('p') from the target database.
 */
typedef struct TargetRelkindContext
{
	char sqlstate[6];

	StreamApplyContext *applyContext;
	bool parsedOk;
} TargetRelkindContext;


/*
 * parseTargetRelkinds is the pgsql_execute_with_params callback that processes
 * each row from the pg_class query: upserts into the target SQLite s_rel
 * catalog and adds to the in-memory TargetRelkind hash.
 */
static void
parseTargetRelkinds(void *ctx, PGresult *result)
{
	TargetRelkindContext *context = (TargetRelkindContext *) ctx;
	StreamApplyContext *applyContext = context->applyContext;

	int nTuples = PQntuples(result);

	for (int i = 0; i < nTuples; i++)
	{
		char *nspname = PQgetvalue(result, i, 0);
		char *relname = PQgetvalue(result, i, 1);
		char *relkindStr = PQgetvalue(result, i, 2);
		char *ispopstr = PQgetvalue(result, i, 3);
		char relkind = relkindStr[0];
		bool ispopulated = (ispopstr != NULL && ispopstr[0] == 't');

		/* populate SQLite target catalog when available */
		if (applyContext->targetDB != NULL &&
			applyContext->targetDB->db != NULL)
		{
			if (!catalog_upsert_target_s_rel(applyContext->targetDB,
											 nspname, relname,
											 relkind, ispopulated))
			{
				log_warn("Failed to cache target relation \"%s\".\"%s\" "
						 "(relkind '%c') in SQLite",
						 nspname, relname, relkind);
			}
		}

		/* always populate the in-memory hash */
		TargetRelkind *entry =
			(TargetRelkind *) calloc(1, sizeof(TargetRelkind));

		if (entry == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			context->parsedOk = false;
			return;
		}

		strlcpy(entry->nspname, nspname, sizeof(entry->nspname));
		strlcpy(entry->relname, relname, sizeof(entry->relname));
		entry->relkind = relkind;

		unsigned keylen =
			offsetof(TargetRelkind, relname) +
			sizeof(entry->relname) -
			offsetof(TargetRelkind, nspname);

		HASH_ADD(hh, applyContext->targetRelkindCache, nspname, keylen, entry);
	}

	context->parsedOk = true;
}


/*
 * stream_apply_load_target_relkinds queries the target database for all
 * matviews ('m') and partitioned tables ('p'), persists them into the target
 * SQLite catalog's s_rel table, and builds the in-memory TargetRelkind hash
 * used during DML apply for REFRESH interception and TRUNCATE rewriting.
 */
static bool
stream_apply_load_target_relkinds(StreamApplyContext *context)
{
	/* open/init the target SQLite catalog when provided */
	if (context->targetDB != NULL && context->targetDB->db == NULL)
	{
		if (!catalog_init(context->targetDB))
		{
			log_warn("Failed to open target schema catalog; "
					 "relkind cache will be in-memory only");
		}
	}

	char *sql =
		"  select n.nspname, c.relname, c.relkind, c.relispopulated "
		"    from pg_class c "
		"    join pg_namespace n on n.oid = c.relnamespace "
		"   where c.relkind in ('m', 'p') "
		"     and n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')"
		"   order by n.nspname, c.relname";

	TargetRelkindContext relkindContext = {
		.applyContext = context,
		.parsedOk = false,
	};

	if (!pgsql_execute_with_params(&(context->controlPgConn), sql,
								   0, NULL, NULL,
								   &relkindContext, &parseTargetRelkinds))
	{
		log_error("Failed to query target database for relkind metadata");
		return false;
	}

	if (!relkindContext.parsedOk)
	{
		log_error("Failed to cache target relkind metadata");
		return false;
	}

	int count = HASH_COUNT(context->targetRelkindCache);

	if (count > 0)
	{
		log_info("Loaded %d target relation(s) into relkind cache "
				 "(matviews and partitioned tables)",
				 count);
	}
	else
	{
		log_debug("No matviews or partitioned tables found on target; "
				  "REFRESH interception and TRUNCATE rewriting are inactive");
	}

	return true;
}


/*
 * setupConnection sets up a connection to the target database.
 */
static bool
setupConnection(PGSQL *pgsql, StreamApplyContext *context)
{
	if (!pgsql_init(pgsql,
					context->connStrings->target_pguri,
					PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	/* we're going to send several replication origin commands */
	pgsql->connectionStatementType = PGSQL_CONNECTION_MULTI_STATEMENT;

	/* we also might want to skip logging any SQL query that we apply */
	pgsql->logSQL = context->logSQL;

	/*
	 * Grab the Postgres server version on the target, we need to know that for
	 * being able to call pgsql_current_wal_insert_lsn using the right Postgres
	 * function name.
	 */
	if (!pgsql_server_version(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * setupReplicationOrigin ensures that a replication origin has been created on
 * the target database, and if it has been created previously then fetches the
 * previous LSN position it was at.
 *
 * Also setupReplicationOrigin calls pg_replication_origin_setup() in the
 * current connection.
 */
bool
setupReplicationOrigin(StreamApplyContext *context)
{
	char *nodeName = context->origin;

	/*
	 * A dedicated connection to apply logical messages into the target.
	 * This will be converted to pipeline mode after we have setup the
	 * replication origin.
	 */
	PGSQL *applyPgConn = &(context->applyPgConn);
	if (!setupConnection(applyPgConn, context))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Establish a regular connection for operations requiring immediate
	 * responses, such as finding the WAL insert LSN.
	 */
	if (!setupConnection(&context->controlPgConn, context))
	{
		log_error("Failed to setup pipeline mode on target connection");
		return false;
	}

	uint32_t oid = 0;

	if (!pgsql_replication_origin_oid(applyPgConn, nodeName, &oid))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("setupReplicationOrigin: oid == %u", oid);

	if (oid == 0)
	{
		log_error("Failed to fetch progress for replication origin \"%s\": "
				  "replication origin not found on target database",
				  nodeName);
		(void) pgsql_finish(applyPgConn);
		(void) pgsql_finish(&context->controlPgConn);
		return false;
	}

	/*
	 * Fetch the replication origin LSN tracking, which is maintained in a
	 * transactional fashion with the SQL that's been replayed. It's the
	 * authoritative value for progress at reconnect, given that we use
	 * synchronous_commit off.
	 */
	uint64_t originLSN = InvalidXLogRecPtr;

	if (!pgsql_replication_origin_progress(applyPgConn, nodeName, true, &originLSN))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * The context->previousLSN may have been initialized already from the
	 * sentinel, when restarting a follow operation. For more details see
	 * function stream_apply_wait_for_sentinel().
	 */
	if (context->previousLSN == InvalidXLogRecPtr)
	{
		log_info("Setting up previous LSN from "
				 "replication origin \"%s\" progress at %X/%X",
				 nodeName,
				 LSN_FORMAT_ARGS(originLSN));

		context->previousLSN = originLSN;
	}
	else if (context->previousLSN != originLSN)
	{
		log_info("Setting up previous LSN from "
				 "replication origin \"%s\" progress at %X/%X, "
				 "overriding previous value %X/%X",
				 nodeName,
				 LSN_FORMAT_ARGS(originLSN),
				 LSN_FORMAT_ARGS(context->previousLSN));

		context->previousLSN = originLSN;
	}

	log_debug("setupReplicationOrigin: replication origin \"%s\" "
			  "found at %X/%X",
			  nodeName,
			  LSN_FORMAT_ARGS(context->previousLSN));

	if (!pgsql_replication_origin_session_setup(applyPgConn, nodeName))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Enter into pipeline mode, SQL statements which expects sync responses
	 * are not allowed in this connection anymore.
	 */
	if (!pgsql_enable_pipeline_mode(applyPgConn))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * stream_apply_init_context initializes our context from pieces.
 */
bool
stream_apply_init_context(StreamApplyContext *context,
						  DatabaseCatalog *sourceDB,
						  DatabaseCatalog *replayDB,
						  CDCPaths *paths,
						  ConnStrings *connStrings,
						  char *origin,
						  uint64_t endpos)
{
	context->sourceDB = sourceDB;
	context->paths = *paths;

	/*
	 * We have to consider both the --endpos command line option and the
	 * pgcopydb sentinel endpos value. Typically the sentinel is updated after
	 * the fact, but we still give precedence to --endpos.
	 *
	 * The endpos parameter here comes from the --endpos command line option,
	 * the context->endpos might have been set by calling
	 * stream_apply_wait_for_sentinel() earlier (when in STREAM_MODE_PREFETCH).
	 */
	if (endpos != InvalidXLogRecPtr)
	{
		if (context->endpos != InvalidXLogRecPtr && context->endpos != endpos)
		{
			log_warn("Option --endpos %X/%X is used, "
					 "even when the pgcopydb sentinel endpos was set to %X/%X",
					 LSN_FORMAT_ARGS(endpos),
					 LSN_FORMAT_ARGS(context->endpos));
		}
		context->endpos = endpos;
	}

	context->reachedStartPos = false;
	context->continuedTxn = false;
	context->reachedEOF = false;

	context->connStrings = connStrings;

	strlcpy(context->origin, origin, sizeof(context->origin));

	return true;
}


/*
 * parseSQLAction returns the action that is implemented in the given SQL
 * query.
 */
bool
parseSQLAction(const char *query, LogicalMessageMetadata *metadata)
{
	metadata->action = STREAM_ACTION_UNKNOWN;

	if (strcmp(query, "") == 0)
	{
		return true;
	}

	char *message = NULL;
	char *begin = strstr(query, OUTPUT_BEGIN);
	char *commit = strstr(query, OUTPUT_COMMIT);
	char *rollback = strstr(query, OUTPUT_ROLLBACK);
	char *switchwal = strstr(query, OUTPUT_SWITCHWAL);
	char *keepalive = strstr(query, OUTPUT_KEEPALIVE);
	char *endpos = strstr(query, OUTPUT_ENDPOS);

	/* do we have a BEGIN or a COMMIT message to parse metadata of? */
	if (query == begin)
	{
		metadata->action = STREAM_ACTION_BEGIN;
		message = begin + strlen(OUTPUT_BEGIN);
	}
	else if (query == commit)
	{
		metadata->action = STREAM_ACTION_COMMIT;
		message = commit + strlen(OUTPUT_COMMIT);
	}
	else if (query == rollback)
	{
		metadata->action = STREAM_ACTION_ROLLBACK;
		message = rollback + strlen(OUTPUT_ROLLBACK);
	}
	else if (query == switchwal)
	{
		metadata->action = STREAM_ACTION_SWITCH;
		message = switchwal + strlen(OUTPUT_SWITCHWAL);
	}
	else if (query == keepalive)
	{
		metadata->action = STREAM_ACTION_KEEPALIVE;
		message = keepalive + strlen(OUTPUT_KEEPALIVE);
	}
	else if (query == endpos)
	{
		metadata->action = STREAM_ACTION_ENDPOS;
		message = endpos + strlen(OUTPUT_ENDPOS);
	}

	if (message != NULL)
	{
		JSON_Value *json = json_parse_string(message);

		if (!parseMessageMetadata(metadata, message, json, true))
		{
			/* errors have already been logged */
			return false;
		}


		return true;
	}

	/*
	 * So the SQL Action is a DML (or a TRUNCATE).
	 */
	size_t tLen = sizeof(TRUNCATE) - 1;
	size_t pLen = sizeof(PREPARE) - 1;
	size_t eLen = sizeof(EXECUTE) - 1;

	if (strncmp(query, TRUNCATE, tLen) == 0)
	{
		metadata->action = STREAM_ACTION_TRUNCATE;
	}
	else if (strncmp(query, PREPARE, pLen) == 0)
	{
		char *spc = strchr(query + pLen, ' ');

		if (spc == NULL)
		{
			log_error("Failed to parse PREPARE statement: %s", query);
			return false;
		}

		/* make a copy of just the hexadecimal string */
		int len = spc - (query + pLen);
		char str[BUFSIZE] = { 0 };

		sformat(str, sizeof(str), "%.*s", len, query + pLen);

		uint32_t hash = 0;

		if (!hexStringToUInt32(str, &hash))
		{
			log_error("Failed to parse PREPARE statement name: %s", query);
			return false;
		}

		metadata->hash = hash;

		size_t iLen = sizeof(INSERT) - 1;
		size_t uLen = sizeof(UPDATE) - 1;
		size_t dLen = sizeof(DELETE) - 1;

		if (strncmp(spc + 1, INSERT, iLen) == 0)
		{
			/* skip ' AS ' and point to INSERT */
			metadata->stmt = spc + 1 + 3;
			metadata->action = STREAM_ACTION_INSERT;
		}
		else if (strncmp(spc + 1, UPDATE, uLen) == 0)
		{
			/* skip ' AS ' and point to UPDATE */
			metadata->stmt = spc + 1 + 3;
			metadata->action = STREAM_ACTION_UPDATE;
		}
		else if (strncmp(spc + 1, DELETE, dLen) == 0)
		{
			/* skip ' AS ' and point to DELETE */
			metadata->stmt = spc + 1 + 3;
			metadata->action = STREAM_ACTION_DELETE;
		}
	}
	else if (strncmp(query, EXECUTE, eLen) == 0)
	{
		metadata->action = STREAM_ACTION_EXECUTE;

		char *json = strchr(query + eLen, '[');

		if (json == NULL)
		{
			log_error("Failed to parse EXECUTE statement: %s", query);
			return false;
		}

		/* make a copy of just the hexadecimal string */
		int len = json - (query + eLen);
		char str[BUFSIZE] = { 0 };

		sformat(str, sizeof(str), "%.*s", len, query + pLen);

		uint32_t hash = 0;

		if (!hexStringToUInt32(str, &hash))
		{
			log_error("Failed to parse EXECUTE statement name: %s", query);
			return false;
		}

		metadata->hash = hash;

		/* chomp ; at the end of the query string */
		len = strlen(json) - 1;
		size_t bytes = len + 1;

		metadata->jsonBuffer = (char *) calloc(bytes, sizeof(char));

		if (metadata->jsonBuffer == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		sformat(metadata->jsonBuffer, bytes, "%.*s", len, json);
	}

	if (metadata->action == STREAM_ACTION_UNKNOWN)
	{
		log_error("Failed to parse action from query: %s", query);
		return false;
	}

	return true;
}


/*
 * stream_apply_find_durable_lsn fetches the LSN for the current durable
 * location on the target system using pg_replication_origin_progress.
 */
bool
stream_apply_find_durable_lsn(StreamApplyContext *context, uint64_t *durableLSN)
{
	uint64_t flushLSN = InvalidXLogRecPtr;

	bool flush = true;

	if (!pgsql_replication_origin_progress(&(context->controlPgConn),
										   context->origin,
										   flush,
										   &flushLSN))
	{
		/* errors have already been logged */
		log_error("Failed to retrieve origin progress, "
				  "see above for details");
		return false;
	}

	*durableLSN = flushLSN;

	return true;
}


/*
 * readTxnCommitLSN ensures metadata has transaction COMMIT LSN by fetching it
 * from metadata file if it is not present
 */
static bool
readTxnCommitLSN(LogicalMessageMetadata *metadata,
				 const char *dir,
				 bool *txnCommitLSNFound)
{
	/* if txnCommitLSN is invalid, then fetch it from txn metadata file */
	if (metadata->txnCommitLSN != InvalidXLogRecPtr)
	{
		*txnCommitLSNFound = true;
		return true;
	}

	char txnfilename[MAXPGPATH] = { 0 };

	if (!computeTxnMetadataFilename(metadata->xid,
									dir,
									txnfilename))
	{
		/* errors have already been logged */
		return false;
	}

	if (!file_exists(txnfilename))
	{
		*txnCommitLSNFound = false;
		return true;
	}

	log_debug("stream_apply_sql: BEGIN message without a commit LSN, "
			  "fetching commit LSN from transaction metadata file \"%s\"",
			  txnfilename);

	LogicalMessageMetadata txnMetadata = { .xid = metadata->xid };

	if (!parseTxnMetadataFile(txnfilename, &txnMetadata))
	{
		/* errors have already been logged */
		return false;
	}

	*txnCommitLSNFound = true;
	metadata->txnCommitLSN = txnMetadata.txnCommitLSN;

	return true;
}


/*
 * parseTxnMetadataFile returns the transaction metadata content for the given
 * metadata filename.
 */
static bool
parseTxnMetadataFile(const char *filename, LogicalMessageMetadata *metadata)
{
	/* store xid as it will be overwritten while parsing metadata */
	uint32_t xid = metadata->xid;

	if (xid == 0)
	{
		log_error("BUG: parseTxnMetadataFile is called with "
				  "transaction xid: %lld", (long long) xid);
		return false;
	}

	char *txnMetadataContent = NULL;
	long size = 0L;

	if (!read_file(filename, &txnMetadataContent, &size))
	{
		/* errors have already been logged */
		return false;
	}

	JSON_Value *json = json_parse_string(txnMetadataContent);

	if (!parseMessageMetadata(metadata, txnMetadataContent, json, true))
	{
		/* errors have already been logged */
		return false;
	}


	if (metadata->txnCommitLSN == InvalidXLogRecPtr ||
		metadata->xid != xid ||
		IS_EMPTY_STRING_BUFFER(metadata->timestamp))
	{
		log_error("Failed to parse metadata for transaction metadata file "
				  "\"%s\": %s", filename, txnMetadataContent);
		return false;
	}

	return true;
}


/*
 *  computeTxnMetadataFilename computes the file path for transaction metadata
 *  based on its transaction id
 */
static bool
computeTxnMetadataFilename(uint32_t xid, const char *dir, char *filename)
{
	if (dir == NULL)
	{
		log_error("BUG: computeTxnMetadataFilename is called with "
				  "directory: NULL");
		return false;
	}

	if (xid == 0)
	{
		log_error("BUG: computeTxnMetadataFilename is called with "
				  "transaction xid: %lld", (long long) xid);
		return false;
	}

	sformat(filename, MAXPGPATH, "%s/%lld.json", dir, (long long) xid);

	return true;
}


/*
 * writeTxnCommitMetadata writes the transaction metadata to a file in the given
 * directory
 */
static bool
writeTxnCommitMetadata(LogicalMessageMetadata *mesg, const char *dir)
{
	char txnfilename[MAXPGPATH] = { 0 };

	if (mesg->action != STREAM_ACTION_COMMIT)
	{
		log_error("BUG: writeTxnCommitMetadata is called with "
				  "action: %s", StreamActionToString(mesg->action));
		return false;
	}

	if (!computeTxnMetadataFilename(mesg->xid, dir, txnfilename))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("stream_write_commit_metadata_file: writing transaction "
			  "metadata file \"%s\" with commit lsn %X/%X",
			  txnfilename,
			  LSN_FORMAT_ARGS(mesg->lsn));

	char contents[BUFSIZE] = { 0 };

	sformat(contents, BUFSIZE,
			"{\"xid\":%lld,\"commit_lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
			(long long) mesg->xid,
			LSN_FORMAT_ARGS(mesg->lsn),
			mesg->timestamp);

	/* write the metadata to txnfilename */
	if (!write_file(contents, strlen(contents), txnfilename))
	{
		log_error("Failed to write file \"%s\"", txnfilename);
		return false;
	}

	return true;
}


/*
 * stream_apply_to_stdout reads the replayDB stmt + replay tables and writes
 * the SQL that would normally be sent to the target Postgres to the given
 * FILE * (typically stdout).
 *
 * This mirrors pg_restore --no-target behaviour: useful for debugging and for
 * unit-testing the apply read path without an actual database connection.
 *
 * Output format per transaction:
 *
 *   BEGIN
 *   PREPARE <stmt_name> AS <sql_template>;
 *   EXECUTE <stmt_name> (arg1, arg2, ...);
 *   ...
 *   COMMIT
 */
bool
stream_apply_to_stdout(StreamSpecs *specs, FILE *out)
{
	DatabaseCatalog *replayDB = specs->replayDB;

	if (replayDB == NULL || replayDB->db == NULL)
	{
		log_error("BUG: stream_apply_to_stdout: replayDB is NULL");
		return false;
	}

	if (!semaphore_lock(&(replayDB->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	ReplayDBReplayIterator iter = { 0 };

	iter.catalog = replayDB;
	iter.previousLSN = InvalidXLogRecPtr;   /* start from the beginning */
	iter.endpos = specs->endpos;

	if (!ld_store_iter_replay_init(&iter))
	{
		(void) semaphore_unlock(&(replayDB->sema));
		return false;
	}

	bool inTxn = false;
	char prevHash[16] = { 0 };   /* track which stmts have been PREPAREd */

	/*
	 * We use a simple in-memory hash set to avoid printing the same PREPARE
	 * more than once.  For the expected number of distinct statements per
	 * unit-test (< 10) a linear scan is fast enough.
	 */
#define MAX_PREPARED 64
	uint32_t preparedHashes[MAX_PREPARED];
	int preparedCount = 0;
	memset(preparedHashes, 0, sizeof(preparedHashes));

	(void) prevHash;   /* suppress unused-variable warning */

	for (;;)
	{
		if (!ld_store_iter_replay_next(&iter))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(replayDB->sema));
			return false;
		}

		ReplayDBStmt *s = iter.current;

		if (s == NULL)
		{
			break;  /* no more rows */
		}

		switch (s->action)
		{
			case STREAM_ACTION_BEGIN:
			{
				fformat(out, "BEGIN\n");
				inTxn = true;
				break;
			}

			case STREAM_ACTION_COMMIT:
			{
				fformat(out, "COMMIT\n");
				inTxn = false;

				/* stop at endpos */
				if (specs->endpos != InvalidXLogRecPtr &&
					specs->endpos <= s->lsn)
				{
					goto done;
				}

				break;
			}

			case STREAM_ACTION_ROLLBACK:
			{
				fformat(out, "ROLLBACK\n");
				inTxn = false;
				break;
			}

			case STREAM_ACTION_INSERT:
			case STREAM_ACTION_UPDATE:
			case STREAM_ACTION_DELETE:
			case STREAM_ACTION_TRUNCATE:
			{
				if (s->stmt == NULL || s->data == NULL)
				{
					/* no SQL template or args — skip */
					break;
				}

				/* PREPARE once per distinct hash */
				bool alreadyPrepared = false;

				for (int i = 0; i < preparedCount; i++)
				{
					if (preparedHashes[i] == s->hash)
					{
						alreadyPrepared = true;
						break;
					}
				}

				if (!alreadyPrepared)
				{
					char name[NAMEDATALEN] = { 0 };
					sformat(name, sizeof(name), "%x", s->hash);

					fformat(out, "PREPARE %s AS %s;\n", name, s->stmt);

					if (preparedCount < MAX_PREPARED)
					{
						preparedHashes[preparedCount++] = s->hash;
					}
				}

				/* EXECUTE with args */
				char name[NAMEDATALEN] = { 0 };
				sformat(name, sizeof(name), "%x", s->hash);

				/*
				 * Parse the JSON array of argument values and format them as
				 * a comma-separated list: EXECUTE name ('v1', 'v2', NULL, ...)
				 */
				JSON_Value *js = json_parse_string(s->data);

				if (js == NULL ||
					json_value_get_type(js) != JSONArray)
				{
					log_error("stream_apply_to_stdout: failed to parse "
							  "args JSON for replay id %lld: %s",
							  (long long) s->id, s->data);
					json_value_free(js);
					(void) semaphore_unlock(&(replayDB->sema));
					return false;
				}

				JSON_Array *jsArray = json_value_get_array(js);
				int count = json_array_get_count(jsArray);

				fformat(out, "EXECUTE %s (", name);

				for (int i = 0; i < count; i++)
				{
					const char *val = json_array_get_string(jsArray, i);

					if (i > 0)
					{
						fformat(out, ", ");
					}

					if (val == NULL)
					{
						fformat(out, "NULL");
					}
					else
					{
						fformat(out, "'%s'", val);
					}
				}

				fformat(out, ")\n");

				json_value_free(js);
				break;
			}

			default:
			{
				/* KEEPALIVE, SWITCH, ENDPOS: skip */
				break;
			}
		}
	}

done:
	if (inTxn)
	{
		/* unterminated transaction at end of file — emit ROLLBACK */
		fformat(out, "ROLLBACK\n");
	}

	(void) semaphore_unlock(&(replayDB->sema));

	if (fflush(out) != 0)
	{
		log_error("stream_apply_to_stdout: fflush failed: %m");
		return false;
	}

	return true;
}


/*
 * stream_apply_stdin reads SQL text from stdin (as produced by
 * `pgcopydb stream transform - -`) and applies each statement to the target
 * database.  This is the legacy Unix-pipe path:
 *
 *   pgcopydb stream receive --to-stdout  \
 *     | pgcopydb stream transform - -    \
 *     | pgcopydb stream apply -
 *
 * Each line is parsed with parseSQLAction to determine the action, then
 * handed to stream_apply_sql.  The transform step always includes
 * "commit_lsn" in BEGIN lines, so readTxnCommitLSN never needs to read
 * a .txnmeta file.
 */
bool
stream_apply_stdin(StreamSpecs *specs, StreamApplyContext *context)
{
	char line[BUFSIZE * 16];

	while (fgets(line, sizeof(line), stdin) != NULL)
	{
		/* strip trailing newline */
		int len = strlen(line);

		if (len > 0 && line[len - 1] == '\n')
		{
			line[--len] = '\0';
		}

		if (len == 0)
		{
			continue;
		}

		LogicalMessageMetadata metadata = { 0 };

		if (!parseSQLAction(line, &metadata))
		{
			log_error("stream_apply_stdin: failed to parse line: %s", line);
			return false;
		}

		if (metadata.action == STREAM_ACTION_UNKNOWN)
		{
			/* not a recognised control line — skip silently */
			continue;
		}

		if (!stream_apply_sql(context, &metadata, line))
		{
			/* errors have already been logged */
			return false;
		}

		if (context->reachedEndPos)
		{
			log_info("stream_apply_stdin: reached end position %X/%X",
					 LSN_FORMAT_ARGS(context->endpos));
			break;
		}
	}

	return true;
}
