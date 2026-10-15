/*
 * src/bin/pgcopydb/ld_depkeys.c
 *	 Dependency keys of a CDC transaction.
 *
 * The transform process computes, for every transaction, the set of keys it
 * touches and stores it next to the transaction in the replayDB txn table.
 * The apply scheduler then runs two transactions concurrently only when
 * their key sets do not intersect, and runs a barrier transaction alone.
 *
 * Apply runs with session_replication_role = replica, so foreign keys and
 * ordinary triggers do not fire on the target: what remains to order is the
 * changes to a same row, and the unique (and exclusion) constraints. A table
 * with a primary key is keyed per row (table + primary key value). Its other
 * unique indexes, when they are btree indexes on plain columns, add a key per
 * value written (the new tuple of an INSERT or an UPDATE): two transactions
 * that write a same unique value run in source order. Any other unique or
 * exclusion constraint (expression, partial, INCLUDE, NULLS NOT DISTINCT,
 * COLLATE), or no primary key, keys the table as a whole. Whatever cannot be
 * classified with certainty makes the whole transaction a barrier.
 *
 * test_decoding does not send the old value of a unique column that an UPDATE
 * or a DELETE frees, so freeing a value has no key: a later transaction that
 * takes that value may run first, and the target raises a unique violation.
 * The apply scheduler then runs that transaction again alone, once every
 * transaction before it is applied, which is the order of the source. A
 * violation then is an error: apply stops, it never applies out of order.
 */

#include <inttypes.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"

#include "lookup3.h"

#include "catalog.h"
#include "ld_depkeys.h"
#include "log.h"
#include "pg_utils.h"
#include "pgsql.h"
#include "string_utils.h"

static DepTable * depkeys_lookup_table(StreamContext *privateContext,
									   const char *nspname,
									   const char *relname);
static bool depkeys_fetch_count(SQLiteQuery *query);
static bool depkeys_add_key(DepKeySet *set, uint64_t key);
static bool depkeys_add_table_name(DepKeySet *set, const char *nspname,
								   const char *relname);
static uint64_t depkeys_table_key(const char *nspname, const char *relname);
static bool depkeys_add_tuple(DepKeySet *set, DepTable *table,
							  LogicalMessageTuple *tuple, bool optional);
static bool depkeys_add_tuple_array(DepKeySet *set, DepTable *table,
									LogicalMessageTupleArray *tuples,
									bool optional);
static bool depkeys_add_unique_keys(DepKeySet *set, DepTable *table,
									LogicalMessageTupleArray *tuples);
static void depkeys_append_value(PQExpBuffer buf, LogicalMessageValue *v);
static bool depkeys_add_hashed(DepKeySet *set, PQExpBuffer buf);
static bool depkeys_parse_ident(const char **p, char *out, size_t size);
static bool depkeys_parse_unique(const char *sql, DepUnique *unique);


/*
 * depkeys_reset frees the set and makes it empty again.
 */
void
depkeys_reset(DepKeySet *set)
{
	DepKey *k, *ktmp;

	HASH_ITER(hh, set->keys, k, ktmp)
	{
		HASH_DEL(set->keys, k);
		free(k);
	}

	DepTableName *t, *ttmp;

	HASH_ITER(hh, set->tables, t, ttmp)
	{
		HASH_DEL(set->tables, t);
		free(t->qname);
		free(t);
	}

	set->barrier = false;
	set->count = 0;
	set->keys = NULL;
	set->tables = NULL;
}


/*
 * depkeys_add_statement adds the keys of one transaction statement.
 */
bool
depkeys_add_statement(StreamContext *privateContext,
					  DepKeySet *set,
					  LogicalTransactionStatement *stmt)
{
	LogicalMessageRelation *rel = NULL;

	switch (stmt->action)
	{
		case STREAM_ACTION_INSERT:
		{
			rel = &(stmt->stmt.insert.table);
			break;
		}

		case STREAM_ACTION_UPDATE:
		{
			rel = &(stmt->stmt.update.table);
			break;
		}

		case STREAM_ACTION_DELETE:
		{
			rel = &(stmt->stmt.delete.table);
			break;
		}

		case STREAM_ACTION_TRUNCATE:
		{
			/* TRUNCATE may cascade on the target: run it alone */
			set->barrier = true;
			return depkeys_add_table_name(set,
										  stmt->stmt.truncate.table.nspname,
										  stmt->stmt.truncate.table.relname);
		}

		default:
		{
			/* unexpected in a transaction: be safe */
			set->barrier = true;
			return true;
		}
	}

	if (!depkeys_add_table_name(set, rel->nspname, rel->relname))
	{
		/* errors have already been logged */
		return false;
	}

	/* once a barrier, the keys do not matter anymore */
	if (set->barrier)
	{
		return true;
	}

	DepTable *table = depkeys_lookup_table(privateContext,
										   rel->nspname,
										   rel->relname);

	if (table == NULL)
	{
		/* errors have already been logged */
		return false;
	}

	if (!table->rowLevel)
	{
		return depkeys_add_key(set, depkeys_table_key(rel->nspname,
													  rel->relname));
	}

	switch (stmt->action)
	{
		case STREAM_ACTION_INSERT:
		{
			return depkeys_add_tuple_array(set, table, &(stmt->stmt.insert.new),
										   false) &&
				   depkeys_add_unique_keys(set, table, &(stmt->stmt.insert.new));
		}

		case STREAM_ACTION_UPDATE:
		{
			/* the row before (identity) and after (it may change the key) */
			return depkeys_add_tuple_array(set, table, &(stmt->stmt.update.old),
										   false) &&
				   depkeys_add_tuple_array(set, table, &(stmt->stmt.update.new),
										   true) &&
				   depkeys_add_unique_keys(set, table, &(stmt->stmt.update.new));
		}

		case STREAM_ACTION_DELETE:
		{
			return depkeys_add_tuple_array(set, table, &(stmt->stmt.delete.old),
										   false);
		}

		default:
		{
			set->barrier = true;
			return true;
		}
	}
}


/*
 * depkeys_add_tuple_array adds the row keys of every row of every tuple.
 */
static bool
depkeys_add_tuple_array(DepKeySet *set, DepTable *table,
						LogicalMessageTupleArray *tuples, bool optional)
{
	for (int t = 0; t < tuples->count && !set->barrier; t++)
	{
		if (!depkeys_add_tuple(set, table, &(tuples->array[t]), optional))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * depkeys_add_tuple adds one key per row of the tuple: the hash of the table
 * name and of the primary key values. When a primary key column is missing
 * from the tuple (an unchanged TOAST value) the row cannot be identified: the
 * transaction becomes a barrier. Only the new tuple of an UPDATE (optional)
 * may lack the whole key, the old tuple then identifies the row.
 */
static bool
depkeys_add_tuple(DepKeySet *set, DepTable *table, LogicalMessageTuple *tuple,
				  bool optional)
{
	/* an UPDATE where every column is unchanged has an empty new tuple */
	if (tuple->attributes.count == 0 || tuple->values.count == 0)
	{
		return true;
	}

	int *pos = (int *) calloc(table->pkeyCount, sizeof(int));

	if (pos == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/* the primary key columns in this tuple, absent or not */
	int found = 0;

	for (int k = 0; k < table->pkeyCount; k++)
	{
		pos[k] = -1;

		for (int c = 0; c < tuple->attributes.count; c++)
		{
			if (streq(tuple->attributes.array[c].attname, table->pkeys[k]))
			{
				pos[k] = c;
				++found;
				break;
			}
		}
	}

	if (found < table->pkeyCount)
	{
		free(pos);

		/*
		 * The new tuple of an UPDATE may list only the changed columns: when
		 * no primary key column is among them, the old tuple holds the key.
		 * Anything else does not identify the row.
		 */
		if (!(optional && found == 0))
		{
			set->barrier = true;
		}

		return true;
	}

	for (int r = 0; r < tuple->values.count; r++)
	{
		LogicalMessageValues *values = &(tuple->values.array[r]);
		PQExpBuffer buf = createPQExpBuffer();

		appendPQExpBuffer(buf, "R|%s.%s", table->nspname, table->relname);

		for (int k = 0; k < table->pkeyCount; k++)
		{
			if (pos[k] >= values->cols)
			{
				set->barrier = true;
				break;
			}

			depkeys_append_value(buf, &(values->array[pos[k]]));
		}

		if (set->barrier)
		{
			destroyPQExpBuffer(buf);
			break;
		}

		if (!depkeys_add_hashed(set, buf))
		{
			free(pos);
			return false;
		}
	}

	free(pos);

	return true;
}


/*
 * depkeys_add_unique_keys adds a key per unique value the tuples write, for
 * each unique index on plain columns: the hash of the table, the index and
 * the values. A row with a NULL in the index takes no value (NULLs are
 * distinct), and a tuple without every column of the index (an UPDATE that
 * leaves an unchanged TOAST value out) does not change that value.
 */
static bool
depkeys_add_unique_keys(DepKeySet *set, DepTable *table,
						LogicalMessageTupleArray *tuples)
{
	for (int t = 0; t < tuples->count && !set->barrier; t++)
	{
		LogicalMessageTuple *tuple = &(tuples->array[t]);

		for (int u = 0; u < table->uniqueCount && !set->barrier; u++)
		{
			DepUnique *unique = &(table->uniques[u]);
			int pos[INDEX_MAX_KEYS];
			bool complete = true;

			for (int k = 0; k < unique->colCount && complete; k++)
			{
				pos[k] = -1;

				for (int c = 0; c < tuple->attributes.count; c++)
				{
					if (streq(tuple->attributes.array[c].attname,
							  unique->cols[k]))
					{
						pos[k] = c;
						break;
					}
				}

				complete = pos[k] >= 0;
			}

			if (!complete)
			{
				continue;
			}

			for (int r = 0; r < tuple->values.count; r++)
			{
				LogicalMessageValues *values = &(tuple->values.array[r]);
				bool hasNull = false;

				for (int k = 0; k < unique->colCount && !hasNull; k++)
				{
					hasNull = pos[k] >= values->cols ||
							  values->array[pos[k]].isNull;
				}

				if (hasNull)
				{
					continue;
				}

				PQExpBuffer buf = createPQExpBuffer();

				appendPQExpBuffer(buf, "U|%s.%s|%s",
								  table->nspname, table->relname, unique->name);

				for (int k = 0; k < unique->colCount; k++)
				{
					depkeys_append_value(buf, &(values->array[pos[k]]));
				}

				if (!depkeys_add_hashed(set, buf))
				{
					return false;
				}
			}
		}
	}

	return true;
}


/*
 * depkeys_append_value appends one column value to a key being built.
 */
static void
depkeys_append_value(PQExpBuffer buf, LogicalMessageValue *v)
{
	if (v->isNull)
	{
		appendPQExpBufferStr(buf, "|\x01");
		return;
	}

	switch (v->oid)
	{
		case BOOLOID:
		{
			appendPQExpBuffer(buf, "|%c", v->val.boolean ? 't' : 'f');
			break;
		}

		case INT8OID:
		{
			appendPQExpBuffer(buf, "|%" PRIu64, v->val.int8);
			break;
		}

		case FLOAT8OID:
		{
			appendPQExpBuffer(buf, "|%.17g", v->val.float8);
			break;
		}

		default:
		{
			const char *str = v->val.str != NULL ? v->val.str : "";

			appendPQExpBuffer(buf, "|%zu:%s", strlen(str), str);
			break;
		}
	}
}


/*
 * depkeys_add_hashed adds the hash of the key built in buf, and frees buf.
 */
static bool
depkeys_add_hashed(DepKeySet *set, PQExpBuffer buf)
{
	if (PQExpBufferBroken(buf))
	{
		log_error("Failed to compute a dependency key: Out of Memory");
		destroyPQExpBuffer(buf);
		return false;
	}

	uint32_t h1 = 0, h2 = 0;

	hashlittle2(buf->data, buf->len, &h1, &h2);
	destroyPQExpBuffer(buf);

	return depkeys_add_key(set, ((uint64_t) h1 << 32) | h2);
}


/*
 * depkeys_table_key is the key of a whole table.
 */
static uint64_t
depkeys_table_key(const char *nspname, const char *relname)
{
	char buf[2 * PG_NAMEDATALEN + 8] = { 0 };
	uint32_t h1 = 0, h2 = 0;

	int len = sformat(buf, sizeof(buf), "T|%s.%s", nspname, relname);

	hashlittle2(buf, len, &h1, &h2);

	return ((uint64_t) h1 << 32) | h2;
}


/*
 * depkeys_add_key adds a key to the set; past DEPKEYS_MAX keys the
 * transaction is applied as a barrier instead.
 */
static bool
depkeys_add_key(DepKeySet *set, uint64_t key)
{
	DepKey *k = NULL;

	HASH_FIND(hh, set->keys, &key, sizeof(key), k);

	if (k != NULL)
	{
		return true;
	}

	if (set->count >= DEPKEYS_MAX)
	{
		set->barrier = true;
		return true;
	}

	k = (DepKey *) calloc(1, sizeof(DepKey));

	if (k == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	k->key = key;
	HASH_ADD(hh, set->keys, key, sizeof(k->key), k);
	++set->count;

	return true;
}


/*
 * depkeys_add_table_name records the tables a transaction changes, which the
 * apply scheduler checks against the target relkinds.
 */
static bool
depkeys_add_table_name(DepKeySet *set, const char *nspname,
					   const char *relname)
{
	char qname[PG_NAMEDATALEN_FQ] = { 0 };

	sformat(qname, sizeof(qname), "%s.%s", nspname, relname);

	DepTableName *t = NULL;

	HASH_FIND_STR(set->tables, qname, t);

	if (t != NULL)
	{
		return true;
	}

	t = (DepTableName *) calloc(1, sizeof(DepTableName));

	if (t == NULL || (t->qname = strdup(qname)) == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	HASH_ADD_KEYPTR(hh, set->tables, t->qname, strlen(t->qname), t);

	return true;
}


/*
 * depkeys_serialize returns the malloc'ed keys and tables columns of the txn
 * table: keys as space separated hexadecimal values, or DEPKEYS_BARRIER, or
 * NULL when the transaction has no change; tables as newline separated
 * qualified names.
 */
bool
depkeys_serialize(DepKeySet *set, char **keys, char **tables)
{
	*keys = NULL;
	*tables = NULL;

	if (set->barrier)
	{
		*keys = strdup(DEPKEYS_BARRIER);
	}
	else if (set->count > 0)
	{
		PQExpBuffer buf = createPQExpBuffer();
		DepKey *k, *ktmp;

		HASH_ITER(hh, set->keys, k, ktmp)
		{
			appendPQExpBuffer(buf, "%s%" PRIx64, buf->len > 0 ? " " : "", k->key);
		}

		if (PQExpBufferBroken(buf))
		{
			log_error("Failed to serialize dependency keys: Out of Memory");
			destroyPQExpBuffer(buf);
			return false;
		}

		*keys = strdup(buf->data);
		destroyPQExpBuffer(buf);
	}

	if (set->tables != NULL)
	{
		PQExpBuffer buf = createPQExpBuffer();
		DepTableName *t, *ttmp;

		HASH_ITER(hh, set->tables, t, ttmp)
		{
			appendPQExpBuffer(buf, "%s%s", buf->len > 0 ? "\n" : "", t->qname);
		}

		if (PQExpBufferBroken(buf))
		{
			log_error("Failed to serialize table names: Out of Memory");
			destroyPQExpBuffer(buf);
			return false;
		}

		*tables = strdup(buf->data);
		destroyPQExpBuffer(buf);
	}

	return true;
}


/*
 * depkeys_parse parses the keys column of the txn table into a malloc'ed
 * array.
 */
bool
depkeys_parse(const char *keys, uint64_t **array, int *count, bool *barrier)
{
	*array = NULL;
	*count = 0;
	*barrier = false;

	if (keys == NULL || keys[0] == '\0')
	{
		return true;
	}

	if (streq(keys, DEPKEYS_BARRIER))
	{
		*barrier = true;
		return true;
	}

	int n = 1;

	for (const char *p = keys; *p != '\0'; p++)
	{
		n += *p == ' ';
	}

	*array = (uint64_t *) calloc(n, sizeof(uint64_t));

	if (*array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	const char *p = keys;

	while (*p != '\0')
	{
		char *end = NULL;
		uint64_t key = strtoull(p, &end, 16);

		if (end == p)
		{
			log_error("Failed to parse dependency keys \"%s\"", keys);
			free(*array);
			*array = NULL;
			return false;
		}

		(*array)[(*count)++] = key;
		p = (*end == ' ') ? end + 1 : end;
	}

	return true;
}


/*
 * depkeys_lookup_table returns the cached classification of a table,
 * computing it from the source catalog on first use.
 */
static DepTable *
depkeys_lookup_table(StreamContext *privateContext,
					 const char *nspname,
					 const char *relname)
{
	DepTable lookup = { 0 };

	strlcpy(lookup.nspname, nspname, sizeof(lookup.nspname));
	strlcpy(lookup.relname, relname, sizeof(lookup.relname));

	unsigned keylen = offsetof(DepTable, relname) +
					  sizeof(lookup.relname) -
					  offsetof(DepTable, nspname);

	DepTable *item = NULL;

	HASH_FIND(hh, privateContext->depTableCache, &lookup.nspname, keylen, item);

	if (item != NULL)
	{
		return item;
	}

	item = (DepTable *) calloc(1, sizeof(DepTable));

	if (item == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return NULL;
	}

	strlcpy(item->nspname, nspname, sizeof(item->nspname));
	strlcpy(item->relname, relname, sizeof(item->relname));

	DatabaseCatalog *sourceDB = privateContext->sourceDB;
	SourceTable table = { 0 };

	if (sourceDB == NULL || sourceDB->db == NULL ||
		!catalog_lookup_s_table_by_name(sourceDB, nspname, relname, &table) ||
		table.oid == 0)
	{
		log_notice("Table %s.%s is not in the source catalog: its changes "
				   "are applied in the order of the source",
				   nspname, relname);
	}
	else if (!catalog_s_table_fetch_attrs(sourceDB, &table))
	{
		log_error("Failed to fetch table %s attribute list", table.qname);
		free(item);
		return NULL;
	}
	else
	{
		/* exclusion constraints: no value key can tell an overlap */
		char *sql =
			"select count(*) "
			"  from s_constraint c join s_index i on i.oid = c.indexoid "
			" where i.tableoid = $1 and c.sql like '%EXCLUDE%'";

		int64_t others = 0;

		SQLiteQuery query = {
			.context = &others,
			.fetchFunction = &depkeys_fetch_count
		};

		BindParam params[] = {
			{ BIND_PARAMETER_TYPE_INT64, "oid", table.oid, NULL }
		};

		if (!catalog_sql_prepare(sourceDB->db, sql, &query) ||
			!catalog_sql_bind(&query, params, 1) ||
			!catalog_sql_execute_once(&query))
		{
			/* errors have already been logged */
			free(item);
			return NULL;
		}

		/* the other unique indexes: keyed per value when on plain columns */
		char *uniqueSQL =
			"select sql from s_index "
			" where tableoid = $1 and isunique and not isprimary "
			" order by oid";

		SQLiteQuery uq = { 0 };

		if (!catalog_sql_prepare(sourceDB->db, uniqueSQL, &uq) ||
			!catalog_sql_bind(&uq, params, 1))
		{
			free(item);
			return NULL;
		}

		for (;;)
		{
			int rc = catalog_sql_step(&uq);

			if (rc == SQLITE_DONE)
			{
				break;
			}

			if (rc != SQLITE_ROW)
			{
				log_error("Failed to list the unique indexes of %s.%s: %s",
						  nspname, relname, sqlite3_errmsg(sourceDB->db));
				(void) catalog_sql_finalize(&uq);
				free(item);
				return NULL;
			}

			const char *indexdef = (const char *) sqlite3_column_text(uq.ppStmt, 0);
			DepUnique *uniques =
				realloc(item->uniques, (item->uniqueCount + 1) * sizeof(DepUnique));

			if (uniques == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				(void) catalog_sql_finalize(&uq);
				free(item);
				return NULL;
			}

			item->uniques = uniques;

			DepUnique *unique = &(item->uniques[item->uniqueCount]);

			bzero(unique, sizeof(DepUnique));

			if (indexdef != NULL && depkeys_parse_unique(indexdef, unique))
			{
				++item->uniqueCount;
			}
			else
			{
				log_debug("Unique index of %s.%s keys the table as a whole: %s",
						  nspname, relname, indexdef ? indexdef : "");
				++others;
			}
		}

		(void) catalog_sql_finalize(&uq);

		int pkeyCount = 0;

		for (int i = 0; i < table.attributes.count; i++)
		{
			pkeyCount += table.attributes.array[i].attisprimary ? 1 : 0;
		}

		if (pkeyCount > 0 && others == 0)
		{
			item->pkeys = calloc(pkeyCount, sizeof(*item->pkeys));

			if (item->pkeys == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				free(item);
				return NULL;
			}

			/* attributes come in attnum order */
			for (int i = 0; i < table.attributes.count; i++)
			{
				SourceTableAttribute *attr = &(table.attributes.array[i]);

				if (attr->attisprimary)
				{
					strlcpy(item->pkeys[item->pkeyCount++], attr->attname,
							PG_NAMEDATALEN);
				}
			}

			item->rowLevel = true;
		}

		log_debug("Dependency keys of %s.%s are per %s (%d unique indexes "
				  "keyed per value)",
				  nspname, relname, item->rowLevel ? "row" : "table",
				  item->rowLevel ? item->uniqueCount : 0);

		if (!item->rowLevel)
		{
			item->uniqueCount = 0;
		}
	}

	HASH_ADD(hh, privateContext->depTableCache, nspname, keylen, item);

	return item;
}


/*
 * depkeys_parse_ident reads one SQL identifier, quoted or not, unquoted into
 * out, and moves *p past it.
 */
static bool
depkeys_parse_ident(const char **p, char *out, size_t size)
{
	const char *s = *p;
	size_t len = 0;

	if (*s == '"')
	{
		for (++s; *s != '\0'; ++s)
		{
			if (*s == '"')
			{
				if (s[1] != '"')
				{
					break;
				}
				++s;
			}

			if (len + 1 >= size)
			{
				return false;
			}
			out[len++] = *s;
		}

		if (*s != '"' || len == 0)
		{
			return false;
		}

		++s;
	}
	else
	{
		/* what quote_ident leaves unquoted: [a-z_][a-z0-9_]* */
		while ((*s >= 'a' && *s <= 'z') || *s == '_' ||
			   (len > 0 && *s >= '0' && *s <= '9'))
		{
			if (len + 1 >= size)
			{
				return false;
			}
			out[len++] = *s++;
		}

		if (len == 0)
		{
			return false;
		}
	}

	out[len] = '\0';
	*p = s;

	return true;
}


/*
 * depkeys_parse_unique reads the definition of a unique index, as
 * pg_get_indexdef prints it, and keeps the index when it is exactly a btree
 * index on plain columns:
 *
 *   CREATE UNIQUE INDEX name ON [ONLY] table USING btree (col, ...)
 *
 * Anything else (an expression, an operator class, COLLATE, DESC, INCLUDE,
 * NULLS NOT DISTINCT, WHERE) is false: that table is keyed as a whole.
 */
static bool
depkeys_parse_unique(const char *sql, DepUnique *unique)
{
	const char *prefix = "CREATE UNIQUE INDEX ";
	const char *using = " USING btree (";

	if (strncmp(sql, prefix, strlen(prefix)) != 0)
	{
		return false;
	}

	const char *p = sql + strlen(prefix);

	if (!depkeys_parse_ident(&p, unique->name, sizeof(unique->name)) ||
		strncmp(p, " ON ", 4) != 0)
	{
		return false;
	}

	/* the table name may hold anything quoted: the column list follows */
	p = strstr(p, using);

	if (p == NULL)
	{
		return false;
	}

	p += strlen(using);

	char cols[INDEX_MAX_KEYS][PG_NAMEDATALEN];
	int count = 0;

	for (;;)
	{
		if (count >= INDEX_MAX_KEYS ||
			!depkeys_parse_ident(&p, cols[count], PG_NAMEDATALEN))
		{
			return false;
		}

		++count;

		if (strncmp(p, ", ", 2) == 0)
		{
			p += 2;
			continue;
		}

		break;
	}

	if (strcmp(p, ")") != 0)
	{
		return false;
	}

	unique->cols = calloc(count, sizeof(*unique->cols));

	if (unique->cols == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	for (int k = 0; k < count; k++)
	{
		strlcpy(unique->cols[k], cols[k], PG_NAMEDATALEN);
	}

	unique->colCount = count;

	return true;
}


static bool
depkeys_fetch_count(SQLiteQuery *query)
{
	int64_t *count = (int64_t *) query->context;

	*count = sqlite3_column_int64(query->ppStmt, 0);

	return true;
}
