/*
 * src/bin/pgcopydb/ld_depkeys.h
 *	 Dependency keys of a CDC transaction, used by the parallel apply
 *	 scheduler to decide which transactions may run concurrently.
 */

#ifndef LD_DEPKEYS_H
#define LD_DEPKEYS_H

#include <stdbool.h>
#include <stdint.h>

#include "uthash.h"

#include "ld_stream.h"

/* a transaction with more keys than this one is applied as a barrier */
#define DEPKEYS_MAX 1000

/* the keys column of the txn table, for a barrier transaction */
#define DEPKEYS_BARRIER "*"

/*
 * Per-table classification, computed once from the source catalog:
 *
 *  - row level: the table has a primary key and no other unique or
 *    exclusion constraint, so two changes conflict only when they touch the
 *    same primary key value;
 *
 *  - table level: any other table (no primary key, extra unique index,
 *    exclusion constraint, or not found in the catalog), where any two
 *    changes are considered dependent.
 */
/* a unique index on plain columns, other than the primary key */
typedef struct DepUnique
{
	char name[PG_NAMEDATALEN];
	int colCount;
	char (*cols)[PG_NAMEDATALEN];           /* malloc'ed, index order */
} DepUnique;

typedef struct DepTable
{
	char nspname[PG_NAMEDATALEN];            /* compound hash key, part 1 */
	char relname[PG_NAMEDATALEN];            /* compound hash key, part 2 */

	bool rowLevel;
	int pkeyCount;
	char (*pkeys)[PG_NAMEDATALEN];          /* malloc'ed, attnum order */

	int uniqueCount;
	DepUnique *uniques;                     /* malloc'ed */

	UT_hash_handle hh;
} DepTable;

typedef struct DepKey
{
	uint64_t key;
	UT_hash_handle hh;
} DepKey;

typedef struct DepTableName
{
	char *qname;                /* malloc'ed: nspname.relname */
	UT_hash_handle hh;
} DepTableName;

typedef struct DepKeySet
{
	bool barrier;
	int count;
	DepKey *keys;
	DepTableName *tables;
} DepKeySet;

void depkeys_reset(DepKeySet *set);

bool depkeys_add_statement(StreamContext *privateContext,
						   DepKeySet *set,
						   LogicalTransactionStatement *stmt);

bool depkeys_serialize(DepKeySet *set, char **keys, char **tables);

bool depkeys_parse(const char *keys, uint64_t **array, int *count,
				   bool *barrier);

#endif /* LD_DEPKEYS_H */
