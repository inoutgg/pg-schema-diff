package diff

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestArchivalSameNameReplacementAndCleanup(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	mustExecArchivalTest(t, db.ConnPool, `
		CREATE TABLE accounts (id bigint PRIMARY KEY, payload text);
		INSERT INTO accounts VALUES (1, 'retained');
	`)

	plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource([]string{`
		CREATE TABLE accounts (id bigint PRIMARY KEY, payload text) PARTITION BY HASH (id);
	`}), WithTempDbFactory(factory), WithRandReader(strings.NewReader("12345678")))
	require.NoError(t, err)
	require.NotEmpty(t, plan.CleanupStatements)
	archiveIndex, createIndex := -1, -1
	for index, statement := range plan.Statements {
		if strings.Contains(statement.DDL, "SET SCHEMA") {
			archiveIndex = index
		}
		if strings.HasPrefix(statement.DDL, "CREATE TABLE \"public\".\"accounts\"") {
			createIndex = index
		}
		_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err)
	}
	assert.Greater(t, createIndex, archiveIndex)

	var archivedRows int
	require.NoError(t, db.ConnPool.QueryRow(t.Context(), `
		SELECT count(*)
		FROM pgschemadiff_archive_3132333435363738.accounts
	`).Scan(&archivedRows))
	assert.Equal(t, 1, archivedRows)
	for _, statement := range plan.CleanupStatements {
		_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err)
	}
}

func TestArchivalCompleteNestedPartitionTree(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	mustExecArchivalTest(t, db.ConnPool, `
		CREATE TABLE events (tenant_id bigint, happened_at date) PARTITION BY LIST (tenant_id);
		CREATE TABLE events_tenant_1 PARTITION OF events FOR VALUES IN (1) PARTITION BY RANGE (happened_at);
		CREATE TABLE events_tenant_1_2026 PARTITION OF events_tenant_1
			FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
		INSERT INTO events VALUES (1, '2026-07-01');
	`)

	plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(nil),
		WithTempDbFactory(factory), WithRandReader(strings.NewReader("abcdefgh")))
	require.NoError(t, err)
	require.Len(t, plan.Statements, 1)
	assert.Less(t, strings.Index(plan.Statements[0].DDL,
		`LOCK TABLE ONLY "public"."events_tenant_1_2026"`),
		strings.Index(plan.Statements[0].DDL, `LOCK TABLE ONLY "public"."events"`))
	_, err = db.ConnPool.Exec(t.Context(), plan.Statements[0].ToSQL())
	require.NoError(t, err)

	var rows int
	require.NoError(t, db.ConnPool.QueryRow(t.Context(), `
		SELECT count(*) FROM pgschemadiff_archive_6162636465666768.events
	`).Scan(&rows))
	assert.Equal(t, 1, rows)
	for _, statement := range plan.CleanupStatements {
		_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err)
	}
	var archiveExists bool
	require.NoError(t, db.ConnPool.QueryRow(t.Context(), `
		SELECT to_regnamespace('pgschemadiff_archive_6162636465666768') IS NOT NULL
	`).Scan(&archiveExists))
	assert.False(t, archiveExists)
}

func TestArchivalCleanupRejectsReplacedRoot(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	mustExecArchivalTest(t, db.ConnPool, `CREATE TABLE removed (id bigint);`)
	plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(nil),
		WithTempDbFactory(factory), WithRandReader(strings.NewReader("rootstale")))
	require.NoError(t, err)
	applyArchivalStatements(t, db.ConnPool, plan.Statements)
	mustExecArchivalTest(t, db.ConnPool, `
		DROP TABLE pgschemadiff_archive_726f6f747374616c.removed;
		CREATE TABLE pgschemadiff_archive_726f6f747374616c.removed (id bigint);
	`)
	_, err = db.ConnPool.Exec(t.Context(), plan.CleanupStatements[0].ToSQL())
	require.ErrorContains(t, err, "archival relation OIDs changed")
}

func TestArchivalCleanupRejectsExternalPartition(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	mustExecArchivalTest(t, db.ConnPool, `
		CREATE TABLE removed (id bigint) PARTITION BY RANGE (id);
		CREATE TABLE removed_1 PARTITION OF removed FOR VALUES FROM (0) TO (10);
	`)
	plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(nil),
		WithTempDbFactory(factory), WithRandReader(strings.NewReader("partdrft")))
	require.NoError(t, err)
	applyArchivalStatements(t, db.ConnPool, plan.Statements)
	mustExecArchivalTest(t, db.ConnPool, `
		CREATE TABLE public.injected_partition
		PARTITION OF pgschemadiff_archive_7061727464726674.removed
		FOR VALUES FROM (10) TO (20);
	`)
	_, err = db.ConnPool.Exec(t.Context(), plan.CleanupStatements[0].ToSQL())
	require.ErrorContains(t, err, "external partition edge")
}

func TestArchivalSameNameSerialReplacement(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	mustExecArchivalTest(t, db.ConnPool, `
		CREATE TABLE serial_accounts (id serial PRIMARY KEY, bucket integer NOT NULL);
		INSERT INTO serial_accounts (bucket) VALUES (1);
	`)
	plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource([]string{`
		CREATE TABLE serial_accounts (
			id serial,
			bucket integer NOT NULL,
			PRIMARY KEY (id, bucket)
		) PARTITION BY HASH (bucket);
	`}), WithTempDbFactory(factory), WithRandReader(strings.NewReader("serialxx")))
	require.NoError(t, err)
	archiveIndex, sequenceIndex, tableIndex := -1, -1, -1
	for index, statement := range plan.Statements {
		switch {
		case strings.Contains(statement.DDL, "SET SCHEMA"):
			archiveIndex = index
		case strings.HasPrefix(statement.DDL, `CREATE SEQUENCE "public"."serial_accounts_id_seq"`):
			sequenceIndex = index
		case strings.HasPrefix(statement.DDL, `CREATE TABLE "public"."serial_accounts"`):
			tableIndex = index
		}
	}
	assert.Less(t, archiveIndex, sequenceIndex)
	assert.Less(t, sequenceIndex, tableIndex)
	applyArchivalStatements(t, db.ConnPool, plan.Statements)
	var activeSequence, archivedSequence string
	require.NoError(t, db.ConnPool.QueryRow(t.Context(), `
		SELECT pg_get_serial_sequence('public.serial_accounts', 'id'),
		       pg_get_serial_sequence('pgschemadiff_archive_73657269616c7878.serial_accounts', 'id')
	`).Scan(&activeSequence, &archivedSequence))
	assert.Equal(t, "public.serial_accounts_id_seq", activeSequence)
	assert.Equal(t, "pgschemadiff_archive_73657269616c7878.serial_accounts_id_seq", archivedSequence)
}

func TestArchivalExistingGroupValidationWithSchemaFilters(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	mustExecArchivalTest(t, db.ConnPool, `CREATE TABLE removed (id bigint);`)
	plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(nil),
		WithTempDbFactory(factory), WithRandReader(strings.NewReader("filtersx")))
	require.NoError(t, err)
	applyArchivalStatements(t, db.ConnPool, plan.Statements)

	for _, option := range []PlanOpt{
		WithIncludeSchemaPatterns("public"),
		WithExcludeSchemaPatterns("pgschemadiff_archive_.*"),
	} {
		filteredPlan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(nil),
			WithTempDbFactory(factory), option)
		require.NoError(t, err)
		assert.Empty(t, filteredPlan.Statements)
		assert.NotEmpty(t, filteredPlan.CleanupStatements)
	}
}

func TestArchivalValidationWithExistingAndNewGroups(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	mustExecArchivalTest(t, db.ConnPool, `CREATE TABLE archive_a (id bigint);`)
	firstPlan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(nil),
		WithTempDbFactory(factory), WithRandReader(strings.NewReader("archivea")))
	require.NoError(t, err)
	applyArchivalStatements(t, db.ConnPool, firstPlan.Statements)

	mustExecArchivalTest(t, db.ConnPool, `CREATE TABLE archive_b (id bigint);`)
	secondPlan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(nil),
		WithTempDbFactory(factory), WithRandReader(strings.NewReader("archiveb")))
	require.NoError(t, err)
	require.Len(t, secondPlan.CleanupStatements, 2)
	require.Len(t, secondPlan.Statements, 1)
	applyArchivalStatements(t, db.ConnPool, secondPlan.Statements)

	var firstExists, secondExists bool
	require.NoError(t, db.ConnPool.QueryRow(t.Context(), `
		SELECT to_regclass('pgschemadiff_archive_6172636869766561.archive_a') IS NOT NULL,
		       to_regclass('pgschemadiff_archive_6172636869766562.archive_b') IS NOT NULL
	`).Scan(&firstExists, &secondExists))
	assert.True(t, firstExists)
	assert.True(t, secondExists)
}

func TestArchivalRevokesNamedDefaultSchemaPrivileges(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	roleGuard := factory.LockRoles(t, "archive_reader")
	roleGuard.CreateRoles()
	db := factory.CreateDatabase(t)
	mustExecArchivalTest(t, db.ConnPool, `
		ALTER DEFAULT PRIVILEGES GRANT USAGE ON SCHEMAS TO archive_reader;
		CREATE TABLE removed (id bigint);
		INSERT INTO removed VALUES (1);
	`)
	plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(nil),
		WithTempDbFactory(factory), WithRandReader(strings.NewReader("aclguard")))
	require.NoError(t, err)
	applyArchivalStatements(t, db.ConnPool, plan.Statements)
	var hasUsage bool
	require.NoError(t, db.ConnPool.QueryRow(t.Context(), `
		SELECT has_schema_privilege('archive_reader', 'pgschemadiff_archive_61636c6775617264', 'USAGE')
	`).Scan(&hasUsage))
	assert.False(t, hasUsage)
	conn, err := db.ConnPool.Acquire(t.Context())
	require.NoError(t, err)
	defer conn.Release()
	_, err = conn.Exec(t.Context(), `SET ROLE archive_reader`)
	require.NoError(t, err)
	var count int
	err = conn.QueryRow(t.Context(), `
		SELECT count(*) FROM pgschemadiff_archive_61636c6775617264.removed
	`).Scan(&count)
	require.ErrorContains(t, err, "permission denied")
	_, err = conn.Exec(t.Context(), `RESET ROLE`)
	require.NoError(t, err)
}

func TestChangedExtensionHiddenTableMemberRejectedWithoutValidation(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	mustExecArchivalTest(t, db.ConnPool, `
		CREATE EXTENSION pg_trgm;
		CREATE SCHEMA hidden;
		CREATE TABLE hidden.extension_table (id bigint);
		ALTER EXTENSION pg_trgm ADD TABLE hidden.extension_table;
	`)
	_, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(nil),
		WithTempDbFactory(factory), WithDoNotValidatePlan())
	require.ErrorContains(t, err, "owns hidden table-like member hidden.extension_table")
}

func TestChangedArchivedTableFunctionDependencyRejected(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name, oldDDL, targetDDL string
	}{
		{
			name: "trigger function",
			oldDDL: `CREATE FUNCTION archive_guard() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RETURN NEW; END $$;
				CREATE TABLE removed (id bigint);
				CREATE TRIGGER archive_guard BEFORE INSERT ON removed
				FOR EACH ROW EXECUTE FUNCTION archive_guard();`,
			targetDDL: `CREATE FUNCTION archive_guard() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'changed'; END $$;`,
		},
		{
			name: "policy function",
			oldDDL: `CREATE FUNCTION archive_guard(bigint) RETURNS boolean
				LANGUAGE sql IMMUTABLE RETURN $1 > 0;
				CREATE TABLE removed (id bigint);
				CREATE POLICY archive_policy ON removed USING (archive_guard(id));`,
			targetDDL: `CREATE FUNCTION archive_guard(bigint) RETURNS boolean
				LANGUAGE sql IMMUTABLE RETURN $1 >= 0;`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			factory := testdb.MustNewFactory(t)
			db := factory.CreateDatabase(t)
			mustExecArchivalTest(t, db.ConnPool, testCase.oldDDL)
			_, err := Generate(t.Context(), DBSchemaSource(db.ConnPool),
				DDLSchemaSource([]string{testCase.targetDDL}),
				WithTempDbFactory(factory))
			require.ErrorContains(t, err, "function dependency")
		})
	}
}

func TestArchivalDiscoveryRejectsMalformedMarker(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	mustExecArchivalTest(t, db.ConnPool, `
		CREATE SCHEMA pgschemadiff_archive_0123456789abcdef;
		CREATE TABLE pgschemadiff_archive_0123456789abcdef.old_table (id bigint);
		COMMENT ON SCHEMA pgschemadiff_archive_0123456789abcdef IS '{"version":2,"unknown":true}';
	`)
	_, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(nil),
		WithTempDbFactory(factory))
	require.ErrorContains(t, err, "parsing marker")
}

func TestArchivalFocusedUnsupportedConditions(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name, ddl, errorText string
	}{
		{
			name: "cross-boundary foreign key",
			ddl: `CREATE TABLE parent (id bigint PRIMARY KEY);
				CREATE TABLE removed (parent_id bigint REFERENCES parent(id));`,
			errorText: "cross-boundary foreign keys",
		},
		{
			name: "extended statistics",
			ddl: `CREATE TABLE removed (a bigint, b bigint);
				CREATE STATISTICS removed_stats ON a, b FROM removed;`,
			errorText: "extended statistics",
		},
		{
			name: "publication membership",
			ddl: `CREATE TABLE removed (id bigint);
				CREATE PUBLICATION removed_publication FOR TABLE removed;`,
			errorText: "publication membership",
		},
		{
			name: "enabled event trigger",
			ddl: `CREATE TABLE removed (id bigint);
				CREATE FUNCTION archival_event_trigger() RETURNS event_trigger
				LANGUAGE plpgsql AS $$ BEGIN END $$;
				CREATE EVENT TRIGGER archival_event_trigger
				ON ddl_command_start EXECUTE FUNCTION archival_event_trigger();`,
			errorText: "enabled event triggers",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			factory := testdb.MustNewFactory(t)
			db := factory.CreateDatabase(t)
			mustExecArchivalTest(t, db.ConnPool, testCase.ddl)
			target := []string{"CREATE TABLE parent (id bigint PRIMARY KEY);"}
			if testCase.name != "cross-boundary foreign key" {
				target = nil
			}
			_, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(target),
				WithTempDbFactory(factory))
			require.ErrorContains(t, err, testCase.errorText)
		})
	}
}

func TestArchivalRequiresLiveCurrentSourceOnlyWhenNeeded(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	current := DDLSchemaSource([]string{"CREATE TABLE removed (id bigint);"})
	_, err := Generate(t.Context(), current, DDLSchemaSource(nil), WithTempDbFactory(factory))
	require.ErrorContains(t, err, "live database-backed current schema source")

	plan, err := Generate(t.Context(), current, current, WithTempDbFactory(factory))
	require.NoError(t, err)
	assert.Empty(t, plan.Statements)
}

func mustExecArchivalTest(t *testing.T, pool *pgxpool.Pool, ddl string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), ddl)
	require.NoError(t, err)
}

func applyArchivalStatements(t *testing.T, pool *pgxpool.Pool, statements []Statement) {
	t.Helper()
	for _, statement := range statements {
		_, err := pool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err)
	}
}
