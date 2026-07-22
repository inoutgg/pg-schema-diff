package diff

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

type fakeSchemaSource struct {
	t *testing.T

	expectedDeps schemaSourcePlanDeps
	snapshot     schema.SchemaSnapshot
	err          error
}

func (f fakeSchemaSource) GetSchemaSnapshot(_ context.Context, deps schemaSourcePlanDeps) (schema.SchemaSnapshot, error) {
	assert.Equal(f.t, f.expectedDeps.logger, deps.logger)
	assert.Equal(f.t, f.expectedDeps.tempDBFactory, deps.tempDBFactory)
	// We can't easily compare the function pointers, so we'll just assert the length of the slices.
	assert.Len(f.t, f.expectedDeps.getSchemaOpts, len(deps.getSchemaOpts))
	if f.err != nil {
		return schema.SchemaSnapshot{}, f.err
	}
	return f.snapshot, nil
}

func mustApplyDDLToTestDB(t testing.TB, db *pgxpool.Pool, ddl []string) {
	t.Helper()
	for _, stmt := range ddl {
		_, err := db.Exec(t.Context(), stmt)
		assert.NoError(t, err)
	}
}

func mustApplyMigrationPlan(t testing.TB, db *pgxpool.Pool, plan Plan) {
	t.Helper()
	// Run the migration
	for _, stmt := range plan.Statements {
		_, err := db.Exec(t.Context(), stmt.ToSQL())
		require.NoError(t, err)
	}
}

func TestSimpleMigratorTestSuite(t *testing.T) {
	t.Parallel()

	t.Run("TestGenerate", func(t *testing.T) {
		t.Parallel()
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		initialDDL := `
	CREATE TABLE foobar(
	    id CHAR(16) PRIMARY KEY
    ); `
		newSchemaDDL := `
	CREATE TABLE foobar(
	    id  CHAR(16) PRIMARY KEY,
		new_column VARCHAR(128) NOT NULL
    );
	`

		mustApplyDDLToTestDB(t, db.ConnPool, []string{initialDDL})

		plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool),
			DDLSchemaSource([]string{newSchemaDDL}), WithTempDbFactory(factory))
		assert.NoError(t, err)

		mustApplyMigrationPlan(t, db.ConnPool, plan)
		// Ensure that some sort of migration ran. we're really not testing the correctness of the
		// migration in this test suite
		_, err = db.ConnPool.Exec(t.Context(),
			"SELECT new_column FROM foobar;")
		assert.NoError(t, err)
	})

	t.Run("TestGeneratePlan_SchemaSourceErr", func(t *testing.T) {
		t.Parallel()
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		logger := slog.Default()

		getSchemaOpts := []schema.GetSchemaOpt{
			schema.WithIncludeSchemaPatterns("schema_1"),
			schema.WithIncludeSchemaPatterns("schema_2"),
		}
		expectedGetSchemaOpts := append([]schema.GetSchemaOpt{}, getSchemaOpts...)

		expectedErr := fmt.Errorf("some error")
		fakeSchemaSource := fakeSchemaSource{
			t: t,
			expectedDeps: schemaSourcePlanDeps{
				tempDBFactory: factory,
				logger:        logger,
				getSchemaOpts: expectedGetSchemaOpts,
			},
			err: expectedErr,
		}

		_, err := Generate(
			t.Context(), DBSchemaSource(db.ConnPool), fakeSchemaSource,
			WithTempDbFactory(factory),
			WithGetSchemaOpts(getSchemaOpts...),
			WithLogger(logger),
		)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("TestGenerate_CannotBuildMigrationFromDDLWithoutTempDbFactory", func(t *testing.T) {
		t.Parallel()
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		_, err := Generate(
			t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource([]string{``}),
			WithIncludeSchemaPatterns("public"),
			WithDoNotValidatePlan(),
		)
		assert.ErrorContains(t, err, "tempDbFactory is required")
	})

	t.Run("TestGenerate_CannotValidateWithoutTempDbFactory", func(t *testing.T) {
		t.Parallel()
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		_, err := Generate(
			t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource([]string{``}),
			WithIncludeSchemaPatterns("public"),
			WithDoNotValidatePlan(),
		)
		assert.ErrorContains(t, err, "tempDbFactory is required")
	})
}

func TestValidateSchemaPartialArchivalPrefix(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		prefix      string
		expectError bool
	}{
		{name: "default", prefix: defaultSchemaPartialArchivalPrefix},
		{name: "custom", prefix: "deleted"},
		{name: "empty", prefix: "", expectError: true},
		{name: "not simple", prefix: "deleted-schema", expectError: true},
		{name: "reserved pg", prefix: "pg", expectError: true},
		{name: "reserved pg prefix", prefix: "pg_deleted", expectError: true},
		{name: "too long", prefix: "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstu", expectError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSchemaPartialArchivalPrefix(tc.prefix)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGenerateDoesNotTrustUnmarkedPrefixSchemas(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		prefix       string
		schemaPrefix string
		inTarget     bool
	}{
		{name: "default prefix", prefix: defaultSchemaPartialArchivalPrefix},
		{name: "custom prefix", prefix: "deleted"},
		{
			name:         "custom prefix replaces default exclusion",
			prefix:       "deleted",
			schemaPrefix: defaultSchemaPartialArchivalPrefix,
		},
		{name: "target schema", prefix: defaultSchemaPartialArchivalPrefix, inTarget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			factory := testdb.MustNewFactory(t)
			db := factory.CreateDatabase(t)
			schemaPrefix := tc.schemaPrefix
			if schemaPrefix == "" {
				schemaPrefix = tc.prefix
			}
			ddl := fmt.Sprintf(`
				CREATE SCHEMA %s_snapshot;
				CREATE TABLE %s_snapshot.foobar (id bigint PRIMARY KEY);
			`, schemaPrefix, schemaPrefix)
			var targetDDL []string
			if tc.inTarget {
				targetDDL = []string{ddl}
			} else {
				mustApplyDDLToTestDB(t, db.ConnPool, []string{ddl})
			}

			opts := []PlanOpt{WithTempDbFactory(factory)}
			if tc.prefix != defaultSchemaPartialArchivalPrefix {
				opts = append(opts, WithSchemaPartialArchivalPrefix(tc.prefix))
			}
			plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(targetDDL), opts...)
			require.NoError(t, err)
			assert.NotEmpty(t, plan.Statements)
			if tc.inTarget {
				assert.Empty(t, plan.CleanupStatements)
			} else {
				assert.NotEmpty(t, plan.CleanupStatements)
			}
		})
	}
}
