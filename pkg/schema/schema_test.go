package schema_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/testdb"
	"github.com/stripe/pg-schema-diff/pkg/diff"
	"github.com/stripe/pg-schema-diff/pkg/schema"
)

func TestSchemaTestSuite(t *testing.T) {
	t.Parallel()
	t.Run("TestGetPublicSchemaHash", func(t *testing.T) {
		t.Parallel()
		const (
			ddl = `
			CREATE EXTENSION pg_trgm WITH VERSION '1.6';

			CREATE FUNCTION add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;

			CREATE FUNCTION increment(i integer) RETURNS integer AS $$
					BEGIN
							RETURN i + 1;
					END;
			$$ LANGUAGE plpgsql;

			CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN add(a, b) + increment(a);

			CREATE TABLE foo (
				id INTEGER PRIMARY KEY,
				author TEXT COLLATE "C",
				content TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP CHECK (created_at > CURRENT_TIMESTAMP - interval '1 month') NO INHERIT,
				version INT NOT NULL DEFAULT 0,
				CHECK ( function_with_dependencies(id, id) > 0)
			);

			ALTER TABLE foo ADD CONSTRAINT author_check CHECK (author IS NOT NULL AND LENGTH(author) > 0) NO INHERIT NOT VALID;
			CREATE INDEX some_idx ON foo USING hash (content);
			CREATE UNIQUE INDEX some_unique_idx ON foo (created_at DESC, author ASC);

			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_trigger
				BEFORE UPDATE ON foo
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();
			
	        CREATE SCHEMA schema_filtered_1;
			CREATE TABLE schema_filtered_1.bar()
		`
		)
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		_, err := db.ConnPool.Exec(t.Context(), ddl)
		require.NoError(t, err)

		hash, err := schema.GetSchemaHash(t.Context(), db.ConnPool,
			schema.WithIncludeSchemaPatterns("public"))
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(hash, "pg-schema-diff:snapshot:v2:sha256:"))
	})

	t.Run("TestGetSchemaHashDoesNotHideUnmarkedPrefixSchemas", func(t *testing.T) {
		t.Parallel()

		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA pgschemadiff_archive_public_foo;
			CREATE TABLE pgschemadiff_archive_public_foo.foo (id bigint PRIMARY KEY);
			CREATE SCHEMA deleted_public_foo;
			CREATE TABLE deleted_public_foo.foo (id bigint PRIMARY KEY);
		`)
		require.NoError(t, err)

		hash, err := schema.GetSchemaHash(t.Context(), db.ConnPool)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(hash, "pg-schema-diff:snapshot:v2:sha256:"))

		customHash, err := schema.GetSchemaHash(t.Context(), db.ConnPool,
			schema.WithExcludeSchemaPatterns("deleted.*"))
		require.NoError(t, err)
		assert.NotEqual(t, hash, customHash)
	})
}

func TestGetSchemaHashMatchesGenerateWithExistingArchiveAndFilter(t *testing.T) {
	t.Parallel()
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA managed;
		CREATE TABLE managed.keep (id bigint PRIMARY KEY);
		CREATE TABLE managed.removed (id bigint);
		CREATE SCHEMA ignored;
		CREATE TABLE ignored.unmanaged (id bigint);
	`)
	require.NoError(t, err)
	targetDDL := []string{`
		CREATE SCHEMA managed;
		CREATE TABLE managed.keep (id bigint PRIMARY KEY);
	`}
	options := []diff.PlanOpt{
		diff.WithTempDbFactory(factory),
		diff.WithSchemaPartialArchivalPrefix("hash_archive"),
		diff.WithIncludeSchemaPatterns("managed"),
		diff.WithRandReader(strings.NewReader("hashmark")),
	}
	archivePlan, err := diff.Generate(t.Context(), diff.DBSchemaSource(db.ConnPool),
		diff.DDLSchemaSource(targetDDL), options...)
	require.NoError(t, err)
	for _, statement := range archivePlan.Statements {
		_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err)
	}

	plan, err := diff.Generate(t.Context(), diff.DBSchemaSource(db.ConnPool),
		diff.DDLSchemaSource(targetDDL), options...)
	require.NoError(t, err)
	assert.Empty(t, plan.Statements)
	assert.NotEmpty(t, plan.CleanupStatements)
	publicHash, err := schema.GetSchemaHashWithArchivalPrefix(t.Context(), db.ConnPool, "hash_archive",
		schema.WithIncludeSchemaPatterns("managed"))
	require.NoError(t, err)
	assert.Equal(t, plan.CurrentSchemaHash, publicHash)
}
