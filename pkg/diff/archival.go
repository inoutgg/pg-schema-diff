package diff

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

const archivalMarkerVersion = 2

var archivalGroupIDPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

type archivalName struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

type archivalMarker struct {
	Version int                    `json:"version"`
	GroupID string                 `json:"group_id"`
	Root    archivalName           `json:"root"`
	RootOID uint32                 `json:"root_oid"`
	Schemas []string               `json:"schemas"`
	Members []archivalMarkerMember `json:"members"`
}

type archivalMarkerMember struct {
	OID       uint32 `json:"oid"`
	Name      string `json:"name"`
	ParentOID uint32 `json:"parent_oid"`
}

type archivalMember struct {
	oid       uint32
	parentOID uint32
	name      archivalName
	depth     int
}

type archivalGroup struct {
	marker    archivalMarker
	markerSQL string
	members   []archivalMember
	statement Statement
}

type archivalTablePlan struct {
	group *archivalGroup
	root  bool
}

type archivalPlan struct {
	created         []*archivalGroup
	byTable         map[string]archivalTablePlan
	excludedSchemas []string
	createdSchemas  []string
	cleanup         []Statement
}

// GetSchemaHash returns the same modeled-schema and archival-marker hash used by Generate.
func GetSchemaHash(
	ctx context.Context,
	pool *pgxpool.Pool,
	prefix string,
	opts ...schema.GetSchemaOpt,
) (string, error) {
	if err := validateSchemaPartialArchivalPrefix(prefix); err != nil {
		return "", err
	}
	snapshot, err := schema.GetSchemaSnapshot(ctx, pool, opts...)
	if err != nil {
		return "", fmt.Errorf("getting modeled schema: %w", err)
	}
	groups, err := discoverArchivalGroups(ctx, pool, prefix)
	if err != nil {
		return "", fmt.Errorf("discovering archival groups: %w", err)
	}
	var excluded []string
	for _, group := range groups {
		excluded = append(excluded, group.marker.Schemas...)
	}
	return buildArchivalSchemaHash(schema.ExcludeSchemaNames(snapshot.Schema, excluded), groups)
}

func discoverArchivalGroups(
	ctx context.Context,
	pool *pgxpool.Pool,
	prefix string,
) ([]*archivalGroup, error) {
	return discoverArchivalGroupsBySchemaNames(ctx, pool, prefix, nil)
}

func discoverArchivalGroupsBySchemaNames(
	ctx context.Context,
	pool *pgxpool.Pool,
	prefix string,
	schemaNames []string,
) ([]*archivalGroup, error) {
	query := `
		SELECT n.nspname, COALESCE(pg_catalog.obj_description(n.oid, 'pg_namespace'), '')
		FROM pg_catalog.pg_namespace AS n
		WHERE n.nspname LIKE $1 ESCAPE '\'
		ORDER BY n.nspname
	`
	args := []any{strings.ReplaceAll(prefix, `\`, `\\`) + `\_%`}
	if schemaNames != nil {
		query = `
			SELECT n.nspname, COALESCE(pg_catalog.obj_description(n.oid, 'pg_namespace'), '')
			FROM pg_catalog.pg_namespace AS n
			WHERE n.nspname = ANY($1::text[])
			ORDER BY n.nspname
		`
		args = []any{schemaNames}
	}
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying archival schema markers: %w", err)
	}
	defer rows.Close()

	exactName := regexp.MustCompile(`^` + regexp.QuoteMeta(prefix) + `_[0-9a-f]{16}$`)
	comments := make(map[string]string)
	for rows.Next() {
		var name, comment string
		if err := rows.Scan(&name, &comment); err != nil {
			return nil, fmt.Errorf("scanning archival schema marker: %w", err)
		}
		if exactName.MatchString(name) {
			comments[name] = comment
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading archival schema markers: %w", err)
	}

	groupsByID := make(map[string]*archivalGroup)
	markerTextByID := make(map[string]string)
	for name, comment := range comments {
		marker, canonical, err := parseArchivalMarker(comment, prefix)
		if err != nil {
			return nil, fmt.Errorf("parsing marker on archival schema %q: %w", name, err)
		}
		if !slices.Contains(marker.Schemas, name) {
			return nil, fmt.Errorf("archival marker group %q does not list schema %q", marker.GroupID, name)
		}
		if previous, ok := markerTextByID[marker.GroupID]; ok && previous != canonical {
			return nil, fmt.Errorf("archival marker group %q has inconsistent marker copies", marker.GroupID)
		}
		markerTextByID[marker.GroupID] = canonical
		groupsByID[marker.GroupID] = &archivalGroup{marker: marker, markerSQL: canonical}
	}

	groups := make([]*archivalGroup, 0, len(groupsByID))
	for _, group := range groupsByID {
		for _, name := range group.marker.Schemas {
			comment, ok := comments[name]
			if !ok {
				return nil, fmt.Errorf("archival marker group %q is missing schema %q", group.marker.GroupID, name)
			}
			_, canonical, err := parseArchivalMarker(comment, prefix)
			if err != nil || canonical != group.markerSQL {
				return nil, fmt.Errorf("archival marker group %q has inconsistent marker copy on schema %q",
					group.marker.GroupID, name)
			}
		}
		if err := validateDiscoveredArchivalGroup(ctx, pool, group.marker); err != nil {
			return nil, fmt.Errorf("validating archival marker group %q: %w", group.marker.GroupID, err)
		}
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].marker.GroupID < groups[j].marker.GroupID
	})
	return groups, nil
}

func parseArchivalMarker(text, prefix string) (archivalMarker, string, error) {
	if text == "" {
		return archivalMarker{}, "", fmt.Errorf("marker comment is empty")
	}
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.DisallowUnknownFields()
	var marker archivalMarker
	if err := decoder.Decode(&marker); err != nil {
		return archivalMarker{}, "", fmt.Errorf("decoding strict JSON: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return archivalMarker{}, "", fmt.Errorf("marker contains trailing JSON data")
	}
	if marker.Version != archivalMarkerVersion {
		return archivalMarker{}, "", fmt.Errorf("unsupported marker version %d", marker.Version)
	}
	if !archivalGroupIDPattern.MatchString(marker.GroupID) {
		return archivalMarker{}, "", fmt.Errorf("invalid group ID %q", marker.GroupID)
	}
	if marker.Root.Schema == "" || marker.Root.Name == "" {
		return archivalMarker{}, "", fmt.Errorf("root qualified name is incomplete")
	}
	if marker.RootOID == 0 {
		return archivalMarker{}, "", fmt.Errorf("root OID is zero")
	}
	if len(marker.Schemas) != 1 {
		return archivalMarker{}, "", fmt.Errorf("group %q must have exactly one archival schema", marker.GroupID)
	}
	expectedSchema := prefix + "_" + marker.GroupID
	if marker.Schemas[0] != expectedSchema {
		return archivalMarker{}, "", fmt.Errorf("group %q schema must be %q", marker.GroupID, expectedSchema)
	}
	if len(marker.Members) == 0 {
		return archivalMarker{}, "", fmt.Errorf("group %q has no members", marker.GroupID)
	}
	canonicalMembers := slices.Clone(marker.Members)
	sort.Slice(canonicalMembers, func(i, j int) bool {
		if canonicalMembers[i].Name != canonicalMembers[j].Name {
			return canonicalMembers[i].Name < canonicalMembers[j].Name
		}
		return canonicalMembers[i].OID < canonicalMembers[j].OID
	})
	if !slices.Equal(marker.Members, canonicalMembers) {
		return archivalMarker{}, "", fmt.Errorf("group %q members are not canonically sorted", marker.GroupID)
	}
	byOID := make(map[uint32]archivalMarkerMember, len(marker.Members))
	names := make(map[string]struct{}, len(marker.Members))
	for _, member := range marker.Members {
		if member.OID == 0 || member.Name == "" {
			return archivalMarker{}, "", fmt.Errorf("group %q has an incomplete member", marker.GroupID)
		}
		if _, duplicate := byOID[member.OID]; duplicate {
			return archivalMarker{}, "", fmt.Errorf("group %q repeats member OID %d", marker.GroupID, member.OID)
		}
		if _, duplicate := names[member.Name]; duplicate {
			return archivalMarker{}, "", fmt.Errorf("group %q repeats member name %q", marker.GroupID, member.Name)
		}
		byOID[member.OID] = member
		names[member.Name] = struct{}{}
	}
	rootMember, ok := byOID[marker.RootOID]
	if !ok || rootMember.Name != marker.Root.Name || rootMember.ParentOID != 0 {
		return archivalMarker{}, "", fmt.Errorf("group %q root does not match its member record", marker.GroupID)
	}
	for _, member := range marker.Members {
		seen := make(map[uint32]struct{})
		current := member
		for current.ParentOID != 0 {
			if _, duplicate := seen[current.OID]; duplicate {
				return archivalMarker{}, "", fmt.Errorf(
					"group %q member tree contains a cycle", marker.GroupID,
				)
			}
			seen[current.OID] = struct{}{}
			parent, ok := byOID[current.ParentOID]
			if !ok {
				return archivalMarker{}, "", fmt.Errorf(
					"group %q member OID %d has unknown parent OID %d",
					marker.GroupID, current.OID, current.ParentOID,
				)
			}
			current = parent
		}
		if current.OID != marker.RootOID {
			return archivalMarker{}, "", fmt.Errorf(
				"group %q member OID %d is disconnected from the root",
				marker.GroupID, member.OID,
			)
		}
	}
	canonicalBytes, err := json.Marshal(marker)
	if err != nil {
		return archivalMarker{}, "", fmt.Errorf("encoding canonical marker: %w", err)
	}
	return marker, string(canonicalBytes), nil
}

func buildArchivalPlan(
	ctx context.Context,
	pool *pgxpool.Pool,
	current, target schema.Schema,
	deletedTables []schema.Table,
	existing []*archivalGroup,
	options *planOptions,
) (archivalPlan, error) {
	result := archivalPlan{byTable: make(map[string]archivalTablePlan)}
	for _, group := range existing {
		result.excludedSchemas = append(result.excludedSchemas, group.marker.Schemas...)
	}
	if len(deletedTables) == 0 {
		result.cleanup = renderArchivalCleanup(existing)
		return result, nil
	}
	if pool == nil {
		return archivalPlan{}, fmt.Errorf("table archival requires a live database-backed current schema source")
	}

	deletedByName := buildSchemaObjByNameMap(deletedTables)
	childrenByParent := make(map[string][]schema.Table)
	for _, table := range current.Tables {
		if table.ParentTable != nil {
			childrenByParent[table.ParentTable.GetName()] =
				append(childrenByParent[table.ParentTable.GetName()], table)
		}
	}

	var roots []schema.Table
	for _, table := range deletedTables {
		if table.ParentTable == nil {
			roots = append(roots, table)
			continue
		}
		if _, parentDeleted := deletedByName[table.ParentTable.GetName()]; !parentDeleted {
			return archivalPlan{}, fmt.Errorf("cannot archive retained-parent subtree rooted at %s", table.GetName())
		}
	}
	sort.Slice(roots, func(i, j int) bool {
		return roots[i].GetName() < roots[j].GetName()
	})

	usedSchemas := make(map[string]struct{})
	for _, named := range append(slices.Clone(current.NamedSchemas), target.NamedSchemas...) {
		usedSchemas[named.Name] = struct{}{}
	}
	for _, group := range existing {
		for _, name := range group.marker.Schemas {
			usedSchemas[name] = struct{}{}
		}
	}
	for _, root := range roots {
		var modeledMembers []schema.Table
		queue := []schema.Table{root}
		for len(queue) > 0 {
			member := queue[0]
			queue = queue[1:]
			modeledMembers = append(modeledMembers, member)
			for _, child := range childrenByParent[member.GetName()] {
				if _, deleted := deletedByName[child.GetName()]; !deleted {
					return archivalPlan{}, fmt.Errorf("cannot archive incomplete partition tree: descendant %s is retained", child.GetName())
				}
				queue = append(queue, child)
			}
		}
		members, err := inspectArchivalTree(ctx, pool, root, modeledMembers, current, target)
		if err != nil {
			return archivalPlan{}, err
		}
		groupID, err := newArchivalGroupID(options.randReader)
		if err != nil {
			return archivalPlan{}, err
		}
		archiveSchema := options.schemaPartialArchivalPrefix + "_" + groupID
		if _, exists := usedSchemas[archiveSchema]; exists {
			return archivalPlan{}, fmt.Errorf("generated archival schema %q already exists", archiveSchema)
		}
		usedSchemas[archiveSchema] = struct{}{}
		rootName, err := archivalNameFromTable(root)
		if err != nil {
			return archivalPlan{}, err
		}
		markerMembers := make([]archivalMarkerMember, 0, len(members))
		var rootOID uint32
		for _, member := range members {
			markerMembers = append(markerMembers, archivalMarkerMember{
				OID: member.oid, Name: member.name.Name, ParentOID: member.parentOID,
			})
			if member.name == rootName {
				rootOID = member.oid
			}
		}
		sort.Slice(markerMembers, func(i, j int) bool {
			if markerMembers[i].Name != markerMembers[j].Name {
				return markerMembers[i].Name < markerMembers[j].Name
			}
			return markerMembers[i].OID < markerMembers[j].OID
		})
		marker := archivalMarker{
			Version: archivalMarkerVersion, GroupID: groupID, Root: rootName, RootOID: rootOID,
			Schemas: []string{archiveSchema}, Members: markerMembers,
		}
		markerBytes, err := json.Marshal(marker)
		if err != nil {
			return archivalPlan{}, fmt.Errorf("encoding archival marker: %w", err)
		}
		group := &archivalGroup{marker: marker, markerSQL: string(markerBytes), members: members}
		group.statement = renderArchivalStatement(group)
		result.created = append(result.created, group)
		result.excludedSchemas = append(result.excludedSchemas, archiveSchema)
		result.createdSchemas = append(result.createdSchemas, archiveSchema)
		for _, member := range modeledMembers {
			result.byTable[member.GetName()] = archivalTablePlan{
				group: group,
				root:  member.GetName() == root.GetName(),
			}
		}
	}
	if len(result.byTable) != len(deletedByName) {
		return archivalPlan{}, fmt.Errorf("not every removed table was assigned to a complete archival group")
	}
	allGroups := append(slices.Clone(existing), result.created...)
	result.cleanup = renderArchivalCleanup(allGroups)
	slices.Sort(result.excludedSchemas)
	return result, nil
}

func newArchivalGroupID(reader io.Reader) (string, error) {
	bytes := make([]byte, 8)
	if _, err := io.ReadFull(reader, bytes); err != nil {
		return "", fmt.Errorf("generating archival group ID: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

func archivalNameFromTable(table schema.Table) (archivalName, error) {
	name, err := unescapeIdentifier(table.EscapedName)
	if err != nil {
		return archivalName{}, fmt.Errorf("reading table name %s: %w", table.GetName(), err)
	}
	return archivalName{Schema: table.SchemaName, Name: name}, nil
}

func unescapeIdentifier(identifier string) (string, error) {
	if len(identifier) < 2 || identifier[0] != '"' || identifier[len(identifier)-1] != '"' {
		return "", fmt.Errorf("expected quoted identifier, got %q", identifier)
	}
	return strings.ReplaceAll(identifier[1:len(identifier)-1], `""`, `"`), nil
}

func renderArchivalStatement(group *archivalGroup) Statement {
	archiveSchema := group.marker.Schemas[0]
	var sql strings.Builder
	sql.WriteString("DO $pgschemadiff$\nDECLARE\n    archived_tables oid[];\n    archival_grantee text;\n    marker_text text;\nBEGIN\n")
	members := slices.Clone(group.members)
	sort.Slice(members, func(i, j int) bool {
		if members[i].depth != members[j].depth {
			return members[i].depth > members[j].depth
		}
		return qualifiedArchivalName(members[i].name) < qualifiedArchivalName(members[j].name)
	})
	for _, member := range members {
		fmt.Fprintf(&sql, "    LOCK TABLE ONLY %s IN ACCESS EXCLUSIVE MODE;\n", qualifiedArchivalName(member.name))
	}
	fmt.Fprintf(&sql, "    archived_tables := %s;\n", archivalOIDArray(group.members))
	renderArchivalSQLPreflight(&sql, group)
	fmt.Fprintf(&sql, "    CREATE SCHEMA %s;\n", schema.EscapeIdentifier(archiveSchema))
	fmt.Fprintf(&sql, "    REVOKE ALL ON SCHEMA %s FROM PUBLIC;\n", schema.EscapeIdentifier(archiveSchema))
	fmt.Fprintf(&sql, `    FOR archival_grantee IN
		SELECT DISTINCT role.rolname
		FROM pg_catalog.pg_namespace AS n
		CROSS JOIN LATERAL pg_catalog.aclexplode(
			COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner))
		) AS acl
		JOIN pg_catalog.pg_roles AS role ON role.oid = acl.grantee
		WHERE n.nspname = %s AND acl.grantee <> n.nspowner
	LOOP
		EXECUTE format('REVOKE ALL ON SCHEMA %%I FROM %%I', %s, archival_grantee);
	END LOOP;
`, schema.EscapeLiteral(archiveSchema), schema.EscapeLiteral(archiveSchema))
	fmt.Fprintf(&sql, `    SELECT pg_catalog.jsonb_build_object(
		'version', %d,
		'group_id', %s,
		'root', pg_catalog.jsonb_build_object('schema', %s, 'name', %s),
		'root_oid', %s::regclass::oid::bigint,
		'schemas', pg_catalog.jsonb_build_array(%s),
		'members', pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
			'oid', member.oid::bigint,
			'name', member.relname,
			'parent_oid', COALESCE(inheritance.inhparent, 0)::bigint
		) ORDER BY member.relname, member.oid)
	)::text
	INTO marker_text
	FROM pg_catalog.pg_class AS member
	LEFT JOIN pg_catalog.pg_inherits AS inheritance ON inheritance.inhrelid = member.oid
	WHERE member.oid = ANY(archived_tables);
	EXECUTE format('COMMENT ON SCHEMA %%I IS %%L', %s, marker_text);
`, archivalMarkerVersion, schema.EscapeLiteral(group.marker.GroupID),
		schema.EscapeLiteral(group.marker.Root.Schema),
		schema.EscapeLiteral(group.marker.Root.Name),
		schema.EscapeLiteral(qualifiedArchivalName(group.marker.Root)),
		schema.EscapeLiteral(archiveSchema), schema.EscapeLiteral(archiveSchema))
	for _, member := range members {
		fmt.Fprintf(&sql, "    ALTER TABLE %s SET SCHEMA %s;\n",
			qualifiedArchivalName(member.name), schema.EscapeIdentifier(archiveSchema))
	}
	sql.WriteString("END\n$pgschemadiff$")
	return Statement{
		DDL: sql.String(),
		Hazards: []MigrationHazard{
			{
				Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
				Message: "Archiving locks the removed table tree while it is moved atomically.",
			},
			{Type: MigrationHazardTypeAuthzUpdate, Message: "The generated archival schema is accessible only to its owner."},
		},
	}
}

func archivalOIDArray(members []archivalMember) string {
	values := make([]string, 0, len(members))
	for _, member := range members {
		values = append(values, schema.EscapeLiteral(qualifiedArchivalName(member.name))+"::regclass::oid")
	}
	return "ARRAY[" + strings.Join(values, ", ") + "]::oid[]"
}

func qualifiedArchivalName(name archivalName) string {
	return schema.EscapeIdentifier(name.Schema) + "." + schema.EscapeIdentifier(name.Name)
}

func renderArchivalCleanup(groups []*archivalGroup) []Statement {
	groups = slices.Clone(groups)
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].marker.GroupID < groups[j].marker.GroupID
	})
	statements := make([]Statement, 0, len(groups))
	for _, group := range groups {
		statements = append(statements, Statement{
			DDL: renderArchivalCleanupGroup(group),
			Hazards: []MigrationHazard{
				{Type: MigrationHazardTypeDeletesData, Message: "Cleanup permanently deletes the retained table tree."},
				{
					Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
					Message: "Cleanup locks the retained table tree while dropping it.",
				},
			},
		})
	}
	return statements
}

func renderArchivalCleanupGroup(group *archivalGroup) string {
	archiveSchema := group.marker.Schemas[0]
	root := archivalName{Schema: archiveSchema, Name: group.marker.Root.Name}
	var sql strings.Builder
	sql.WriteString("DO $pgschemadiff_cleanup$\nDECLARE\n    archived_tables oid[];\n    marker_text text;\nBEGIN\n")
	for _, member := range group.marker.Members {
		fmt.Fprintf(&sql, "    LOCK TABLE ONLY %s IN ACCESS EXCLUSIVE MODE;\n",
			qualifiedArchivalName(archivalName{Schema: archiveSchema, Name: member.Name}))
	}
	fmt.Fprintf(&sql, `    SELECT pg_catalog.obj_description(n.oid, 'pg_namespace')
	INTO marker_text
	FROM pg_catalog.pg_namespace AS n
	WHERE n.nspname = %s;
	IF marker_text IS DISTINCT FROM (%s::jsonb)::text THEN
		RAISE EXCEPTION 'archival schema marker changed';
	END IF;
	archived_tables := %s;
	IF NOT (archived_tables <@ %s AND archived_tables @> %s) THEN
		RAISE EXCEPTION 'archival relation OIDs changed';
	END IF;
`, schema.EscapeLiteral(archiveSchema), schema.EscapeLiteral(group.markerSQL),
		archivalOIDArrayForSchema(group.marker.Members, archiveSchema),
		archivalMarkerOIDArray(group.marker.Members),
		archivalMarkerOIDArray(group.marker.Members))
	fmt.Fprintf(&sql, `    IF (SELECT count(*) FROM pg_catalog.pg_class AS c
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = %s AND c.relkind IN ('r', 'p', 'f')) <> %d THEN
		RAISE EXCEPTION 'archival schema table set changed';
	END IF;
	IF EXISTS (
		SELECT 1
		FROM (VALUES %s) AS expected(oid, name, parent_oid)
		LEFT JOIN pg_catalog.pg_class AS c ON c.oid = expected.oid
		LEFT JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE c.relname IS DISTINCT FROM expected.name OR n.nspname IS DISTINCT FROM %s
		   OR (expected.parent_oid = 0 AND EXISTS (
			SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid = expected.oid
		   ))
		   OR (expected.parent_oid <> 0 AND (SELECT count(*) FROM pg_catalog.pg_inherits
			WHERE inhrelid = expected.oid AND inhparent = expected.parent_oid) <> 1)
		   OR (SELECT count(*) FROM pg_catalog.pg_inherits WHERE inhrelid = expected.oid) > 1
	) THEN
		RAISE EXCEPTION 'archival relation identity or tree changed';
	END IF;
	IF EXISTS (
		SELECT 1 FROM pg_catalog.pg_inherits
		WHERE (inhrelid = ANY(archived_tables)) <> (inhparent = ANY(archived_tables))
	) THEN
		RAISE EXCEPTION 'archival tree has an external partition edge';
	END IF;
`, schema.EscapeLiteral(archiveSchema), len(group.marker.Members),
		archivalMarkerValues(group.marker.Members),
		schema.EscapeLiteral(archiveSchema))
	fmt.Fprintf(&sql, "    DROP TABLE %s RESTRICT;\n", qualifiedArchivalName(root))
	fmt.Fprintf(&sql, "    DROP SCHEMA %s RESTRICT;\n", schema.EscapeIdentifier(archiveSchema))
	sql.WriteString("END\n$pgschemadiff_cleanup$")
	return sql.String()
}

func archivalOIDArrayForSchema(members []archivalMarkerMember, archiveSchema string) string {
	values := make([]string, 0, len(members))
	for _, member := range members {
		name := qualifiedArchivalName(archivalName{Schema: archiveSchema, Name: member.Name})
		values = append(values, schema.EscapeLiteral(name)+"::regclass::oid")
	}
	return "ARRAY[" + strings.Join(values, ", ") + "]::oid[]"
}

func archivalMarkerOIDArray(members []archivalMarkerMember) string {
	values := make([]string, 0, len(members))
	for _, member := range members {
		values = append(values, fmt.Sprintf("%d::oid", member.OID))
	}
	return "ARRAY[" + strings.Join(values, ", ") + "]::oid[]"
}

func archivalMarkerValues(members []archivalMarkerMember) string {
	values := make([]string, 0, len(members))
	for _, member := range members {
		values = append(values, fmt.Sprintf("(%d::oid, %s::text, %d::oid)",
			member.OID, schema.EscapeLiteral(member.Name), member.ParentOID))
	}
	return strings.Join(values, ", ")
}

func buildArchivalSchemaHash(managed schema.Schema, existing []*archivalGroup) (string, error) {
	modeledHash, err := managed.Hash()
	if err != nil {
		return "", fmt.Errorf("hashing modeled schema: %w", err)
	}
	markers := make([]archivalMarker, 0, len(existing))
	for _, group := range existing {
		markers = append(markers, group.marker)
	}
	sort.Slice(markers, func(i, j int) bool {
		return markers[i].GroupID < markers[j].GroupID
	})
	payload, err := json.Marshal(struct {
		Modeled string           `json:"modeled"`
		Groups  []archivalMarker `json:"archive_groups"`
	}{Modeled: modeledHash, Groups: markers})
	if err != nil {
		return "", fmt.Errorf("encoding schema hash input: %w", err)
	}
	digest := sha256.Sum256(payload)
	return "pg-schema-diff:snapshot:v2:sha256:" + hex.EncodeToString(digest[:]), nil
}
