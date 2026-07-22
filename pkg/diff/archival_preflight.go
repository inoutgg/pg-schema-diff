package diff

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type archivalTableFact struct {
	oid         uint32
	name        archivalName
	kind        string
	persistence string
	isPartition bool
	parentCount int
	parent      archivalName
	parentOID   uint32
}

func validateChangedExtensionTableMembers(
	ctx context.Context,
	pool *pgxpool.Pool,
	current, target schema.Schema,
) error {
	targetByName := make(map[string]schema.Extension, len(target.Extensions))
	for _, extension := range target.Extensions {
		name, err := unescapeIdentifier(extension.EscapedName)
		if err != nil {
			return err
		}
		targetByName[name] = extension
	}
	for _, extension := range current.Extensions {
		name, err := unescapeIdentifier(extension.EscapedName)
		if err != nil {
			return err
		}
		if targetExtension, exists := targetByName[name]; exists &&
			reflect.DeepEqual(extension, targetExtension) {
			continue
		}
		if pool == nil {
			return fmt.Errorf("changing extension %q requires a live database-backed current schema source", name)
		}
		var memberSchema, memberName, memberKind string
		err = pool.QueryRow(ctx, `
			SELECT n.nspname, c.relname, c.relkind::text
			FROM pg_catalog.pg_extension AS extension
			JOIN pg_catalog.pg_depend AS dependency
			  ON dependency.refclassid = 'pg_extension'::regclass
			 AND dependency.refobjid = extension.oid
			 AND dependency.deptype = 'e'
			JOIN pg_catalog.pg_class AS c
			  ON dependency.classid = 'pg_class'::regclass AND dependency.objid = c.oid
			JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
			WHERE extension.extname = $1 AND c.relkind IN ('r', 'p', 'f')
			ORDER BY n.nspname, c.relname
			LIMIT 1
		`, name).Scan(&memberSchema, &memberName, &memberKind)
		if err == nil {
			return fmt.Errorf("changed extension %q owns hidden table-like member %s.%s (kind %s)",
				name, memberSchema, memberName, memberKind)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("querying table-like members of changed extension %q: %w", name, err)
		}
	}
	return nil
}

func validateDiscoveredArchivalGroup(
	ctx context.Context,
	pool *pgxpool.Pool,
	marker archivalMarker,
) error {
	archiveSchema := marker.Schemas[0]
	rows, err := pool.Query(ctx, `
		SELECT c.oid::bigint, c.relname, c.relkind::text, c.relpersistence::text,
		       count(i.inhparent), COALESCE(min(i.inhparent), 0)::bigint
		FROM pg_catalog.pg_class AS c
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		LEFT JOIN pg_catalog.pg_inherits AS i ON i.inhrelid = c.oid
		WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'f')
		GROUP BY c.oid, c.relname, c.relkind, c.relpersistence
		ORDER BY c.relname, c.oid
	`, archiveSchema)
	if err != nil {
		return fmt.Errorf("querying archived members: %w", err)
	}
	defer rows.Close()
	actual := make(map[uint32]archivalMarkerMember)
	for rows.Next() {
		var oid, parentOID int64
		var name, kind, persistence string
		var parentCount int
		if err := rows.Scan(&oid, &name, &kind, &persistence, &parentCount, &parentOID); err != nil {
			return fmt.Errorf("scanning archived member: %w", err)
		}
		if kind != "r" && kind != "p" || persistence != "p" {
			return fmt.Errorf("archived relation %s.%s has unsupported kind or persistence", archiveSchema, name)
		}
		if parentCount > 1 {
			return fmt.Errorf("archived relation %s.%s has multiple parents", archiveSchema, name)
		}
		actual[uint32(oid)] = archivalMarkerMember{OID: uint32(oid), Name: name, ParentOID: uint32(parentOID)}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading archived members: %w", err)
	}
	if len(actual) != len(marker.Members) {
		return fmt.Errorf("archived relation set has %d members instead of %d", len(actual), len(marker.Members))
	}
	expectedOIDs := make([]uint32, 0, len(marker.Members))
	for _, expected := range marker.Members {
		if actual[expected.OID] != expected {
			return fmt.Errorf("archived member OID %d does not match its marker identity or parent", expected.OID)
		}
		expectedOIDs = append(expectedOIDs, expected.OID)
	}
	var recursiveCount int
	var boundary bool
	err = pool.QueryRow(ctx, `
		WITH RECURSIVE tree AS (
			SELECT $1::oid AS oid
			UNION ALL
			SELECT i.inhrelid FROM tree JOIN pg_catalog.pg_inherits AS i ON i.inhparent = tree.oid
		)
		SELECT (SELECT count(DISTINCT oid) FROM tree), EXISTS (
			SELECT 1 FROM pg_catalog.pg_inherits
			WHERE (inhrelid = ANY($2::oid[])) <> (inhparent = ANY($2::oid[]))
		)
	`, marker.RootOID, expectedOIDs).Scan(&recursiveCount, &boundary)
	if err != nil {
		return fmt.Errorf("checking archived member tree: %w", err)
	}
	if boundary || recursiveCount != len(marker.Members) {
		return fmt.Errorf("archived recursive tree has external or missing members")
	}
	return nil
}

func inspectArchivalTree(
	ctx context.Context,
	pool *pgxpool.Pool,
	root schema.Table,
	modeledMembers []schema.Table,
	current schema.Schema,
	target schema.Schema,
) ([]archivalMember, error) {
	rootName, err := archivalNameFromTable(root)
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
		WITH RECURSIVE tree AS (
			SELECT c.oid
			FROM pg_catalog.pg_class AS c
			JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2
			UNION ALL
			SELECT i.inhrelid
			FROM tree
			JOIN pg_catalog.pg_inherits AS i ON i.inhparent = tree.oid
		)
		SELECT c.oid::bigint, n.nspname, c.relname, c.relkind::text,
		       c.relpersistence::text, c.relispartition,
		       (SELECT count(*) FROM pg_catalog.pg_inherits AS p WHERE p.inhrelid = c.oid),
		       COALESCE(parent_namespace.nspname, ''), COALESCE(pc.relname, ''), COALESCE(i.inhparent, 0)::bigint
		FROM tree
		JOIN pg_catalog.pg_class AS c ON c.oid = tree.oid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		LEFT JOIN pg_catalog.pg_inherits AS i ON i.inhrelid = c.oid AND i.inhseqno = 1
		LEFT JOIN pg_catalog.pg_class AS pc ON pc.oid = i.inhparent
		LEFT JOIN pg_catalog.pg_namespace AS parent_namespace ON parent_namespace.oid = pc.relnamespace
		ORDER BY n.nspname, c.relname, c.oid
	`, rootName.Schema, rootName.Name)
	if err != nil {
		return nil, fmt.Errorf("inspecting archival tree rooted at %s: %w", root.GetName(), err)
	}
	defer rows.Close()

	var facts []archivalTableFact
	seenOIDs := make(map[uint32]struct{})
	for rows.Next() {
		var oid, parentOID int64
		var fact archivalTableFact
		if err := rows.Scan(&oid, &fact.name.Schema, &fact.name.Name, &fact.kind,
			&fact.persistence, &fact.isPartition, &fact.parentCount,
			&fact.parent.Schema, &fact.parent.Name, &parentOID); err != nil {
			return nil, fmt.Errorf("scanning archival tree fact: %w", err)
		}
		fact.oid = uint32(oid)
		fact.parentOID = uint32(parentOID)
		if _, duplicate := seenOIDs[fact.oid]; duplicate {
			return nil, fmt.Errorf("table %s participates in multiple inheritance", qualifiedArchivalName(fact.name))
		}
		seenOIDs[fact.oid] = struct{}{}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading archival tree facts: %w", err)
	}
	if len(facts) == 0 {
		return nil, fmt.Errorf("archival root %s does not exist in the live current database", root.GetName())
	}

	expected := make(map[string]struct{}, len(modeledMembers))
	for _, member := range modeledMembers {
		name, err := archivalNameFromTable(member)
		if err != nil {
			return nil, err
		}
		expected[name.Schema+"\x00"+name.Name] = struct{}{}
	}
	factsByName := make(map[string]archivalTableFact, len(facts))
	childrenByParent := make(map[string][]archivalTableFact)
	destinationNames := make(map[string]struct{})
	for _, fact := range facts {
		key := fact.name.Schema + "\x00" + fact.name.Name
		if _, ok := expected[key]; !ok {
			return nil, fmt.Errorf("archival tree rooted at %s contains unmodeled descendant %s",
				root.GetName(), qualifiedArchivalName(fact.name))
		}
		delete(expected, key)
		if fact.kind != "r" && fact.kind != "p" {
			return nil, fmt.Errorf("cannot archive unsupported relation kind %q for %s",
				fact.kind, qualifiedArchivalName(fact.name))
		}
		if fact.persistence != "p" {
			return nil, fmt.Errorf("cannot archive non-permanent table %s", qualifiedArchivalName(fact.name))
		}
		if _, duplicate := destinationNames[fact.name.Name]; duplicate {
			return nil, fmt.Errorf("partition tree rooted at %s has duplicate table name %q across schemas",
				root.GetName(), fact.name.Name)
		}
		destinationNames[fact.name.Name] = struct{}{}
		factsByName[key] = fact
		if fact.parent.Name != "" {
			parentKey := fact.parent.Schema + "\x00" + fact.parent.Name
			childrenByParent[parentKey] = append(childrenByParent[parentKey], fact)
		}
	}
	if len(expected) != 0 {
		return nil, fmt.Errorf("modeled archival tree rooted at %s does not match the live partition tree", root.GetName())
	}
	rootFact := factsByName[rootName.Schema+"\x00"+rootName.Name]
	if rootFact.parentCount != 0 || rootFact.isPartition {
		return nil, fmt.Errorf("cannot archive retained-parent subtree rooted at %s", root.GetName())
	}
	for _, fact := range facts {
		if fact.oid == rootFact.oid {
			continue
		}
		if fact.parentCount != 1 || !fact.isPartition {
			return nil, fmt.Errorf("cannot archive traditional or multiple inheritance member %s",
				qualifiedArchivalName(fact.name))
		}
		if _, parentIncluded := factsByName[fact.parent.Schema+"\x00"+fact.parent.Name]; !parentIncluded {
			return nil, fmt.Errorf("cannot archive incomplete partition tree at %s", qualifiedArchivalName(fact.name))
		}
	}
	for key, children := range childrenByParent {
		if len(children) > 0 && factsByName[key].kind != "p" {
			return nil, fmt.Errorf("inheritance parent %s is not a declaratively partitioned table", key)
		}
	}

	if err := validateArchivalSourceSafety(ctx, pool, rootName, facts, current, target); err != nil {
		return nil, fmt.Errorf("archival preflight for %s: %w", root.GetName(), err)
	}

	depthByOID := map[uint32]int{rootFact.oid: 0}
	remaining := len(facts) - 1
	for remaining > 0 {
		progress := false
		for _, fact := range facts {
			if _, set := depthByOID[fact.oid]; set {
				continue
			}
			parent := factsByName[fact.parent.Schema+"\x00"+fact.parent.Name]
			if depth, ok := depthByOID[parent.oid]; ok {
				depthByOID[fact.oid] = depth + 1
				remaining--
				progress = true
			}
		}
		if !progress {
			return nil, fmt.Errorf("partition tree rooted at %s contains a cycle", root.GetName())
		}
	}
	members := make([]archivalMember, 0, len(facts))
	for _, fact := range facts {
		members = append(members, archivalMember{
			oid: fact.oid, parentOID: fact.parentOID, name: fact.name, depth: depthByOID[fact.oid],
		})
	}
	sort.Slice(members, func(i, j int) bool {
		return qualifiedArchivalName(members[i].name) < qualifiedArchivalName(members[j].name)
	})
	return members, nil
}

func validateArchivalSourceSafety(
	ctx context.Context,
	pool *pgxpool.Pool,
	root archivalName,
	facts []archivalTableFact,
	current schema.Schema,
	target schema.Schema,
) error {
	var eventTrigger, crossFK, publication, statistics, extensionOwned, nameConflict bool
	err := pool.QueryRow(ctx, `
		WITH RECURSIVE tree AS (
			SELECT c.oid
			FROM pg_catalog.pg_class AS c
			JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2
			UNION ALL
			SELECT i.inhrelid FROM tree JOIN pg_catalog.pg_inherits AS i ON i.inhparent = tree.oid
		), members AS (SELECT oid FROM tree)
		SELECT
			EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger WHERE evtenabled <> 'D'),
			EXISTS (
				SELECT 1 FROM pg_catalog.pg_constraint AS con
				WHERE con.contype = 'f'
				  AND ((con.conrelid IN (SELECT oid FROM members)) <> (con.confrelid IN (SELECT oid FROM members)))
			),
			EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE puballtables)
			OR EXISTS (
				SELECT 1 FROM pg_catalog.pg_publication_tables AS published
				JOIN pg_catalog.pg_namespace AS n ON n.nspname = published.schemaname
				JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid AND c.relname = published.tablename
				WHERE c.oid IN (SELECT oid FROM members)
			),
			EXISTS (SELECT 1 FROM pg_catalog.pg_statistic_ext WHERE stxrelid IN (SELECT oid FROM members)),
			EXISTS (
				SELECT 1 FROM pg_catalog.pg_depend AS d
				WHERE d.deptype = 'e' AND (
					(d.classid = 'pg_class'::regclass AND (
						d.objid IN (SELECT oid FROM members)
						OR d.objid IN (SELECT indexrelid FROM pg_catalog.pg_index WHERE indrelid IN (SELECT oid FROM members))
					))
					OR (d.classid = 'pg_constraint'::regclass AND d.objid IN (
						SELECT oid FROM pg_catalog.pg_constraint WHERE conrelid IN (SELECT oid FROM members)
					))
					OR (d.classid = 'pg_trigger'::regclass AND d.objid IN (
						SELECT oid FROM pg_catalog.pg_trigger WHERE tgrelid IN (SELECT oid FROM members)
					))
				)
			),
			EXISTS (
				SELECT moved.relname
				FROM pg_catalog.pg_class AS moved
				WHERE moved.oid IN (SELECT oid FROM members)
				   OR moved.oid IN (SELECT indexrelid FROM pg_catalog.pg_index WHERE indrelid IN (SELECT oid FROM members))
				   OR moved.oid IN (
					SELECT d.objid FROM pg_catalog.pg_depend AS d
					JOIN pg_catalog.pg_class AS sequence ON sequence.oid = d.objid AND sequence.relkind = 'S'
					WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass
					  AND d.refobjid IN (SELECT oid FROM members) AND d.deptype IN ('a', 'i')
				   )
				GROUP BY moved.relname HAVING count(*) > 1
			)
	`, root.Schema, root.Name).Scan(&eventTrigger, &crossFK, &publication, &statistics, &extensionOwned, &nameConflict)
	if err != nil {
		return fmt.Errorf("querying unsupported source conditions: %w", err)
	}
	if eventTrigger {
		return fmt.Errorf("enabled event triggers may affect archival DDL")
	}
	if crossFK {
		return fmt.Errorf("cross-boundary foreign keys are not supported")
	}
	if publication {
		return fmt.Errorf("publication membership affecting archived tables is not supported")
	}
	if statistics {
		return fmt.Errorf("extended statistics on archived tables are not supported")
	}
	if extensionOwned {
		return fmt.Errorf("extension-owned archived relations or attached objects are not supported")
	}
	if nameConflict {
		return fmt.Errorf("archived tables and their attached indexes or sequences have conflicting destination names")
	}

	groupNames := make(map[string]struct{}, len(facts))
	for _, fact := range facts {
		groupNames[qualifiedArchivalName(fact.name)] = struct{}{}
	}
	return validateStableArchivalDependencies(ctx, pool, root, groupNames, current, target)
}

type archivalDependencyFact struct {
	kind, schema, name, identityArguments, extension string
}

func validateStableArchivalDependencies(
	ctx context.Context,
	pool *pgxpool.Pool,
	root archivalName,
	groupNames map[string]struct{},
	current schema.Schema,
	target schema.Schema,
) error {
	rows, err := pool.Query(ctx, `
		WITH RECURSIVE tree AS (
			SELECT c.oid FROM pg_catalog.pg_class AS c
			JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2
			UNION ALL
			SELECT i.inhrelid FROM tree JOIN pg_catalog.pg_inherits AS i ON i.inhparent = tree.oid
		), members AS (SELECT oid FROM tree), dependencies AS (
			SELECT 'type'::text AS kind, tn.nspname, t.typname, ''::text AS args, t.oid
			FROM pg_catalog.pg_attribute AS a
			JOIN pg_catalog.pg_type AS declared_type ON declared_type.oid = a.atttypid
			JOIN pg_catalog.pg_type AS t ON t.oid = CASE
				WHEN declared_type.typcategory = 'A' AND declared_type.typelem <> 0 THEN declared_type.typelem
				ELSE declared_type.oid
			END
			JOIN pg_catalog.pg_namespace AS tn ON tn.oid = t.typnamespace
			WHERE a.attrelid IN (SELECT oid FROM members) AND a.attnum > 0 AND NOT a.attisdropped
			UNION
			SELECT 'function', routine_namespace.nspname, p.proname,
			       pg_catalog.pg_get_function_identity_arguments(p.oid), p.oid
			FROM pg_catalog.pg_trigger AS tg
			JOIN pg_catalog.pg_proc AS p ON p.oid = tg.tgfoid
			JOIN pg_catalog.pg_namespace AS routine_namespace ON routine_namespace.oid = p.pronamespace
			WHERE tg.tgrelid IN (SELECT oid FROM members) AND NOT tg.tgisinternal
			UNION
			SELECT 'function', routine_namespace.nspname, p.proname,
			       pg_catalog.pg_get_function_identity_arguments(p.oid), p.oid
			FROM pg_catalog.pg_depend AS d
			JOIN pg_catalog.pg_proc AS p ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = p.oid
			JOIN pg_catalog.pg_namespace AS routine_namespace ON routine_namespace.oid = p.pronamespace
			WHERE
				(d.classid = 'pg_constraint'::regclass AND d.objid IN (
					SELECT oid FROM pg_catalog.pg_constraint WHERE conrelid IN (SELECT oid FROM members)
				)) OR
				(d.classid = 'pg_class'::regclass AND d.objid IN (
					SELECT indexrelid FROM pg_catalog.pg_index WHERE indrelid IN (SELECT oid FROM members)
				)) OR
				(d.classid = 'pg_attrdef'::regclass AND d.objid IN (
					SELECT oid FROM pg_catalog.pg_attrdef WHERE adrelid IN (SELECT oid FROM members)
				)) OR
				(d.classid = 'pg_policy'::regclass AND d.objid IN (
					SELECT oid FROM pg_catalog.pg_policy WHERE polrelid IN (SELECT oid FROM members)
				)) OR
				(d.classid = 'pg_rewrite'::regclass AND d.objid IN (
					SELECT oid FROM pg_catalog.pg_rewrite WHERE ev_class IN (SELECT oid FROM members)
				))
			UNION
			SELECT 'dependent_routine', routine_namespace.nspname, p.proname,
			       pg_catalog.pg_get_function_identity_arguments(p.oid), p.oid
			FROM pg_catalog.pg_depend AS d
			JOIN pg_catalog.pg_proc AS p ON d.classid = 'pg_proc'::regclass AND d.objid = p.oid
			JOIN pg_catalog.pg_namespace AS routine_namespace ON routine_namespace.oid = p.pronamespace
			WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid IN (SELECT oid FROM members)
			UNION
			SELECT 'sequence', sn.nspname, s.relname, ''::text, s.oid
			FROM pg_catalog.pg_attrdef AS ad
			JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_attrdef'::regclass AND d.objid = ad.oid
			JOIN pg_catalog.pg_class AS s ON d.refclassid = 'pg_class'::regclass AND d.refobjid = s.oid AND s.relkind = 'S'
			JOIN pg_catalog.pg_namespace AS sn ON sn.oid = s.relnamespace
			WHERE ad.adrelid IN (SELECT oid FROM members)
		)
		SELECT dep.kind, dep.nspname, dep.typname, dep.args,
		       COALESCE(ext.extname, '')
		FROM dependencies AS dep
		LEFT JOIN pg_catalog.pg_depend AS ed
		  ON ed.objid = dep.oid AND ed.deptype = 'e'
		 AND ed.classid = CASE dep.kind
			WHEN 'function' THEN 'pg_proc'::regclass
			WHEN 'dependent_routine' THEN 'pg_proc'::regclass
			WHEN 'sequence' THEN 'pg_class'::regclass
			ELSE 'pg_type'::regclass
		 END
		LEFT JOIN pg_catalog.pg_extension AS ext
		  ON ed.refclassid = 'pg_extension'::regclass AND ed.refobjid = ext.oid
		WHERE dep.nspname <> 'pg_catalog'
		ORDER BY dep.kind, dep.nspname, dep.typname, dep.args
	`, root.Schema, root.Name)
	if err != nil {
		return fmt.Errorf("querying archived table dependencies: %w", err)
	}
	defer rows.Close()
	var dependencies []archivalDependencyFact
	for rows.Next() {
		var dependency archivalDependencyFact
		if err := rows.Scan(&dependency.kind, &dependency.schema, &dependency.name,
			&dependency.identityArguments, &dependency.extension); err != nil {
			return fmt.Errorf("scanning archived table dependency: %w", err)
		}
		dependencies = append(dependencies, dependency)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading archived table dependencies: %w", err)
	}

	currentExtensions := make(map[string]schema.Extension)
	targetExtensions := make(map[string]schema.Extension)
	for _, extension := range current.Extensions {
		name, _ := unescapeIdentifier(extension.EscapedName)
		currentExtensions[name] = extension
	}
	for _, extension := range target.Extensions {
		name, _ := unescapeIdentifier(extension.EscapedName)
		targetExtensions[name] = extension
	}
	targetFunctions := buildSchemaObjByNameMap(target.Functions)
	currentFunctions := buildSchemaObjByNameMap(current.Functions)
	targetProcedures := buildSchemaObjByNameMap(target.Procedures)
	currentEnums := buildSchemaObjByNameMap(current.Enums)
	targetEnums := buildSchemaObjByNameMap(target.Enums)
	currentSequences := buildSchemaObjByNameMap(current.Sequences)
	targetSequences := buildSchemaObjByNameMap(target.Sequences)
	for _, dependency := range dependencies {
		if dependency.kind == "dependent_routine" {
			name := schema.SchemaQualifiedName{
				SchemaName:  dependency.schema,
				EscapedName: fmt.Sprintf("%s(%s)", schema.EscapeIdentifier(dependency.name), dependency.identityArguments),
			}
			_, functionRemains := targetFunctions[name.GetName()]
			_, procedureRemains := targetProcedures[name.GetName()]
			if functionRemains || procedureRemains {
				return fmt.Errorf("retained catalog-tracked routine %s depends on an archived table", name.GetName())
			}
			continue
		}
		if dependency.extension != "" {
			currentExtension, modeled := currentExtensions[dependency.extension]
			if !modeled || !reflect.DeepEqual(currentExtension,
				targetExtensions[dependency.extension]) {
				return fmt.Errorf("extension dependency %q is removed or incompatibly replaced", dependency.extension)
			}
			continue
		}
		switch dependency.kind {
		case "function":
			name := schema.SchemaQualifiedName{
				SchemaName:  dependency.schema,
				EscapedName: fmt.Sprintf("%s(%s)", schema.EscapeIdentifier(dependency.name), dependency.identityArguments),
			}
			currentFunction, modeled := currentFunctions[name.GetName()]
			if !modeled || !reflect.DeepEqual(currentFunction, targetFunctions[name.GetName()]) {
				return fmt.Errorf("function dependency %s is removed or incompatibly replaced", name.GetName())
			}
		case "type":
			name := schema.SchemaQualifiedName{
				SchemaName:  dependency.schema,
				EscapedName: schema.EscapeIdentifier(dependency.name),
			}
			if currentEnum, modeled := currentEnums[name.GetName()]; modeled {
				if !reflect.DeepEqual(currentEnum, targetEnums[name.GetName()]) {
					return fmt.Errorf("type dependency %s is removed or incompatibly replaced", name.GetName())
				}
			} else {
				return fmt.Errorf("user-defined type dependency %s is not a compatible modeled enum or extension type",
					name.GetName())
			}
		case "sequence":
			name := schema.SchemaQualifiedName{
				SchemaName:  dependency.schema,
				EscapedName: schema.EscapeIdentifier(dependency.name),
			}
			sequence, modeled := currentSequences[name.GetName()]
			ownedByGroup := false
			if modeled && sequence.Owner != nil {
				_, ownedByGroup = groupNames[sequence.Owner.TableName.GetFQEscapedName()]
			}
			if ownedByGroup {
				continue
			}
			if !modeled || !reflect.DeepEqual(sequence, targetSequences[name.GetName()]) {
				return fmt.Errorf("sequence dependency %s is removed or incompatibly replaced", name.GetName())
			}
		}
	}
	return nil
}

func renderArchivalSQLPreflight(sql *strings.Builder, group *archivalGroup) {
	root := group.marker.Root
	fmt.Fprintf(sql, `    IF cardinality(archived_tables) <> %d
	       OR (SELECT count(DISTINCT oid) FROM unnest(archived_tables) AS oid) <> %d THEN
		RAISE EXCEPTION 'archival source table set is incomplete';
	END IF;
	IF EXISTS (
		SELECT 1 FROM pg_catalog.pg_class
		WHERE oid = ANY(archived_tables) AND (relkind NOT IN ('r', 'p') OR relpersistence <> 'p')
	) THEN
		RAISE EXCEPTION 'archival source contains an unsupported table kind';
	END IF;
	IF (SELECT relispartition FROM pg_catalog.pg_class WHERE oid = %s::regclass) THEN
		RAISE EXCEPTION 'archival root has a retained parent';
	END IF;
	IF EXISTS (
		SELECT 1 FROM pg_catalog.pg_inherits
		WHERE (inhrelid = ANY(archived_tables)) <> (inhparent = ANY(archived_tables))
	) OR (SELECT count(*) FROM pg_catalog.pg_inherits
	      WHERE inhrelid = ANY(archived_tables) AND inhparent = ANY(archived_tables)) <> %d THEN
		RAISE EXCEPTION 'archival partition tree is incomplete or unsupported';
	END IF;
`, len(group.members), len(group.members), schema.EscapeLiteral(qualifiedArchivalName(root)), len(group.members)-1)
	sql.WriteString(`    IF EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger WHERE evtenabled <> 'D') THEN
		RAISE EXCEPTION 'enabled event triggers may affect archival DDL';
	END IF;
	IF EXISTS (
		SELECT 1 FROM pg_catalog.pg_constraint
		WHERE contype = 'f'
		  AND ((conrelid = ANY(archived_tables)) <> (confrelid = ANY(archived_tables)))
	) THEN
		RAISE EXCEPTION 'cross-boundary foreign keys are not supported for archival';
	END IF;
	IF EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE puballtables)
	   OR EXISTS (
		SELECT 1 FROM pg_catalog.pg_publication_tables AS published
		JOIN pg_catalog.pg_namespace AS n ON n.nspname = published.schemaname
		JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid AND c.relname = published.tablename
		WHERE c.oid = ANY(archived_tables)
	) THEN
		RAISE EXCEPTION 'publication membership is not supported for archival';
	END IF;
	IF EXISTS (SELECT 1 FROM pg_catalog.pg_statistic_ext WHERE stxrelid = ANY(archived_tables)) THEN
		RAISE EXCEPTION 'extended statistics are not supported for archival';
	END IF;
	IF EXISTS (
		SELECT moved.relname
		FROM pg_catalog.pg_class AS moved
		WHERE moved.oid = ANY(archived_tables)
		   OR moved.oid IN (SELECT indexrelid FROM pg_catalog.pg_index WHERE indrelid = ANY(archived_tables))
		   OR moved.oid IN (
			SELECT d.objid FROM pg_catalog.pg_depend AS d
			JOIN pg_catalog.pg_class AS sequence ON sequence.oid = d.objid AND sequence.relkind = 'S'
			WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass
			  AND d.refobjid = ANY(archived_tables) AND d.deptype IN ('a', 'i')
		   )
		GROUP BY moved.relname HAVING count(*) > 1
	) THEN
		RAISE EXCEPTION 'archived objects have conflicting destination names';
	END IF;
	IF EXISTS (
		SELECT 1 FROM pg_catalog.pg_depend AS d
		WHERE d.deptype = 'e' AND (
			(d.classid = 'pg_class'::regclass AND (
				d.objid = ANY(archived_tables)
				OR d.objid IN (SELECT indexrelid FROM pg_catalog.pg_index WHERE indrelid = ANY(archived_tables))
			))
			OR (d.classid = 'pg_constraint'::regclass AND d.objid IN (
				SELECT oid FROM pg_catalog.pg_constraint WHERE conrelid = ANY(archived_tables)
			))
			OR (d.classid = 'pg_trigger'::regclass AND d.objid IN (
				SELECT oid FROM pg_catalog.pg_trigger WHERE tgrelid = ANY(archived_tables)
			))
		)
	) THEN
		RAISE EXCEPTION 'extension-owned archived objects are not supported';
	END IF;
	IF EXISTS (
		SELECT 1
		FROM pg_catalog.pg_depend AS d
		JOIN pg_catalog.pg_rewrite AS r ON d.classid = 'pg_rewrite'::regclass AND d.objid = r.oid
		WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = ANY(archived_tables)
		  AND r.ev_class <> ALL(archived_tables)
	) OR EXISTS (
		SELECT 1 FROM pg_catalog.pg_depend AS d
		WHERE d.classid = 'pg_proc'::regclass AND d.refclassid = 'pg_class'::regclass
		  AND d.refobjid = ANY(archived_tables)
	) THEN
		RAISE EXCEPTION 'retained catalog-tracked object depends on an archived table';
	END IF;
`)
}
