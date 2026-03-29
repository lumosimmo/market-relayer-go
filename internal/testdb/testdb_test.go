package testdb

import (
	"net/url"
	"strings"
	"testing"
)

func TestSchemaDSNAddsSearchPath(t *testing.T) {
	t.Parallel()

	dsn := schemaDSN(defaultBaseDSN, "test_schema")
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}

	if got := parsed.Query().Get("search_path"); got != "test_schema" {
		t.Fatalf("schemaDSN() search_path = %q, want %q", got, "test_schema")
	}
}

func TestSchemaDSNPreservesBracketedIPv6Host(t *testing.T) {
	t.Parallel()

	dsn := schemaDSN("postgres://postgres:postgres@[::1]:5432/market_relayer_test?sslmode=disable", "test_schema")
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}

	if got, want := parsed.Host, "[::1]:5432"; got != want {
		t.Fatalf("schemaDSN() host = %q, want %q", got, want)
	}
	if got := parsed.Query().Get("search_path"); got != "test_schema" {
		t.Fatalf("schemaDSN() search_path = %q, want %q", got, "test_schema")
	}
}

func TestUniqueSchemaNormalizesName(t *testing.T) {
	t.Parallel()

	got := uniqueSchema(`A Name/With Symbols and Extremely Long Tail That Should Be Trimmed`)
	if !strings.HasPrefix(got, "t_a_name_with_symbols_and_extremely_long_") {
		t.Fatalf("uniqueSchema() = %q, want normalized prefix", got)
	}
}

func TestSchemaIdentifierUsesPGXSanitization(t *testing.T) {
	t.Parallel()

	if got := schemaIdentifier(`a"b`); got != `"a""b"` {
		t.Fatalf("schemaIdentifier() = %q, want %q", got, `"a""b"`)
	}
}
