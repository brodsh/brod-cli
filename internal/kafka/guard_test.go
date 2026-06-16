package kafka_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestNoMessageConsumptionPath enforces the read-only / metadata-only pillars at
// build time: NO code path anywhere in the repo may consume topic data or issue
// a Fetch for message payloads. If someone adds a consumer against customer
// topics, this test fails. (Per CLAUDE.md hard rule.)
func TestNoMessageConsumptionPath(t *testing.T) {
	root := repoRoot(t)

	// Forbidden identifiers that signal message-payload consumption across the
	// common Go Kafka clients (sarama, kafka-go, confluent-kafka-go, franz-go).
	forbidden := []*regexp.Regexp{
		regexp.MustCompile(`\bNewConsumer\b`),
		regexp.MustCompile(`\bNewConsumerGroup\b`),
		regexp.MustCompile(`\bConsumePartition\b`),
		regexp.MustCompile(`\bReadMessage\b`),
		regexp.MustCompile(`\bFetchMessage\b`),
		regexp.MustCompile(`\bPollFetches\b`),  // franz-go consume entrypoint
		regexp.MustCompile(`\bPollRecords\b`),  // franz-go consume entrypoint
		regexp.MustCompile(`\bConsumeTopics\b`), // kgo.ConsumeTopics option = configures a consumer
		regexp.MustCompile(`\bConsumePartitions\b`),
		regexp.MustCompile(`\bConsumerGroup\b`), // kgo.ConsumerGroup option = joins a group to consume
		regexp.MustCompile(`\bsarama\.NewConsumer`),
		regexp.MustCompile(`kafka\.NewReader`), // segmentio/kafka-go reader = consumer
	}

	walkGoFiles(t, root, func(path string, src []byte) {
		// Skip this guard file itself (it names the forbidden idents on purpose).
		if strings.HasSuffix(path, "guard_test.go") {
			return
		}
		text := string(src)
		for _, re := range forbidden {
			if re.MatchString(text) {
				t.Errorf("FORBIDDEN message-consumption path %q found in %s — read-only/metadata-only pillar violated", re.String(), rel(root, path))
			}
		}
	})
}

// TestKafkaPackageExposesNoConsumerEntrypoint asserts, at the API level, that
// the kafka package exports no method/function whose name implies consuming
// message data. This complements the repo-wide text scan above: even if a future
// edit introduced a "Consume"/"Fetch"/"Poll"/"Read" method, this fails the build.
func TestKafkaPackageExposesNoConsumerEntrypoint(t *testing.T) {
	root := repoRoot(t)
	pkgDir := filepath.Join(root, "internal", "kafka")

	// Names that, as an exported identifier in this package, would imply a
	// message-consumption capability. (Read-only metadata methods are named
	// Brokers/Topics/Offsets/Groups/etc. — none of these.)
	bad := regexp.MustCompile(`^(Consume|Poll|Fetch[A-Z].*Records|ReadMessage|ReadRecord)`)

	fset := token.NewFileSet()
	entries, err := os.ReadDir(pkgDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(pkgDir, e.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", e.Name(), err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() {
				continue
			}
			if bad.MatchString(fn.Name.Name) {
				t.Errorf("kafka package exports %q — looks like a message-consumption entrypoint; read-only/metadata-only pillar violated", fn.Name.Name)
			}
		}
	}
}

// --- helpers shared by tests in this package ---

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found walking up from test dir")
		}
		dir = parent
	}
}

func rel(root, path string) string {
	r, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return r
}

func walkGoFiles(t *testing.T, root string, fn func(path string, src []byte)) {
	t.Helper()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == ".git" || info.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		// Validate it parses (and so our scans are over real Go).
		if _, perr := parser.ParseFile(token.NewFileSet(), path, src, parser.PackageClauseOnly); perr != nil {
			return nil
		}
		fn(path, src)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
