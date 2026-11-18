// Package version provides build and version metadata injected at build time.
package version

var (
	// Version is the semantic release version of the binary.
	Version = "dev-0.0.0"

	// Commit is the Git commit hash of the build.
	Commit = "000000000000000000000000000000000badf00d"

	// Date is the ISO-8601 build timestamp.
	Date = "1970-01-01T00:00:01Z"

	// BuiltBy is the name of the builder or CI workflow.
	BuiltBy = "dev"
)

// ShortCommit returns a short 6-character commit hash prefix.
func ShortCommit() string {
	if len(Commit) >= 6 {
		return Commit[:6]
	}
	return Commit
}
