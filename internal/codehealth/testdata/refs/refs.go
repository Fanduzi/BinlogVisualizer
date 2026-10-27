// Package refs holds comments with and without issue references.
package refs

// Fixed in #12 after the review (counted).
// See §3 of the design (counted).
// An HTML entity &#123; and a C# mention are not references.
// A shebang like #!/bin/sh and a hash #abc are not references.
/* A block comment with (#7) is counted. */
func F() string {
	return "#99 in a string is not a comment"
}
