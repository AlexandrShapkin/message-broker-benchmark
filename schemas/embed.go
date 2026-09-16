package schemas

import "embed"

// Files contains the versioned public interchange schemas.
//
//go:embed *.json
var Files embed.FS
