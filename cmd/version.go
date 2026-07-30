package main

import (
	"fmt"
	"runtime/debug"
)

// version is overridden at release time via -ldflags. For builds without it —
// including `go install` — the module's VCS stamp is used instead.
var version = ""

func versionString() string {
	if version != "" {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	var revision, modified string
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			if setting.Value == "true" {
				modified = "-dirty"
			}
		}
	}
	if revision != "" {
		if len(revision) > 12 {
			revision = revision[:12]
		}
		return fmt.Sprintf("devel-%s%s", revision, modified)
	}
	return "devel"
}
