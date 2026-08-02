#!/usr/bin/env bash
# Regenerate the embedded third-party notices from every released build graph.
set -euo pipefail

export LC_ALL=C

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_dir"

go_licenses_module="github.com/google/go-licenses/v2"
go_licenses_version="$(go list -m -f '{{.Version}}' "$go_licenses_module")"
main_module="$(go list -m)"
output="internal/legal/THIRD_PARTY_NOTICES"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/k2a-token-sync-notices.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

mkdir -p "$work_dir/tools"
go_licenses="$work_dir/tools/go-licenses"
go build -mod=readonly -o "$go_licenses" "$go_licenses_module"

targets=(
	"linux amd64"
	"linux arm64"
	"darwin amd64"
	"darwin arm64"
)
target_root="$work_dir/targets"
mkdir -p "$target_root"

for target in "${targets[@]}"; do
	read -r goos goarch <<<"$target"
	target_dir="$target_root/${goos}-${goarch}"
	printf 'gen-notices: checking %s/%s licenses\n' "$goos" "$goarch" >&2
	env GOOS="$goos" GOARCH="$goarch" CGO_ENABLED=0 \
		"$go_licenses" check ./cmd --disallowed_types=forbidden,restricted,unknown
	printf 'gen-notices: collecting %s/%s notices\n' "$goos" "$goarch" >&2
	env GOOS="$goos" GOARCH="$goarch" CGO_ENABLED=0 \
		"$go_licenses" save ./cmd --save_path="$target_dir" --force
done

# go-licenses saves source files when a dependency has source-redistribution
# obligations. A text notice cannot satisfy those obligations, so fail closed
# rather than silently omitting the files. Such a dependency needs an explicit
# packaging design before it may enter the build.
unexpected="$(find "$target_root" -type f \
	! \( -iname 'LICENSE*' -o -iname 'COPYING*' -o -iname 'COPYRIGHT*' -o -iname 'NOTICE*' \) \
	-print -quit)"
if [[ -n "$unexpected" ]]; then
	printf 'gen-notices: dependency requires redistributing a non-notice file: %s\n' "$unexpected" >&2
	exit 1
fi

paths_file="$work_dir/paths"
for target_dir in "$target_root"/*; do
	find "$target_dir" -type f \
		\( -iname 'LICENSE*' -o -iname 'COPYING*' -o -iname 'COPYRIGHT*' -o -iname 'NOTICE*' \) \
		-print | while IFS= read -r path; do
			printf '%s\n' "${path#"$target_dir"/}"
		done
done | sort -u >"$paths_file"

generated="$work_dir/THIRD_PARTY_NOTICES"
{
	printf '%s\n' 'k2a-token-sync — third-party notices'
	printf '%s\n' '========================================'
	printf '\n'
	printf '%s\n' 'This binary links the following third-party software. The license texts,'
	printf '%s\n' 'copyright notices, and upstream NOTICE contents required for binary'
	printf '%s\n' 'redistribution are reproduced below.'
	printf '\n'
	printf 'Generated from the Linux and Darwin build graphs by google/go-licenses %s.\n' "$go_licenses_version"
	printf '\n'

	while IFS= read -r relative; do
		case "$relative" in
		"$main_module"/*) continue ;;
		esac

		source_file=""
		for target_dir in "$target_root"/*; do
			candidate="$target_dir/$relative"
			if [[ ! -f "$candidate" ]]; then
				continue
			fi
			if [[ -z "$source_file" ]]; then
				source_file="$candidate"
			elif ! cmp -s "$source_file" "$candidate"; then
				printf 'gen-notices: %s differs between build targets\n' "$relative" >&2
				exit 1
			fi
		done

		kind="$(basename "$relative" | tr '[:lower:]' '[:upper:]')"
		package="$(dirname "$relative")"
		printf '%s\n' '--------------------------------------------------------------------------------'
		printf '%s  (%s)\n' "$package" "${kind%%.*}"
		printf '%s\n' '--------------------------------------------------------------------------------'
		cat "$source_file"
		printf '\n'
	done <"$paths_file"
} >"$generated"

# Preserve the legal text and internal spacing while removing line-ending
# whitespace and terminal blank lines that would make the generated artifact
# fail Git's whitespace checks.
normalized="$work_dir/THIRD_PARTY_NOTICES.normalized"
awk '
	{
		sub(/[[:space:]]+$/, "")
		if ($0 == "") {
			blank++
			next
		}
		while (blank > 0) {
			print ""
			blank--
		}
		print
	}
' "$generated" >"$normalized"
mv "$normalized" "$generated"

mkdir -p "$(dirname "$output")"
if [[ ! -f "$output" ]] || ! cmp -s "$generated" "$output"; then
	cp "$generated" "$output"
fi

printf 'gen-notices: wrote %s (%s lines)\n' "$output" "$(wc -l <"$output" | tr -d ' ')" >&2
