#!/usr/bin/env bash
set -euo pipefail

# Only GitHub's trusted main-branch job calls this publisher. Hosting credentials
# are not needed: Plesk pulls this public, generated branch over HTTPS.
branch="${DEPLOYMENT_BRANCH:-plesk-deploy}"
git check-ref-format --branch "$branch" >/dev/null
revision="$(git rev-parse HEAD)"
git fetch origin main
if [ "$revision" != "$(git rev-parse origin/main)" ]; then
    echo 'Skip an older revision: main has advanced.'
    exit 0
fi

stage="$(mktemp -d)"
trap 'git worktree remove --force "$stage" >/dev/null 2>&1 || rm -rf "$stage"' EXIT
if git ls-remote --exit-code --heads origin "$branch" >/dev/null 2>&1; then
    git fetch origin "$branch"
    git worktree add --detach "$stage" FETCH_HEAD
    git -C "$stage" rm -rf --ignore-unmatch . >/dev/null
else
    git worktree add --detach "$stage" "$revision"
    git -C "$stage" checkout --orphan deployment-build
    git -C "$stage" rm -rf --ignore-unmatch . >/dev/null
fi

git archive "$revision" | tar -x -C "$stage"
# Generated deployment commits must not contain workflows or private runtime data.
rm -rf "$stage/.github"
printf '%s\n' "$revision" > "$stage/DEPLOYED_REVISION"
(
    cd "$stage"
    npm ci --omit=dev --ignore-scripts --no-audit --no-fund
    git add -A
    git add -f node_modules
    if git diff --cached --quiet; then
        echo 'This revision is already published.'
        exit 0
    fi
    git -c user.name=github-actions\[bot\] \
        -c user.email=41898282+github-actions\[bot\]@users.noreply.github.com \
        commit -m "Deploy $revision"
    git push origin "HEAD:refs/heads/$branch"
)
