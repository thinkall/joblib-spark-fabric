---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 1
timestamp: "2026-11-13T12:45:31Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "e80d0a5b89dd452e9cc9dcfc3b646048db5d5d1860ae261791cbbea52e43433d"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\code\joblib-spark\reviews\task-unknown-attempt-1-stage-attempt-1-review-1-grok-4.7.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `.github/workflows/main.yml` permissions (`contents: read` only) and the `package` job `actions/upload-artifact@v4` step that uploads `python-package-distributions`.
- [x] Read `.github/workflows/release.yml`: `checks` calls `main.yml` with no job permissions, and `publish` sets only `id-token: write` before `actions/download-artifact@v4`.
- [x] Read `joblibspark/backend.py` `submit`/`apply_async`, the Spark Connect `< (4, 1)` session call, and the `inheritable_thread_target` session vs callable branch.
- [x] Read `test/test_backend.py` and `test/test_spark.py` for the new submit, session, error, and pinned-thread cases. Reported 3.5/4.3 runs were not re-executed.
- [x] Read `setup.py`, `pyproject.toml`, and `joblibspark/__init__.py`: distribution `joblibspark-fabric`, import `joblibspark`, version `0.7.0`, PySpark kept optional.
- [x] Read `doc/installing.rst` title markup and `doc/guide.rst` scikit-learn example against the updated `README.md` import.
- [ ] Spark 4.0 Connect private-session path, classic pinned-thread Spark 4, and Linux GPU tests were not independently run. Do not treat pending CI as runtime proof.

## Issues

### Issue 1: Release artifact upload and download lack Actions token permissions
- **Severity**: High
- **File**: `.github/workflows/main.yml`, `.github/workflows/release.yml`
- **Line(s)**: main.yml 8-9 and 119-122; release.yml 7-12 and 20-26
- **Description**: Both workflows set an explicit `permissions` block, which disables every unlisted `GITHUB_TOKEN` scope. `main.yml` grants only `contents: read`, but `actions/upload-artifact@v4` needs `actions: write`. The `publish` job replaces workflow permissions with only `id-token: write`, so `actions/download-artifact@v4` also lacks `actions: read`. A called reusable workflow cannot exceed the caller's token, and `checks` does not grant `actions: write`, so fixing only `main.yml` still fails on the release path.
- **Risk**: The `package` job fails on every CI run, `needs: checks` never succeeds, and Trusted Publishing cannot download the distributions. v0.7.0 cannot be published as written.
- **Suggested Fix**: Grant `contents: read` and `actions: write` in `main.yml`, and the same scopes on the `checks` caller job. On `publish`, set both `id-token: write` and `actions: read` (add `attestations: write` if the current PyPI action generates attestations). Skip artifact upload on fork pull requests, where write is denied. If download still cannot see the called workflow's artifact, pass `run-id: ${{ github.run_id }}` and `github-token: ${{ github.token }}`.

### Issue 2: User guide still imports a scikit-learn parallel backend the fork no longer uses
- **Severity**: Low
- **File**: `doc/guide.rst`
- **Line(s)**: 24-32
- **Description**: This is pre-existing guide text, not a new runtime regression, but the fork update left it stale. `README.md` and `test/test_spark.py` now use `joblib.parallel_backend`, while the guide still shows `from sklearn.utils import parallel_backend`. Current scikit-learn, which the CI `test` job installs unpinned, does not provide that `sklearn.utils` export. The same example also calls undefined `inc`, so the doctest cannot pass.
- **Risk**: Users following the shipped guide on a supported scikit-learn install fail before the Spark backend runs, despite the README migration instructions.
- **Suggested Fix**: Match the README: import `parallel_backend` from `joblib`, define `inc` if the snippet is meant to execute, and keep the `register_spark()` step.

### Issue 3: Install page title underline is shorter than the title
- **Severity**: Low
- **File**: `doc/installing.rst`
- **Line(s)**: 1-2
- **Description**: `Installing joblibspark-fabric` is 29 characters. The underline is 28 `=` characters. Docutils treats a short underline as a title error. This was introduced by the fork rename.
- **Risk**: Sphinx/`rst` builds warn or fail on the install page. It does not affect the runtime package.
- **Suggested Fix**: Make the underline at least as long as the title.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary-agent estimate; no human tier selection or repository policy found.",
    "reviewer": "review-code Medium complementary-family policy and current task-tool model catalog.",
    "reasoning": "Maximum supported for grok-4.7 in the current task-tool catalog: xhigh.",
    "stageBehavior": "review-code Medium: six independent concurrent lenses.",
    "safetyStops": "Existing session and execution controls; no additional user ceiling specified."
  },
  "changeSize": "Moderate multi-file fork change: one small Python backend, focused tests, distribution metadata, documentation, and two GitHub workflows. About 385 added and 206 removed tracked lines plus three small new files.",
  "cyclomaticComplexity": "Moderate compatibility branching and interactions between joblib callbacks, thread-pool exceptions, classic Spark thread inheritance, and Spark Connect sessions. No new scheduler or unbounded state machine.",
  "rationale": "Medium reflects the breadth and compatibility/failure-path risks, while the implementation remains bounded to one backend and straightforward packaging/release automation. Linux GPU and full version-matrix validation remain CI gates.",
  "primaryModel": "gpt-6.1-sol",
  "reviewers": [
    {"lens": 1, "model": "grok-4.7", "reasoning": "xhigh"},
    {"lens": 2, "model": "grok-4.7", "reasoning": "xhigh"},
    {"lens": 3, "model": "grok-4.7", "reasoning": "xhigh"},
    {"lens": 4, "model": "grok-4.7", "reasoning": "xhigh"},
    {"lens": 5, "model": "grok-4.7", "reasoning": "xhigh"},
    {"lens": 6, "model": "grok-4.7", "reasoning": "xhigh"}
  ],
  "overrideSource": "None. User requested commit, push, and release only after successful validation. No AGENTS.md or repository-local review policy exists.",
  "uncertainty": "The complete Linux/GPU/version matrix has not yet run; it must pass after push before publishing v0.7.0."
}

## Driver resolution and gate

The original reviewer response and verdict above are preserved. This section
supersedes the initial Open placeholders. The Medium decision is recorded here
once; the other artifacts retain their own provenance and individual resolutions.

All six independent lenses completed with persisted `issues_found` results and
runtime identity `grok-4.7` at `xhigh`. Transport inspection confirmed only
`view`, `grep`, and `glob`, with no sibling feedback reads. No numeric ADO task
was invented for this standalone OSS maintenance change.
Broad files-with-matches searches in two lenses returned feedback filenames
only; the audit confirmed no finding text was returned and neither file was viewed.

| Lens | Outcome | Resolution artifact |
| --- | --- | --- |
| 1 | issues_found, resolved below | This file |
| 2 | issues_found, resolved | review-2-grok-4.7.md |
| 3 | issues_found, resolved | review-3-grok-4.7.md |
| 4 | issues_found, resolved | review-4-grok-4.7.md |
| 5 | issues_found, resolved | review-5-grok-4.7.md |
| 6 | issues_found, resolved | review-6-grok-4.7.md |

| Issue | Disposition | What, why, and verification |
| --- | --- | --- |
| 1 | Rejected: incorrect API premise | Artifact v4 uses the Actions runtime token for same-run upload/download. The reusable workflow's jobs belong to the caller run. No `github-token`, cross-run request, or GitHub attestation-store operation is used. Retained least privilege rather than add unnecessary write access. See the authoritative sources below; actual CI and release remain gates. |
| 2 | Fixed | Updated `doc/guide.rst` to import from joblib and define `inc`, matching the current README/API. The rest of the pre-existing Sphinx migration is outside this compatibility change. |
| 3 | Fixed | Corrected the new RST underlines; title/underline lengths are 29/29 and 34/34. |

Authoritative checks:

- [download-artifact v4 inputs and same-run behavior](https://github.com/actions/download-artifact/blob/v4/README.md): `github-token` and `actions:read` are for a different run/repository.
- [artifact runtime authentication](https://github.com/actions/toolkit/blob/main/packages/artifact/src/internal/shared/config.ts): the artifact client reads `ACTIONS_RUNTIME_TOKEN`, not the workflow's `GITHUB_TOKEN` scopes.
- [reusable workflow context](https://docs.github.com/en/actions/reference/workflows-and-actions/reusing-workflow-configurations#github-context): `github` is associated with the caller, including its event and run identity.

Confirmed findings were reproduced before fixes. Managed joblib 1.6 aborts read
the removed `_backend_args`; driver `SystemExit` lost a ThreadPool result.
Additional managed-reuse testing exposed stale callbacks appending old results.
The fixes support both kwargs attribute names, terminate/reset the aborted pool,
and convert non-Exception failures to chained RuntimeError while preserving
ordinary exceptions. Import failures now avoid replacing managed PySpark.

Post-fix validation:

- Python 3.13/current joblib: 19 focused tests passed.
- Python 3.14/current joblib: 19 focused tests passed.
- Python 3.11/joblib 1.0.1: 15 passed, four optional pandas-dependent cases skipped.
- Real managed Parallel failure/reuse passed on Spark 3.5/joblib 1.0.1 and Spark 4.3/Python 3.14, classic and Connect. Cancellation assertions also passed.
- Pylint: 10/10. Wheel, sdist and twine checks passed.
- Added release-gating CI cells for legacy joblib on Spark 3.5 classic and Spark 4.0 Connect.

**Gate:** required Medium review is complete and introduced findings are resolved
or factually rejected. Original reviewer verdicts are not relabeled clean.
The identified unrelated baseline issues remain explicitly deferred in the
individual artifacts. Full GitHub CI must pass before publication.
