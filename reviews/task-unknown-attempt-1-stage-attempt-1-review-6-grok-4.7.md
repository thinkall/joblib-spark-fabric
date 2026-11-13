---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 6
timestamp: "2026-11-13T12:33:57Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "4c87ef4757b946b2af806db39a41c7292f4a854de0a1718e33fa527c20e813ae"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\code\joblib-spark\reviews\task-unknown-attempt-1-stage-attempt-1-review-6-grok-4.7.md
- **Issues Found**: 8
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `joblibspark/backend.py` submit/apply_async (lines 261-349), stage-scheduling warning (line 135), `joblibspark/__init__.py` import error (lines 33-36), `setup.py`, and `utils.py`
- [x] Read `README.md` (lines 78-94), `CHANGES.rst` (lines 4-5), `doc/installing.rst` (lines 1-2), `doc/guide.rst` (line 30), `doc/developing.rst` (line 6), `doc/index.rst` (line 27), `doc/why.rst` (lines 5-6), and `doc/conf.py` (lines 71, 233, 252-253)
- [x] Read `.github/workflows/main.yml` permissions (line 8), pylint (line 50), release-tag check (lines 98-104), and upload-artifact (line 119); read `.github/workflows/release.yml` permissions (lines 7, 20-21) and download-artifact (line 23)
- [x] Read new submission tests in `test/test_backend.py` (including lines 184-186) and the Parallel error assertion in `test/test_spark.py`; confirmed the Connect 4.0 shim is mocked, not executed against Spark 4.0
- [x] Confirmed new RST underlines are short: `CHANGES.rst` heading underline matches `^-{33}$` (title is 34 characters); `doc/installing.rst` underline matches `^={28}$` (title is 29 characters)
- [ ] Did not execute tests or workflows. Session notes say Linux GPU tests and the full Python/Spark matrix have not run locally; Spark 4.0 Connect is not in the supplied runtime evidence. This review does not treat that pending CI as completed evidence.

## Issues

### Issue 1: Release artifact upload and download lack Actions token permissions
- **Severity**: High
- **File**: `.github/workflows/main.yml`, `.github/workflows/release.yml`
- **Line(s)**: main.yml 8-9, 119-122; release.yml 7-8, 20-26
- **Description**: Both workflows set `permissions: contents: read`, which sets every unspecified `GITHUB_TOKEN` scope to none. `actions/upload-artifact@v4` requires `actions: write`. The publish job replaces permissions with only `id-token: write`, so `actions/download-artifact@v4` also lacks `actions: read`. A called workflow cannot raise permissions above the caller, so fixing only `main.yml` does not allow `release.yml` to upload. README lines 91-94 describe this path as the release mechanism.
- **Risk**: The package job fails on every push and pull request, so CI cannot go green. The publish job cannot download distributions, so v0.7.0 Trusted Publishing cannot succeed. This is a new workflow defect, not a runtime regression in the backend.
- **Suggested Fix**: On the `package` job set `contents: read` and `actions: write`. On the `checks` caller job grant those same scopes. On `publish` set `actions: read` and `id-token: write`. If a reusable-workflow artifact is still invisible to the caller, build the distributions in `release.yml` after checks instead of downloading them. Pass the tag in as a `workflow_call` input and fail closed when it is present but does not match `v{joblibspark.__version__}`; the current check skips when `RELEASE_TAG` is empty.

### Issue 2: Import failure tells managed-runtime users to install PySpark
- **Severity**: Medium
- **File**: `joblibspark/__init__.py`
- **Line(s)**: 33-36
- **Description**: Any `ImportError` from importing the backend, including a missing `joblib` or `packaging` module, is replaced with instructions to run `pip install joblibspark-fabric[spark]`. That extra installs `pyspark>=3.5,<=4.3.0`. The original exception is chained, but the displayed action contradicts README's rule not to replace PySpark in managed environments such as Fabric. Connect-only gaps are not mentioned.
- **Risk**: A Fabric user can follow the new message and upgrade or downgrade the managed Spark runtime. The hint also does not fix non-PySpark import failures.
- **Suggested Fix**: Include the original exception text in the message. Recommend `[spark]` only when the missing module is `pyspark` or `py4j` and no managed PySpark is already present. Tell managed-runtime users not to install that extra. Mention `[connect]` only for Connect dependency gaps.

### Issue 3: Stage-scheduling warning is emitted even when scheduling is enabled
- **Severity**: Medium
- **File**: `joblibspark/backend.py`
- **Line(s)**: 126-135
- **Description**: Pre-existing, not introduced by this diff. In non-local classic mode the code sets `support_stage_scheduling` from `hasattr(..., "withResources")` and then always warns "Spark version does not support stage-level scheduling." `local-cluster[...]`, which the tests use, does not match `local` or `local[`, so it takes this same branch.
- **Risk**: Every cluster backend startup logs a false warning. Operators can conclude that CPU/GPU resource profiles are disabled when `using_stage_scheduling` was set true. That hides real scheduling problems in production logs.
- **Suggested Fix**: Warn only when `support_stage_scheduling` is false, and include the master URL. Treat `local-cluster` like other local masters if that mode truly cannot use stage scheduling.

### Issue 4: New changelog and install headings have short RST underlines
- **Severity**: Low
- **File**: `CHANGES.rst`, `doc/installing.rst`, `doc/why.rst`
- **Line(s)**: CHANGES.rst 4-5; installing.rst 1-2; why.rst 5-6
- **Description**: `Release 0.7.0 (joblibspark-fabric)` is 34 characters and its underline is 33. `Installing joblibspark-fabric` is 29 characters and its underline is 28. Both are new. Pre-existing: `Joblibspark's approach` is 22 characters and its underline is 17.
- **Risk**: Docutils reports "Title underline too short." A Sphinx build with warnings-as-errors fails, and the new changelog section can be parsed as a broken heading.
- **Suggested Fix**: Make each underline at least as long as its title.

### Issue 5: README presents untested Python/Spark cross-products as CI-covered
- **Severity**: Low
- **File**: `README.md`
- **Line(s)**: 16-21, 78-81
- **Description**: The support table lists PySpark 4.0 on Python 3.11-3.13 and PySpark 4.1/4.2/4.3.0 on Python 3.11-3.14. CI tests every Spark line only on Python 3.11, and tests Python 3.12-3.14 only with Spark 4.3.0. The sentence that the matrix covers each Spark line and Python 3.11-3.14 reads as full cross-coverage. This is a documentation gap for untested cross-products, not evidence those combinations are incompatible and not a backend regression.
- **Risk**: Release notes overstate validation. A failure on Python 3.13 with Spark 4.0 or 4.1 would be outside the workflow that this paragraph implies is green.
- **Suggested Fix**: State that CI tests Spark 3.5-4.3.0 on Python 3.11, and Python 3.12-3.14 on Spark 4.3.0, in classic and supported Connect modes. Keep the support table only if it is explicitly labeled as PySpark's compatibility range rather than CI-verified coverage.

### Issue 6: Fork name is not consistent in package metadata or Sphinx docs
- **Severity**: Low
- **File**: `setup.py`, `doc/conf.py`, `doc/index.rst`, `doc/guide.rst`, `doc/developing.rst`
- **Line(s)**: setup.py 10-12; conf.py 71, 233, 252-253; index.rst 27; guide.rst 30; developing.rst 6
- **Description**: README and installing.rst were updated for `joblibspark-fabric`, but PyPI metadata still lists only the upstream author and Databricks email, with no maintainer or changelog URL. Sphinx still titles the project "Joblib", points `github_repo` at `joblib/joblib`, and uses `joblib.__version__`. `doc/guide.rst` still imports `parallel_backend` from `sklearn.utils`, which the README correctly moved to `joblib`. `doc/developing.rst` includes missing `README.rst`; `conf.py` swallows that copy failure, so the updated README never appears. The Sphinx drift is mostly pre-existing; the metadata gap is new with the rename.
- **Risk**: PyPI and in-repo docs send support and version questions to the upstream joblib project. Readers of the Sphinx guide follow a deprecated import that the release README no longer uses.
- **Suggested Fix**: Keep the upstream author for attribution, add maintainer and `project_urls` for this fork, and point Sphinx `project`, version, and GitHub links at `joblibspark-fabric`. Include `README.md` or drop the missing include. Use `from joblib import parallel_backend` in the guide.

### Issue 7: Spark Connect 4.0 session shim is silent
- **Severity**: Low
- **File**: `joblibspark/backend.py`
- **Line(s)**: 273-276, 337-344
- **Description**: New code calls private `_set_default_and_active_session` for Connect releases before 4.1, and selects a session-aware or callable-only `inheritable_thread_target` wrapper, without a log. `Version(pyspark.__version__)` is also parsed on every batch. Tests mock the private call; supplied runtime evidence covers Spark 4.3 Connect, not Spark 4.0 Connect.
- **Risk**: If a managed 4.0 Connect build lacks that private method, batches fail with a bare `AttributeError` and no indication that a compatibility shim ran. There is no driver log showing which inheritance path was chosen.
- **Suggested Fix**: Compute the version gate once on the backend. Log the selected inheritance path once at info or debug. If the private helper is missing, raise a `RuntimeError` that names Spark Connect 4.0 session propagation. Do not log per batch.

### Issue 8: CI repeats pylint on every matrix cell and runs twice on release
- **Severity**: Low
- **File**: `.github/workflows/main.yml`
- **Line(s)**: 4-6, 50-51
- **Description**: Pylint does not depend on Spark or Connect, but `./run-pylint.sh` runs in all 15 test cells. `on: push` has no branch filter, so publishing tag `v0.7.0` starts this workflow and `release.yml` starts it again through `workflow_call`. The old workflow also ran pylint inside its matrix; the tag-plus-release double run is new.
- **Risk**: Release CI costs roughly twice the matrix time, and identical pylint failures are duplicated across cells, which makes the first red log harder to see. This does not change runtime behavior.
- **Suggested Fix**: Run pylint once in the package job. Limit `push` to `main` so tag creation does not duplicate the release workflow's `workflow_call`.

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

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 7
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 8
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver resolutions

These dispositions supersede the initial Open placeholders. See lens 1 for the
single effort decision, runtime reviewer identities, sources, and aggregate gate.

| Issue | Disposition | What, why, and verification |
| --- | --- | --- |
| 1 | Rejected: incorrect artifact API premise | Runtime-token same-run artifact access does not require the suggested GITHUB_TOKEN write grants. The reusable workflow uses the caller github context; its release tag is available. Official source checks are linked in lens 1. |
| 2 | Fixed | Only missing root Spark dependencies receive the standalone-extra hint; it explicitly warns against replacing managed PySpark. Other import failures retain their original identity. Tests cover both paths. |
| 3 | Deferred: unchanged baseline warning | Verified at baseline `6e9931f7`, backend.py line 146. Not a new scheduling regression and not expanded in this compatibility change. |
| 4 | Fixed introduced headings | Changelog/install titles now match their underlines. The unrelated `doc/why.rst` heading is unchanged baseline content and deferred. |
| 5 | Fixed | README now explicitly states which Python/Spark axes and legacy edge cells CI exercises, and says it is not the full Cartesian product. |
| 6 | Fixed fork metadata and directly dependent example; deferred legacy Sphinx rebuild | Kept upstream attribution and added fork maintainer/source/changelog metadata. The guide now uses joblib and defines its example callable. Baseline `doc/conf.py` lines 71, 79, and 233 already name upstream joblib; its old README include and full Sphinx migration are independent existing issues. |
| 7 | Deferred optional hardening | Official Spark 4.0 source confirms the shim exists and does not overwrite an existing default session. The branch now has stronger child-thread/mapper tests and an actual 4.0 Connect CI gate. Additional cached flags and logging are not required for correctness and were not added speculatively. |
| 8 | Fixed new tag duplication; retained existing SDK lint coverage | `push.branches: ["**"]` preserves branch CI without starting a second matrix on release-tag pushes. Per-matrix pylint remains intentional because imports/type inference depend on the installed Spark SDK; it was also present before this change. |
