# Contributing to Roost

Thanks for your interest in helping build Roost.

## Getting set up

```bash
git clone https://github.com/ashhadahsan/roost.git
cd roost
uv sync
uv run pre-commit install
```

You will need Docker running for the test suite (we use `testcontainers[postgres]` to spin up a real Postgres per test session — no mocks).

## Running checks

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest -n auto
```

`pre-commit run --all-files` runs the formatter + linter against your working tree.

## Style

- Ruff for lint + format. The configuration is in `pyproject.toml` — please don't override it locally.
- `mypy --strict` on `src/roost`. Tests are excluded from strict typing.
- Type hints on every public function. We ship a `py.typed` marker.

## Commit messages

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` — new user-visible feature
- `fix:` — bug fix
- `docs:` — documentation only
- `refactor:` — internal change with no user impact
- `test:` — tests only
- `chore:` — tooling / housekeeping

A clean commit history is more useful than a perfectly tidy PR. Please squash trivial fixups before requesting review.

## Pull requests

1. Fork + branch off `main`.
2. Make focused changes — one feature/fix per PR.
3. Add or update tests. PRs without tests will not be merged unless the change is genuinely untestable.
4. Update `CHANGELOG.md` under the `## Unreleased` section.
5. Open the PR; CI must be green.

## Reporting issues

- Bugs: include Python version, Postgres version, and a minimal reproducer.
- Feature requests: explain the use case before proposing the API. We optimize for ergonomics, not feature count.

## Code of conduct

Be kind. Assume good faith. Assume the other person knows something you don't.
