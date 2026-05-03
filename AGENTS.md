# AGENTS.md

## Setup Commands

- Install: `python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt`
- Start local AWS emulator: `docker compose up -d`
- Bootstrap local resources and sample data: `python3 pipeline.py --bootstrap`
- Publish sample events: `python3 pipeline.py`
- Run local EventBridge and Step Functions flow: `python3 local_eventbridge_runner.py`
- Run local downstream worker: `python3 ecs_worker.py`
- Test: `python3 -m unittest discover -s tests`
- E2E: `python3 e2e_test.py`
- Build: no build step detected

## Code Style

- AI-first development: minimize file fragmentation, reduce cognitive load and token use, keep related logic close, favor fast understanding and iteration, avoid over-engineering.
- Prefer simplicity over complexity, readability over cleverness, maintainability over theoretical purity, fast iteration over rigid structure, practical solutions over academic design.
- Use KISS, YAGNI, and DRY without premature abstraction.
- Treat Clean Code and Object Calisthenics as guidance, not dogma.
- Prefer fewer files with cohesive responsibility.
- Use one file per feature when clear.
- Keep related logic in the same file when practical.
- Avoid deep folders and excessive layers such as controller -> service -> usecase -> handler -> mapper -> utils.
- Split only when a file becomes hard to read, a component is truly reusable, or a real domain boundary exists.
- Use lightweight architecture. No rigid layered architecture.
- Keep business logic inline or near-inline when small.
- Extract only when complexity grows.
- Avoid boilerplate, over-layering, indirection, artificial abstractions, interface overuse, hidden side effects, and magic values.
- Code in English with descriptive names.
- Prefer simple Python scripts and cohesive files over extra layers.
- Keep AWS resource names, local defaults, validation helpers, and S3 helpers centralized in `aws_local.py`.
- Keep analytics write rules centralized in `analytics_writer.py`.
- Validate inputs at boundaries and raise meaningful `PipelineError` messages.
- Use small functions, early returns, and direct data structures.
- Comments should explain why, not restate what the code does.
- Follow patterns from `@context/knowledge/patterns/`.

## Response Style

- Caveman mode is active by default for every response.
- Default intensity: full.
- Stop caveman only when user says `stop caveman` or `normal mode`.
- Switch intensity when user says `/caveman lite`, `/caveman full`, or `/caveman ultra`.
- Drop articles, filler, pleasantries, and hedging unless clarity requires them.
- Use fragments when clear, short synonyms, and exact technical terms.
- Keep code blocks, commands, file paths, function names, API names, and error strings exact.
- Use pattern: `[thing] [action] [reason]. [next step].`
- Temporarily drop caveman for security warnings, irreversible action confirmations, ambiguous multi-step sequences, or when compression creates technical ambiguity.
- Resume caveman after clear part is done.
- Write code, commit messages, and PR text in normal professional style unless user asks otherwise.

## Context Files To Load

Before starting any work, load relevant context:

- `@context/.context-mesh-framework.md` (always)
- `@context/intent/project-intent.md` (always)
- `@context/intent/feature-*.md` (for specific feature)
- `@context/decisions/*.md` (relevant decisions)
- `@context/knowledge/patterns/*.md` (patterns to follow)
- `@context/evolution/changelog.md` (before and after changes)
- `@ project-context.md` / ` project-context.md` (source of truth for project goals; note the leading space in the filename)

## Project Structure

```text
root/
|-- AGENTS.md
|-- context/
|   |-- .context-mesh-framework.md
|   |-- intent/
|   |-- decisions/
|   |-- knowledge/
|   |   |-- patterns/
|   |   `-- anti-patterns/
|   |-- agents/
|   `-- evolution/
|-- config/
|   |-- products.json
|   `-- mappings/
|-- samples/
|-- tests/
|-- runtime/
|-- pipeline.py
|-- local_eventbridge_runner.py
|-- local_sfn_runner.py
|-- glue_landing.py
|-- glue_harmonization.py
|-- enrichment_batch.py
|-- analytics_writer.py
|-- analytics_queries.py
|-- glue_catalog.py
|-- ecs_worker.py
|-- evidence.py
|-- e2e_test.py
|-- state-machine.asl.json
|-- athena_views.sql
|-- docker-compose.yml
`-- requirements.txt
```

## AI Agent Rules

### Always

- Load Context Mesh before implementing.
- Load ` project-context.md` before planning, coding, or reviewing.
- Preserve AI-first development rules from `AGENTS.md` when updating Context Mesh or code.
- Respect the separation of feature, decision, and pattern files.
- Follow accepted decisions from `@context/decisions/`.
- Use patterns from `@context/knowledge/patterns/`.
- Keep feature docs high-level: what and why only.
- Keep technical choices in decision files.
- Keep code examples in pattern files.
- Update Context Mesh after any change.

### Never

- Ignore documented decisions.
- Put implementation details, library names, file paths, or code examples in feature files.
- Use anti-patterns from `@context/knowledge/anti-patterns/`.
- Leave context stale after changes.
- Hardcode secrets.
- Write high-volume rejected records to DynamoDB.
- Move product-specific mapping rules into core pipeline code unless a decision is added first.

### After Any Changes (Critical)

- Update relevant feature intent if functionality changed.
- Add or update decision files if a technical approach changed.
- Add or update pattern files if implementation conventions changed.
- Add outcomes to decision files if an accepted approach produced new learnings.
- Update `context/evolution/changelog.md`.
- Create learning files only when a significant insight should be preserved.

## Definition Of Done (Build Phase)

- [ ] Relevant context loaded.
- [ ] ADR exists before implementation when a technical choice is involved.
- [ ] Code follows documented patterns.
- [ ] Decisions respected.
- [ ] Tests passing or verification notes documented.
- [ ] Context updated to reflect changes.
- [ ] Changelog updated.

## Current Project Notes

- This is a local AWS-style POC, not a production deployment.
- MiniStack runs on `localhost:4566` through Docker Compose.
- Local `.parquet` files are JSON Lines with `.parquet` names in this POC.
- Products `orders` and `payments` are configured; `invoices` is intentionally unconfigured in samples.
- The shared business domain is `transaction`.
- Analytics currently uses two enriched datasets: runs and steps.
