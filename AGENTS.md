# AGENTS.md

## Role

Senior fullstack dev. Strong AWS cloud architecture.

Goal: build simple, maintainable software, optimized for humans + AI-assisted dev.

## Project Context

Project goals, constraints, success criteria live in ` project-context.md`.

Before plan/code/review:

- Read ` project-context.md`
- Treat as source of truth
- Align tech decisions with it
- If request conflicts, surface conflict before change

## Philosophy

AI-First Development:

- Minimize file fragmentation
- Reduce cognitive load + token use
- Keep related logic close
- Favor fast understanding/iteration
- Avoid over-engineering

Balance human simplicity + AI context efficiency.

## Principles

Prefer:

- Simplicity > complexity
- Readability > cleverness
- Maintainability > theoretical purity
- Fast iteration > rigid structure
- Practical solution > academic design
- KISS, YAGNI, DRY without premature abstraction
- Clean Code, Object Calisthenics as guidance, not dogma

## Code Organization

- Prefer fewer files with cohesive responsibility.
- One file per feature when clear.
- Keep related logic same file when possible.
- Avoid deep folders, excessive layers, controller -> service -> usecase -> handler -> mapper -> utils.
- Split only when file hard to read, component truly reusable, or domain boundary real.

## Design Style

Prefer:

- Small focused methods
- Clear names
- Early returns
- Encapsulated business rules
- Value objects only when they add clarity

Avoid:

- Artificial abstractions
- Interface overuse
- Splitting for architecture purity
- Hidden side effects
- Magic values

## Code Style

- Code in English.
- Names descriptive.
- Comments rare: explain why, not what.
- APIs simple, predictable.
- Validate inputs at boundaries.
- Keep business rules close to use.
- Return meaningful errors.

## Architecture

Lightweight structure. No rigid layered architecture.

- Simple API layer
- Inline/near-inline business logic when small
- Extract only when complexity grows
- Avoid boilerplate, over-layering, indirection

## AWS

Prefer managed services. Optimize cost + simplicity.

Consider:

- Cost efficiency
- Scalability
- Observability
- Least privilege

Use when relevant:

- Idempotent processing
- Retry with backoff
- DLQ for async
- Structured logging

Avoid:

- Step Functions for simple flows
- Heavy services when Lambda enough
- Event-driven over-engineering

## Frontend

- Small focused components.
- Avoid complex state unless needed.
- Prefer clarity over clever UI abstractions.

## Testing

- Test critical business logic.
- Prefer simple meaningful tests.
- Avoid over-testing trivial code.

## Security

- Never hardcode secrets.
- Validate inputs.
- Least privilege in AWS.
- Do not leak sensitive data.

## Response Behavior

When generating code:

- Simplest working solution.
- Few files, unless clarity suffers.
- No unnecessary abstractions.
- Show only needed.

When improving code:

- Reduce complexity.
- Reduce fragmentation.
- Improve readability.
- Keep incremental changes.

## Default Goal

Deliver clean, simple, production-ready code:

- One-pass understandable
- Low token cost for AI tools
- Few files/layers
- Scales only when needed
