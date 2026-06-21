---
name: project-knowledge
description: |
  Use when you need information about this project's architecture, tech stack,
  coding patterns, data model, deployment setup, git workflow, or UX guidelines.
  Contains comprehensive project documentation including design decisions,
  technical specifications, and development standards.
---

# Project Knowledge

This skill provides access to project documentation that defines how this project works, how code should be written, and how features should be developed.

## When to use

Activate this skill when you need to:
- Understand project architecture, tech stack, and data model
- Learn coding patterns, git workflow, and testing approach
- Check deployment setup, monitoring, and operational procedures
- Apply UX guidelines and design system
- Make technical decisions aligned with project standards

## Core references

All documentation is in the `references/` folder:

- **[project.md](references/project.md)** - Project overview, purpose, target audience, core features, scope boundaries
- **[architecture.md](references/architecture.md)** - Tech stack, project structure, dependencies, external integrations, data flow, data model (schema, migrations, sensitive data)
- **[patterns.md](references/patterns.md)** - Project-specific coding conventions, git workflow (branching, testing, security gates), testing & verification methods, business rules, reference implementations
- **[deployment.md](references/deployment.md)** - Deployment platform, environment variables, CI/CD triggers, rollback, monitoring & observability (logging, error tracking, health checks)

## Optional references

- **[ux-guidelines.md](references/ux-guidelines.md)** - Interface language, tone of voice, domain glossary, text patterns, design system (only for projects with significant UI)
- **{custom}.md** - Domain-specific files added per project (e.g., vault.md, bot.md, mcp.md)

## How to use

Read specific guides as needed for your task:

- Starting feature development - read [project.md](references/project.md), [architecture.md](references/architecture.md), [patterns.md](references/patterns.md)
- Implementing database changes - read [architecture.md](references/architecture.md) (Data Model section)
- Working on UI/UX - read [ux-guidelines.md](references/ux-guidelines.md)
- Setting up deployment - read [deployment.md](references/deployment.md)
- Creating branches or PRs - read [patterns.md](references/patterns.md) (Git Workflow section)
- Investigating logs or errors - read [deployment.md](references/deployment.md) (Monitoring section)
- Working with domain logic - read [patterns.md](references/patterns.md) (Business Rules section)

All guides are maintained as single source of truth for project knowledge.
