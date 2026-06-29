# Frontend Workflow

This directory is frontend-only working scope for GaoshouPlatform production changes.

The default expectation is to deliver working frontend code, not just plans.

## Primary Goal

Use Gemini or other coding agents to improve frontend design quality, information hierarchy, responsiveness, and maintainability without changing business behavior.

## Hard Boundaries

- Do not change backend APIs, request payloads, response contracts, database schemas, auth flows, permissions, route semantics, or task scheduling behavior.
- Do not change business rules, factor logic, backtest logic, selection logic, state transitions, calculations, validation rules, or workflow meaning.
- Do not change button meaning, field meaning, form submission behavior, filter behavior, sorting behavior, pagination behavior, export behavior, or side effects of user actions.
- Do not rename business concepts unless the existing UI text is clearly inconsistent and the change is explicitly justified as a copy improvement only.
- If a visual improvement appears to require business-logic changes, stop and explain the tradeoff instead of implementing it.

## Allowed Changes

- Improve layout, spacing, alignment, typography, color usage, contrast, grouping, and visual hierarchy.
- Improve component structure and split overly large presentational components when behavior stays unchanged.
- Improve accessibility, keyboard usability, labels, focus states, empty states, loading states, error states, and responsive behavior.
- Reuse existing components, tokens, utilities, and patterns whenever possible.
- Add small presentational helpers only when existing abstractions are insufficient.

## Working Style

- First inspect the current page or component and summarize its existing flow before editing.
- Search for existing implementations before creating new helpers or patterns.
- Prefer small, reversible patches over broad rewrites.
- Keep changes local to the target page or its presentational dependencies unless a shared visual primitive is clearly the right reuse point.
- Avoid unrelated cleanup, broad refactors, and style churn outside the task scope.

## Design Direction

- Aim for a professional research and trading platform feel: calm, dense, legible, and intentional.
- Prioritize clarity over novelty.
- Strengthen information hierarchy for tables, filters, metrics, cards, tabs, and detail panels.
- Improve scanability for data-heavy screens.
- Preserve desktop productivity first, but ensure mobile and smaller laptop widths do not break.
- Prefer purposeful visual systems over one-off styling tweaks.

## Prompting Guidance For Agents

When working in this directory, follow this execution order:

1. Identify the target page, entry component, data source, and user actions.
2. List which behaviors must remain unchanged.
3. Propose a low-risk frontend-only improvement approach.
4. Implement the change with the smallest complete patch.
5. Verify that visual changes did not alter business behavior.

Use direct, specific instructions. Avoid vague goals like "redesign the whole app" or "clean up everything."

## Required Output Behavior

After making changes, always report:

- What frontend surfaces changed.
- Which business behaviors were intentionally preserved.
- What was verified.
- What remains unverified or risky.

## Escalation Rule

Pause and ask for confirmation before:

- touching shared app-wide layout primitives in a way that could affect many pages
- changing navigation structure
- changing copy that may affect business interpretation
- introducing a new design system layer or replacing an existing styling approach

## Definition Of Done

A task in this directory is only done when:

- the target UI is visually improved
- the existing business flow is preserved
- the changed frontend code is coherent with existing patterns
- the relevant verification has been completed or explicitly called out
