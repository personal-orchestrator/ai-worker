# AI Worker Testing Rules

- When writing tests for the personal orchestrator projects (such as `ai-worker`), prefer end-to-end build time tests (similar to Spring integration tests) rather than unit tests with excessive mocking. The goal is to test the actual wiring and logic as much as possible.
