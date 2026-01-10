# Contributing to FlyMQ

Thank you for your interest in contributing to FlyMQ! This document provides
guidelines and information for contributors.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and
inclusive environment for all contributors.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/flymq`
3. Create a feature branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Run tests: `go test ./...`
6. Commit your changes: `git commit -m "Add your feature"`
7. Push to your fork: `git push origin feature/your-feature`
8. Open a Pull Request

## Development Setup

### Prerequisites

- Go 1.21 or later
- Make (optional)

### Building

```bash
# Build all binaries
make build

# Run tests
make test

# Run linter
make lint
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run specific package tests
go test ./internal/storage/...
```

## Code Style

### Go Code

- Follow the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- Use `gofmt` for formatting
- Use `golint` and `go vet` for linting
- Write meaningful comments for exported functions and types
- Keep functions focused and small

### Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters
- Reference issues and pull requests when relevant

Example:
```
Add TLS support for client connections

- Implement NewClientTLSConfig for client-side TLS
- Add TLSEnabled option to ClientOptions
- Update server to support TLS listeners

Fixes #123
```

## Pull Request Process

1. Ensure all tests pass
2. Update documentation if needed
3. Add tests for new functionality
4. Keep PRs focused on a single feature or fix
5. Request review from maintainers

### PR Checklist

- [ ] Tests pass locally
- [ ] Code follows project style guidelines
- [ ] Documentation updated (if applicable)
- [ ] Changelog updated (if applicable)
- [ ] No breaking changes (or clearly documented)

## Reporting Issues

### Bug Reports

Include:
- Go version (`go version`)
- Operating system
- Steps to reproduce
- Expected behavior
- Actual behavior
- Relevant logs or error messages

### Feature Requests

Include:
- Use case description
- Proposed solution (if any)
- Alternatives considered

## Architecture Decisions

For significant changes, please open an issue first to discuss the approach.
This helps ensure your contribution aligns with the project direction.

## License

By contributing to FlyMQ, you agree that your contributions will be licensed
under the Apache License 2.0.

## Questions?

Open an issue with the `question` label or reach out to the maintainers.

