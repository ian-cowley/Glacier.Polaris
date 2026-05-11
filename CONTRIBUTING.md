# Contributing to Glacier.Polaris

Thank you for your interest in contributing to Glacier.Polaris! We welcome contributions from the community.

## Getting Started

1. **Fork the repository** on GitHub.
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/your-username/Glacier.Polaris.git
   ```
3. **Build the project**:
   ```bash
   dotnet build -c Release
   ```
4. **Run the tests** to make sure everything passes:
   ```bash
   dotnet test -c Release
   ```

## Development Workflow

### Branching

- Create a feature branch from `master`: `git checkout -b feature/my-feature`
- Use descriptive branch names: `fix/`, `feature/`, `docs/`, `perf/`

### Code Style

- Follow existing code patterns and conventions
- Use nullable reference types (`?` annotations) throughout
- Use `Memory<T>` and `Span<T>` for performance-critical paths
- Enable implicit usings (`ImplicitUsings`)
- Maintain SIMD-friendly code where appropriate for compute kernels
- Keep methods focused and reasonably sized

### Testing

- **All new features must include tests.**
- We maintain two test categories:
  - **Unit tests**: Direct tests for kernels, expressions, and DataFrame operations
  - **Parity tests**: Golden-file verified tests that match Python Polars output
- Run the full test suite before submitting:
  ```bash
  dotnet test -c Release
  ```

### Parity Testing

If you add a new feature that has a Python Polars equivalent, please add a parity test:

1. Generate golden data using Python Polars (see `tests/parity/generate_parity_data.py`)
2. Add the corresponding C# test in `tests/Glacier.Polaris.Tests/ParityTests/ParityTests.cs`
3. Verify both the golden file and the C# test are committed

## Pull Request Process

1. Ensure your code builds and all tests pass
2. Update documentation if you're adding or changing public API surface
3. Write a clear PR description explaining what your changes do and why
4. Link any related issues

## Code Review

All submissions require review. We use GitHub pull requests for this process.
Be responsive to feedback and be willing to make changes if requested.

## Reporting Issues

- Use the GitHub issue tracker
- Search existing issues before filing a new one
- Include a clear description, reproduction steps, and environment details

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
