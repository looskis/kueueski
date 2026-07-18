# Releasing kueueski

## One-time setup

1. Create the public GitHub repository `lookevink/homebrew-tap` with an initial
   README.
2. Create a fine-grained GitHub token that can write repository contents in
   that tap.
3. Add it to the `kueueski` repository as the Actions secret
   `HOMEBREW_TAP_TOKEN`.

The generated release workflow will then publish each stable formula to the tap
automatically.

## Cut a release

1. Replace `0.1.0` in `Cargo.toml` with the release version and commit it.
2. Run `cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`,
   `cargo fmt --check`, and `dist plan`.
3. Tag the commit (for example, `git tag v0.1.0`) and push the tag.
4. `cargo-dist` builds macOS Apple Silicon, macOS Intel, and Linux x86-64
   archives, creates the GitHub release, and publishes the generated formula to
   `lookevink/homebrew-tap`.
5. Verify the public path with `brew install lookevink/tap/kueueski`.

Run `dist init` again when upgrading the `cargo-dist-version` in
`dist-workspace.toml`; the release workflow is generated code.
