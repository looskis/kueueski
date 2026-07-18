# Releasing kueueski

1. Replace `0.1.0` in `Cargo.toml` with the release version and commit it.
2. Run `cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`,
   and `cargo fmt --check`.
3. Tag the commit (for example, `git tag v0.1.0`) and push the tag.
4. The release workflow builds macOS Apple Silicon, macOS Intel, and Linux
   x86-64 archives and attaches SHA-256 checksums to a GitHub release.
5. Create or update a stable Homebrew formula from the tagged source archive:

   ```sh
   brew create https://github.com/kevinloo/kueueski/archive/refs/tags/v0.1.0.tar.gz \
     --set-name kueueski
   ```

   Keep the `depends_on "rust" => :build`, `cargo install` implementation, and
   version test from `Formula/kueueski.rb`. Publish the formula in a
   `homebrew-tap` repository for `brew install kevinloo/tap/kueueski`, or submit
   it to Homebrew core once the project meets its acceptance requirements.

Homebrew requires a stable tagged version and a formula test before a new
formula can be accepted into core.

