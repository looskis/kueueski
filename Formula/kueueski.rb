class Kueueski < Formula
  desc "Fast command-line client for BullMQ queues"
  homepage "https://github.com/lookevink/kueueski"
  license "MIT"
  head "https://github.com/lookevink/kueueski.git", branch: "main"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args(path: ".")
  end

  test do
    assert_match "kueueski", shell_output("#{bin}/kueueski --version")
    assert_match "status", shell_output("#{bin}/kueueski --help")
  end
end
