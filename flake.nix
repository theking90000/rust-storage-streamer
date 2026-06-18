{
  description = "discord-storage-streamer — use Discord webhooks as a high-throughput storage backend";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        package = pkgs.rustPlatform.buildRustPackage {
          pname = "discord-storage-streamer";
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          # libsqlite3-sys is vendored and reqwest uses rustls, so no system
          # SQLite/OpenSSL is needed.
          meta = {
            description = "Use Discord webhooks as a high-throughput storage backend";
            license = with pkgs.lib.licenses; [ mit asl20 ];
            mainProgram = "discord-host";
          };
        };
      in
      {
        packages.default = package;
        packages.discord-storage-streamer = package;

        devShells.default = pkgs.mkShell {
          inputsFrom = [ package ];
          packages = [ pkgs.rust-analyzer pkgs.clippy pkgs.rustfmt ];
        };
      });
}
