{
  description = "Storage gateways backed by pluggable framed stores";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
        version = cargoToml.workspace.package.version;
        mkPackage = { pname, description, cargoPackage ? pname }:
          pkgs.rustPlatform.buildRustPackage {
            inherit pname version;
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            cargoBuildFlags = [ "-p" cargoPackage ];
            cargoTestFlags = [ "-p" cargoPackage ];
            # libsqlite3-sys is vendored and reqwest uses rustls, so no system
            # SQLite/OpenSSL is needed.
            meta = {
              inherit description;
              license = with pkgs.lib.licenses; [ mit asl20 ];
              mainProgram = pname;
            };
          };
        filesDiscord = mkPackage {
          pname = "streamer-files-discord";
          description = "Files gateway backed by Discord webhooks";
        };
        s3Discord = mkPackage {
          pname = "streamer-s3-discord";
          description = "S3 gateway backed by Discord webhooks";
        };
        filesCli = mkPackage {
          pname = "streamer-files-cli";
          cargoPackage = "files-cli";
          description = "Command-line client for the files gateway";
        };
      in
      {
        packages.default = filesDiscord;
        packages.streamer-files-discord = filesDiscord;
        packages.streamer-s3-discord = s3Discord;
        packages.streamer-files-cli = filesCli;

        devShells.default = pkgs.mkShell {
          inputsFrom = [ filesDiscord s3Discord filesCli ];
          packages = [ pkgs.rust-analyzer pkgs.clippy pkgs.rustfmt ];
        };
      });
}
