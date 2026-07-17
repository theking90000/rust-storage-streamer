{ self }:
{ config, lib, pkgs, utils, ... }:

let
  inherit (lib) mkEnableOption mkIf mkMerge mkOption optionals types;

  cfg = config.services.rust-storage-streamer;
  system = pkgs.stdenv.hostPlatform.system;

  mkServiceOptions =
    {
      description,
      packageName,
      defaultPort,
      defaultStateDirectory,
      databaseFile,
      serviceCfg,
    }:
    {
      enable = mkEnableOption description;

      package = mkOption {
        type = types.package;
        default = self.packages.${system}.${packageName};
        description = "Package providing the ${packageName} executable.";
      };

      listenAddress = mkOption {
        type = types.str;
        default = "127.0.0.1";
        description = "Address on which ${description} listens.";
      };

      port = mkOption {
        type = types.port;
        default = defaultPort;
        description = "TCP port on which ${description} listens.";
      };

      webhooksFile = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "/run/secrets/discord-webhooks";
        description = ''
          Runtime path to a Discord webhooks file. The source file is loaded
          through systemd credentials and is not copied into the Nix store.
        '';
      };

      proxyUrls = mkOption {
        type = types.listOf types.str;
        default = [ ];
        example = [ "socks5h://127.0.0.1:25344" ];
        description = "Discord proxy URLs, passed as repeated --proxy-url arguments.";
      };

      stateDirectory = mkOption {
        type = types.strMatching "[A-Za-z0-9_.-]+";
        default = defaultStateDirectory;
        description = "StateDirectory name below /var/lib managed by systemd.";
      };

      databaseUrl = mkOption {
        type = types.str;
        default = "sqlite:///var/lib/${serviceCfg.stateDirectory}/${databaseFile}?mode=rwc";
        description = "SQLite connection URL used by ${description}.";
      };

      openFirewall = mkOption {
        type = types.bool;
        default = false;
        description = "Whether to open the configured TCP port in the NixOS firewall.";
      };

      extraArgs = mkOption {
        type = types.listOf types.str;
        default = [ ];
        example = [ "--frame-size" "65536" ];
        description = "Additional command-line arguments appended after managed options.";
      };
    };

  proxyArgs = proxyUrls:
    lib.concatMap (proxyUrl: [ "--proxy-url" proxyUrl ]) proxyUrls;

  webhooksCredential = serviceCfg:
    if serviceCfg.webhooksFile == null then "/dev/null" else serviceCfg.webhooksFile;

  mkService =
    {
      description,
      serviceCfg,
      user,
      executable,
      arguments,
    }:
    {
      inherit description;
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];

      serviceConfig = {
        User = user;
        Group = user;

        StateDirectory = serviceCfg.stateDirectory;
        StateDirectoryMode = "0700";
        WorkingDirectory = "/var/lib/${serviceCfg.stateDirectory}";
        UMask = "0077";

        LoadCredential = [ "webhooks:${webhooksCredential serviceCfg}" ];
        ExecStart = utils.escapeSystemdExecArgs ([
          "${serviceCfg.package}/bin/${executable}"
        ] ++ arguments);

        Restart = "on-failure";
        RestartSec = "5s";

        AmbientCapabilities = "";
        CapabilityBoundingSet = "";
        LockPersonality = true;
        MemoryDenyWriteExecute = true;
        NoNewPrivileges = true;
        PrivateDevices = true;
        PrivateTmp = true;
        ProtectControlGroups = true;
        ProtectHome = true;
        ProtectHostname = true;
        ProtectKernelLogs = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        ProtectSystem = "strict";
        RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        SystemCallArchitectures = "native";
      };
    };
in
{
  options.services.rust-storage-streamer = {
    files = mkServiceOptions {
      description = "Rust Storage Streamer files gateway";
      packageName = "streamer-files-discord";
      defaultPort = 8080;
      defaultStateDirectory = "rust-storage-streamer-files";
      databaseFile = "catalog.db";
      serviceCfg = cfg.files;
    };

    s3 = mkServiceOptions {
      description = "Rust Storage Streamer S3 gateway";
      packageName = "streamer-s3-discord";
      defaultPort = 8081;
      defaultStateDirectory = "rust-storage-streamer-s3";
      databaseFile = "s3-catalog.db";
      serviceCfg = cfg.s3;
    };
  };

  config = mkMerge [
    {
      environment.systemPackages =
        optionals cfg.files.enable [ cfg.files.package ]
        ++ optionals cfg.s3.enable [ cfg.s3.package ];

      networking.firewall.allowedTCPPorts = lib.unique (
        optionals (cfg.files.enable && cfg.files.openFirewall) [ cfg.files.port ]
        ++ optionals (cfg.s3.enable && cfg.s3.openFirewall) [ cfg.s3.port ]
      );
    }

    (mkIf cfg.files.enable {
      assertions = [
        {
          assertion = cfg.files.webhooksFile != null;
          message = "services.rust-storage-streamer.files.webhooksFile must be set when the files service is enabled.";
        }
      ];

      users.groups."rust-storage-streamer-files" = { };
      users.users."rust-storage-streamer-files" = {
        isSystemUser = true;
        group = "rust-storage-streamer-files";
      };

      systemd.services."rust-storage-streamer-files" = mkService {
        description = "Rust Storage Streamer files gateway";
        serviceCfg = cfg.files;
        user = "rust-storage-streamer-files";
        executable = "streamer-files-discord";
        arguments = [
          "--bind"
          "${cfg.files.listenAddress}:${toString cfg.files.port}"
          "--database-url"
          cfg.files.databaseUrl
          "--webhooks-file"
          "%d/webhooks"
        ] ++ proxyArgs cfg.files.proxyUrls ++ cfg.files.extraArgs;
      };
    })

    (mkIf cfg.s3.enable {
      assertions = [
        {
          assertion = cfg.s3.webhooksFile != null;
          message = "services.rust-storage-streamer.s3.webhooksFile must be set when the S3 service is enabled.";
        }
      ];

      users.groups."rust-storage-streamer-s3" = { };
      users.users."rust-storage-streamer-s3" = {
        isSystemUser = true;
        group = "rust-storage-streamer-s3";
      };

      systemd.services."rust-storage-streamer-s3" = mkService {
        description = "Rust Storage Streamer S3 gateway";
        serviceCfg = cfg.s3;
        user = "rust-storage-streamer-s3";
        executable = "streamer-s3-discord";
        arguments = [
          "--database-url"
          cfg.s3.databaseUrl
          "serve"
          "--bind"
          "${cfg.s3.listenAddress}:${toString cfg.s3.port}"
          "--webhooks-file"
          "%d/webhooks"
        ] ++ proxyArgs cfg.s3.proxyUrls ++ cfg.s3.extraArgs;
      };
    })
  ];
}
