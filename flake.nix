{
  description = "Batsched - Lucas Doctorate Project fork";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    kapack = {
      url = "github:oar-team/nur-kapack/master";
      flake = false;
    };
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-darwin" ];

      perSystem = { pkgs, system, ... }:
        let
          kapackSet = import inputs.kapack { inherit pkgs; };
          fixedRedox = kapackSet.redox.overrideAttrs (old: {
            cmakeFlags =
              let
                baseCmakeFlags = old.cmakeFlags or [];
              in
              baseCmakeFlags ++ [ "-DCMAKE_POLICY_VERSION_MINIMUM=3.5" ];
          });
          # gtest 1.17 needs C++17 CTAD but intervalset builds with -std=c++14,
          # so the unittest target fails to compile. Drop gtest so the test exe
          # is not built (gtest_dep.found() becomes false) and skip checks.
          withoutGtest = deps:
            pkgs.lib.filter (dep:
              let
                name = dep.pname or dep.name or "";
              in
              !(pkgs.lib.hasPrefix "gtest" name)
            ) deps;
          fixedIntervalset = kapackSet.intervalset.overrideAttrs (old: {
            buildInputs = withoutGtest (old.buildInputs or []);
            nativeBuildInputs = withoutGtest (old.nativeBuildInputs or []);
            propagatedBuildInputs = withoutGtest (old.propagatedBuildInputs or []);
            doCheck = false;
          });
          # loguru's build links libloguru.so without an absolute install_name,
          # so on Darwin dependents record the bare "libloguru.so" name. Set the
          # dylib id to its final path so linkers capture the full /nix/store path.
          fixedLoguru = kapackSet.loguru.overrideAttrs (old: {
            postFixup =
              let
                basePostFixup = old.postFixup or "";
              in
              basePostFixup + pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
                install_name_tool -id $out/lib/libloguru.so $out/lib/libloguru.so
              '';
          });
          makeBatsched = { debug, doCoverage ? false }:
            pkgs.callPackage ./nix/batsched.nix {
              batschedPackage = kapackSet.batsched;
              redox = fixedRedox;
              intervalset = fixedIntervalset;
              loguru = fixedLoguru;
              inherit debug doCoverage;
            };
          batsched = makeBatsched {
            debug = false;
            doCoverage = false;
          };
          batsched-debug = makeBatsched {
            debug = true;
            doCoverage = false;
          };
          batsched-coverage = makeBatsched {
            debug = true;
            doCoverage = true;
          };
        in {
          packages.default = batsched;
          packages.batsched = batsched;
          packages.batsched-debug = batsched-debug;
          packages.batsched-coverage = batsched-coverage;

          devShells.default = pkgs.mkShell {
            inputsFrom = [ batsched-debug ];
            packages = [
              pkgs.meson
              pkgs.ninja
              pkgs.pkg-config
            ];
          };
        };
    };
}
