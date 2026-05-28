{
  description = "Batsched - Lucas Doctorate Project fork";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    kapack = {
      url = "github:oar-team/nur-kapack/master";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, kapack, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-darwin" ];
      forEachSystem = f: nixpkgs.lib.genAttrs systems f;
    in {
      packages = forEachSystem (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          kapackSet = import kapack { inherit pkgs; };
          fixedRedox = kapackSet.redox.overrideAttrs (old: {
            cmakeFlags = (old.cmakeFlags or []) ++ [ "-DCMAKE_POLICY_VERSION_MINIMUM=3.5" ];
          });
          # gtest 1.17 needs C++17 CTAD but intervalset builds with -std=c++14,
          # so the unittest target fails to compile. Drop gtest so the test exe
          # is not built (gtest_dep.found() becomes false) and skip checks.
          fixedIntervalset = kapackSet.intervalset.overrideAttrs (old: {
            buildInputs = [];
            doCheck = false;
          });
          # loguru's build links libloguru.so without an absolute install_name,
          # so on Darwin dependents record the bare "libloguru.so" name. Set the
          # dylib id to its final path so linkers capture the full /nix/store path.
          fixedLoguru = kapackSet.loguru.overrideAttrs (old: {
            postFixup = (old.postFixup or "") + nixpkgs.lib.optionalString pkgs.stdenv.isDarwin ''
              install_name_tool -id $out/lib/libloguru.so $out/lib/libloguru.so
            '';
          });
          jobs = import ./release.nix { kapack = kapackSet; };
          # kapack hands batsched its deps via callPackage, so the instances in
          # buildInputs are not == kapackSet.{intervalset,redox,loguru}.
          # Match by name to reliably swap in the fixed derivations.
          replaceDep = dep:
            let n = dep.pname or dep.name or "";
            in if nixpkgs.lib.hasPrefix "redox" n then fixedRedox
               else if nixpkgs.lib.hasPrefix "intervalset" n then fixedIntervalset
               else if nixpkgs.lib.hasPrefix "loguru" n then fixedLoguru
               else dep;
          batsched = jobs.batsched.overrideAttrs (old: {
            buildInputs = map replaceDep (old.buildInputs or []);
            nativeBuildInputs = map replaceDep (old.nativeBuildInputs or []);
          });
        in {
          default = batsched;
          inherit batsched;
        }
      );
    };
}
