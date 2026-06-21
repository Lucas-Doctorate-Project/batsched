{ lib
, boost
, cppzmq
, gmp
, meson
, ninja
, pkg-config
, rapidjson
, zeromq
, batschedPackage
, redox
, intervalset
, loguru
, debug ? false
, doCoverage ? false
}:

(batschedPackage.override {
  inherit redox;
}).overrideAttrs (old: rec {
  src = lib.sourceByRegex ../. [
    "^src"
    "^src/.*\.?pp"
    "^src/algo"
    "^src/algo/.*\.?pp"
    "^src/external"
    "^src/external/.*\.?pp"
    "^meson\.build"
  ];

  mesonFlags = (old.mesonFlags or [])
    ++ lib.optional doCoverage "-Db_coverage=true";
  nativeBuildInputs = [
    meson
    ninja
    pkg-config
  ];
  buildInputs = [
    boost
    gmp
    rapidjson
    intervalset
    loguru
    cppzmq
    zeromq
  ];
  mesonBuildType = if debug then "debug" else "release";
  hardeningDisable = [ "all" ];
  dontStrip = true;

  postInstall = lib.optionalString doCoverage ''
    mkdir -p $out/gcno
    cp batsched.p/*.gcno $out/gcno/
  '';

  passthru =
    let
      debugSrcDirs = [ "${src}/src" ];
    in
    (old.passthru or {}) // {
      hasDebugSymbols = debug;
      hasCoverage = doCoverage;
      DEBUG_SRC_DIRS = debugSrcDirs;
      GDB_DIR_ARGS = map (path: "--directory=" + path) debugSrcDirs;
    };
})
