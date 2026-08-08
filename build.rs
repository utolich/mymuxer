fn main() {
    if std::env::var_os("CARGO_FEATURE_DVBCSA").is_none() {
        return;
    }

    let lib = pkg_config::Config::new()
        .probe("libdvbcsa")
        .or_else(|_| pkg_config::Config::new().probe("dvbcsa"));

    if lib.is_ok() {
        return;
    }

    let mut lib_dir = std::env::var_os("LIBDVBCSA_LIB_DIR").map(std::path::PathBuf::from);
    let mut include_dir = std::env::var_os("LIBDVBCSA_INCLUDE_DIR").map(std::path::PathBuf::from);

    if lib_dir.is_none() || include_dir.is_none() {
        if let Some(root) = std::env::var_os("LIBDVBCSA_DIR") {
            let root = std::path::PathBuf::from(root);
            if lib_dir.is_none() {
                lib_dir = Some(root.join("lib"));
            }
            if include_dir.is_none() {
                include_dir = Some(root.join("include"));
            }
        }
    }

    if let (Some(lib_dir), Some(include_dir)) = (lib_dir, include_dir) {
        if !lib_dir.exists() {
            println!(
                "cargo:warning=LIBDVBCSA_LIB_DIR does not exist: {}",
                lib_dir.display()
            );
        }
        if !include_dir.exists() {
            println!(
                "cargo:warning=LIBDVBCSA_INCLUDE_DIR does not exist: {}",
                include_dir.display()
            );
        }

        let header = include_dir.join("dvbcsa").join("dvbcsa.h");
        if !header.exists() {
            println!("cargo:warning=dvbcsa.h not found at {}", header.display());
        }

        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        println!("cargo:rustc-link-lib=dvbcsa");
        println!("cargo:include={}", include_dir.display());
        return;
    }

    let roots = ["/usr/local", "/usr"];
    for root in roots {
        let lib_dir = std::path::Path::new(root).join("lib");
        let include_dir = std::path::Path::new(root).join("include");
        let header = include_dir.join("dvbcsa").join("dvbcsa.h");
        let lib1 = lib_dir.join("libdvbcsa.so");
        let lib2 = lib_dir.join("libdvbcsa.a");

        if header.exists() && (lib1.exists() || lib2.exists()) {
            println!("cargo:rustc-link-search=native={}", lib_dir.display());
            println!("cargo:rustc-link-lib=dvbcsa");
            println!("cargo:include={}", include_dir.display());
            return;
        }
    }

    println!(
        "cargo:warning=libdvbcsa not found via pkg-config and no env paths set. \
         Set LIBDVBCSA_DIR or LIBDVBCSA_LIB_DIR/LIBDVBCSA_INCLUDE_DIR"
    );
}
