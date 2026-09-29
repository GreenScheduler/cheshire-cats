//! Embeds an rpath to the libslurm the build linked against, so the daemon
//! loads the same library at run time without LD_LIBRARY_PATH.

fn main() {
    println!("cargo:rerun-if-env-changed=DEP_SLURM_LIB_DIR");
    if let Ok(dir) = std::env::var("DEP_SLURM_LIB_DIR") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
}
