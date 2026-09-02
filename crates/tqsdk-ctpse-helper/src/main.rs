fn main() {
    if let Err(error) = tqsdk_ctpse_helper::run_from_env() {
        eprintln!("tqsdk-ctpse-helper: {error}");
        std::process::exit(1);
    }
}
