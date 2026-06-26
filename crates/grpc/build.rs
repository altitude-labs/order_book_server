fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build scripts run single-threaded for this crate; setting PROTOC
    // before invoking prost-build only affects this build process and children.
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/orderbook.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/orderbook.proto");
    Ok(())
}
