fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(false)
        .compile_protos(&["proto/etcd/rpc.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
