fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The schemas sit outside this package, so Cargo wouldn't otherwise notice them change.
    println!("cargo:rerun-if-changed=../../proto");
    tonic_prost_build::configure()
        .compile_protos(&["../../proto/client.proto", "../../proto/shard.proto"], &["../../proto"])?;
    Ok(())
}
