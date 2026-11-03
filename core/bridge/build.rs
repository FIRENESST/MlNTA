fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost = prost_build::Config::new();
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .bytes(".minta.bridge.v1.Header.value")
        .bytes(".minta.bridge.v1.Packet.data")
        .compile_with_config(prost, &["proto/bridge.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/bridge.proto");
    Ok(())
}
