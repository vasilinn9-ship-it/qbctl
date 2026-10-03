fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc is available");
    std::env::set_var("PROTOC", protoc);

    prost_build::Config::new()
        .compile_protos(
            &[
                "../../proto/common.proto",
                "../../proto/torrent.proto",
                "../../proto/system.proto",
            ],
            &["../../proto"],
        )
        .expect("protobuf schemas compile");

    println!("cargo:rerun-if-changed=../../proto/common.proto");
    println!("cargo:rerun-if-changed=../../proto/torrent.proto");
    println!("cargo:rerun-if-changed=../../proto/system.proto");
}
