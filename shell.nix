{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
    buildInputs = [
        pkgs.rustup
        pkgs.cmake
        pkgs.clang
        pkgs.binutils
        pkgs.gnumake
        pkgs.protobuf # needed for prost to build
        pkgs.perl # needed for grpcio to build
        pkgs.cacert # needed for the codegen test
    ];

    PROTOC = "${pkgs.protobuf}/bin/protoc"; # protoc included in prost crate has a loader that is incompatible with nixos
}