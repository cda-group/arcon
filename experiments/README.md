# Arcon Experiments

Note that binaries compiled from this directory will end up in the root of the Cargo workspace. That is, execution-plane/.

## NEXMark

Building:

```
$ cargo build --release
```

See flags:

```bash
$ ./target/release/nexmark run --help
```

Run Default ConcurrenyConversion Query with Default NEXMark/ArconConf

```bash
$ ./target/release/nexmark run
```

Run Query 1 with debug mode on:

```bash
$ ./target/release/nexmark run -q 1 -d
```

Run Query 1 with custom NEXMark config:

```bash
$ ./target/release/nexmark run -q 1 -c /path/to/nexmark_toml
```

Run Query 1 with tui disabled:

```bash
$ ./target/release/nexmark run -t
```

Log NEXMarkConfig and ArconConf:

```bash
$ # Done by setting RUST_LOG to info
$ RUST_LOG=info ./target/release/nexmark run -t
```
