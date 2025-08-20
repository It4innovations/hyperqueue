## Binary distribution (recommended)
The easiest way to install **HyperQueue** is to download and unpack the prebuilt `hq` executable:

1. Download the latest release archive from [this link](https://github.com/It4innovations/hyperqueue/releases/latest).

    !!! info "Target architecture"
    
        Make sure to choose the correct binary for your architecture. Currently, we provide prebuilt binaries for `x86-64`, `AArch64` (ARM) and `PowerPC` architectures.

2. Unpack the downloaded archive:

    ```bash
    $ tar -xvzf hq-<version>-linux-<architecture>.tar.gz
    ```
    
    The archive contains a single binary `hq`, which is used both for deploying the *HQ* cluster and submitting tasks into *HQ*.
    You can add `hq` to your system `$PATH` to make its usage easier.

See [Quickstart](quickstart.md) for an example "Hello world" HyperQueue computation.

## Compilation from source code

You can also compile HyperQueue from source. This allows you to build HyperQueue for architectures for which we do not
provide prebuilt binaries. It can also generate binaries with support for vectorization, which could in theory improve
the performance of HyperQueue in extreme cases.

1. Setup a [Rust toolchain](https://www.rust-lang.org/tools/install)
2. Clone the HyperQueue repository:

    ```bash
    $ git clone https://github.com/It4innovations/hyperqueue/
    ```

3. Build HyperQueue:

    ```bash
    $ RUSTFLAGS="-C target-cpu=native" cargo build --release
    ```

    ??? tip "Jemalloc dependency"
    
        HyperQueue by default depends on the [Jemalloc](https://github.com/jemalloc/jemalloc) memory allocator, which is a
        C library. If you're having problems with installing HyperQueue because of this dependency, you can opt-out of it and
        use the default system allocator by building *HQ* with `--no-default-features`:
    
        ```bash
        $ cargo build --release --no-default-features
        ```

5. Use the executable located in `./target/release/hq`
