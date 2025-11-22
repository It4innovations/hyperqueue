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
    $ cargo build --profile=dist
    ```

4. Use the executable located in `./target/dist/hq`

### Build dependencies

Apart from a Rust compiler, the default build configuration also depends on some C and C++ dependencies ([Jemalloc](https://github.com/jemalloc/jemalloc) and [Highs](https://highs.dev/)), which require a recent C/C++ compiler and also CMake to be built. If you encounter issues trying to compile these dependencies, you can build HyperQueue without them using the following command:

```bash
$ cargo build --profile=dist --no-default-features --features dashboard,microlp
```

This will come at some cost to runtime performance.

Note that even if HyperQueue is built with the C/C++ dependencies, they are always statically linked, so they are not required during runtime, only at build time.
