
# Installation

## Binary distribution

* Download latest binary distribution from https://github.com/spirali/hyperqueue/releases/latest
* Unpack the downloaded archive:

   ``$ tar -xvzf hq-<version>-linux-x64.tar.gz``


## Compilation from source codes

* Requirements: Git, [Rust](https://www.rust-lang.org/tools/install)

* Clone HyperQueue repository:

  ``$ git clone https://github.com/spirali/hyperqueue/``

* Build project:

  ``$ cargo build --release``

* Final executable file will in ``./target/release/hq``