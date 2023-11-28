# MFSR

Sistema de arquivos para o projeto final de sistemas operacionais

# Dependências

- [libfuse](https://packages.ubuntu.com/search?keywords=libfuse)

# Como rodar

Instalar rust com [rustup](https://rustup.rs/)

```
rustup default stable
```

Clonar e rodar o projeto

```
git clone https://github.com/shinobu-uwu/mfsr
cd mfsr
cargo build --release
cd target/release
./mfsr --help
```

Ou baixar o binário em [releases](https://github.com/shinobu-uwu/mfsr/releases)
