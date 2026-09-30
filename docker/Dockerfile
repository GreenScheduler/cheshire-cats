# Dev image for cheshire-cats: the slurm-docker-cluster image (Slurm, libslurm,
# headers and munge already installed) plus a Rust toolchain and the libclang
# that bindgen needs. The daemon is built inside the container, against the
# cluster's own libslurm, because a binary built on the host needs a newer
# glibc than Rocky Linux 8 has.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN set -ex \
    && yum -y install clang clang-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    CARGO_TARGET_DIR=/target \
    PATH=/usr/local/cargo/bin:$PATH

RUN set -ex \
    && curl -sSf https://sh.rustup.rs \
       | sh -s -- -y --no-modify-path --profile minimal --default-toolchain stable \
    && rustup component add clippy \
    && rustc --version

COPY cheshire-entrypoint.sh /usr/local/bin/cheshire-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/cheshire-entrypoint.sh"]
WORKDIR /src
CMD ["sleep", "infinity"]
