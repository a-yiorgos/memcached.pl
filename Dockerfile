FROM perl:stable

# Install Berkeley DB system headers and libraries
RUN apt-get update && \
    apt-get install -y --no-install-recommends libdb-dev && \
    rm -rf /var/lib/apt/lists/*

# Install required Perl modules from CPAN
RUN cpanm --notest BerkeleyDB FreezeThaw

WORKDIR /usr/src/app

COPY . /usr/src/app

# Create data directory with proper ownership for rootless execution
RUN mkdir -p /data && chown -R 10001:10001 /data /usr/src/app

VOLUME /data

USER 10001:10001

EXPOSE 11311

CMD ["perl", "memcached.pl", "--listen=0.0.0.0", "--dbdir=/data"]
