FROM perl:stable

RUN apt-get update && \
    apt-get install -y --no-install-recommends libdb-dev && \
    rm -rf /var/lib/apt/lists/* && \
    cpanm --notest BerkeleyDB FreezeThaw && \
    rm -rf /root/.cpanm

WORKDIR /usr/src/app
COPY . .
RUN chown -R root:root /usr/src/app && \
    chmod -R u=rwX,go=rX /usr/src/app && \
    chmod 0755 memcached.pl && \
    mkdir -p /data && chown 10001:10001 /data && chmod 0700 /data

USER 10001:10001

RUN cp -r /usr/src/app /tmp/build-test && \
    cd /tmp/build-test && prove t/ && \
    rm -rf /tmp/build-test

VOLUME /data
EXPOSE 11311
CMD ["perl", "memcached.pl", "--listen", "0.0.0.0", "--port", "11311", "--dbdir", "/data"]
