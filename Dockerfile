FROM perl:stable

# Install Berkeley DB system headers and libraries
RUN apt-get update && \
    apt-get install -y --no-install-recommends libdb-dev && \
    rm -rf /var/lib/apt/lists/*

# Install required Perl modules from CPAN
RUN cpanm --notest BerkeleyDB FreezeThaw

WORKDIR /usr/src/app

COPY . /usr/src/app

EXPOSE 11311

CMD ["perl", "memcached.pl"]
