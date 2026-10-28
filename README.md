# memcached.pl

An implementation of the memcached text protocol in Perl with persistence using BerkeleyDB (`BerkeleyDB::Hash`).

Originally created for debugging/testing purposes by Yiorgos Adamopoulos (2012).

---

## Features & Persistence

- Implements key-value storage with persistence using a BerkeleyDB hash table (`memcache.db`).
- Supports text protocol commands over TCP socket (default port `11311`).
- Multi-process worker model for concurrent client handling.
- Process-safe CAS (Check-And-Set) unique counter generation.

---

## Docker Quickstart

You can build and run `memcached.pl` using the official `perl:stable` Docker image.

```bash
# Build the Docker image
docker build -t memcached-perl .

# Run the container
docker run -d -p 11311:11311 --name memcached-perl-app memcached-perl
```

Test the running container using `nc` (netcat) or `telnet`:

```bash
nc 127.0.0.1 11311
set mykey 0 0 5
hello
VALUE mykey 0 5
hello
END
quit
```

---

## Protocol Implementation Status

Below is the status of command implementation according to the official `protocol.txt` specification:

### Implemented Commands
- **`set <key> <flags> <exptime> <bytes> [noreply]`** - Stores data.
- **`add <key> <flags> <exptime> <bytes> [noreply]`** - Stores data only if key does not exist.
- **`replace <key> <flags> <exptime> <bytes> [noreply]`** - Stores data only if key exists.
- **`append <key> <flags> <exptime> <bytes> [noreply]`** - Appends data to existing key.
- **`prepend <key> <flags> <exptime> <bytes> [noreply]`** - Prepends data to existing key.
- **`cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]`** - Check-and-set operation.
- **`get <key>*`** - Retrieves item(s).
- **`gets <key>*`** - Retrieves item(s) with CAS unique identifier.
- **`delete <key> [noreply]`** - Explicitly deletes item.
- **`incr <key> <value> [noreply]`** - Increments numeric value.
- **`decr <key> <value> [noreply]`** - Decrements numeric value.
- **`flush_all`** - Flushes all items from cache.
- **`version`** - Returns server version string (`VERSION memcached.pl v0.1`).
- **`verbosity`** - Sets verbosity level (dummy/compatibility implementation).
- **`quit`** - Closes client connection.
- **`show_all`** - Extension command listing all items.

### Unimplemented / Not Implemented Features
- **`touch <key> <exptime> [noreply]`** - Updating item expiration time without fetching data.
- **`slabs reassign` & `slabs automove`** - Memory slab reassignment operations (not applicable to BerkeleyDB backend).
- **`stats` command & sub-arguments** (`stats items`, `stats sizes`, `stats slabs`, `stats settings`) - Returns `SERVER_ERROR command not implemented.`.
- **Delete Queues / Delayed Deletion** - Expiration and deletion are immediate without soft-delete hold period queues.
- **Delayed `flush_all <delay>`** - The numeric delay parameter for `flush_all` is not supported; flushes occur immediately.
- **UDP Protocol** - UDP datagram socket interface is not implemented (TCP only).

---

## Running Tests

Run the test suite using `perl`:

```bash
perl -I/path/to/perl5 t/01_protocol.t
```
