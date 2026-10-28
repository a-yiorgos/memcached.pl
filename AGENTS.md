# AGENTS.md

## Project
memcached.pl is a single-file Perl implementation of the memcached **text** protocol. It persists data to BerkeleyDB (CDB mode) and forks one worker per connection. It is meant for debugging and testing, not production. `protocol.txt` is the authoritative spec. When the code and the spec disagree, the spec wins.

## Scope
- In scope: set, add, replace, append, prepend, cas, get, gets, delete, incr, decr, flush_all, version, verbosity, quit, and the `show_all` extension.
- Out of scope (do not implement, stub or "partially support"): the binary protocol, UDP, touch, stats, slabs, and the `flush_all <delay>` argument.
- Do not add commands, options or features that were not asked for.

## Layout
- `memcached.pl` — the server. Keep it a single file.
- `t/` — Test::More tests. `t/lib/TestServer.pm` holds the shared server harness.
- `Dockerfile`, `.dockerignore`, `.gitignore`, `README.md`, `protocol.txt` (do not edit protocol.txt).

## Dependencies
- Perl 5 core modules, plus BerkeleyDB and FreezeThaw. Nothing else from CPAN.
- System packages: `libdb-dev`. On Debian/Ubuntu: `apt-get install libberkeleydb-perl libfreezethaw-perl`.

## Commands
```sh
perl -c memcached.pl                                     # syntax check
prove -v t/                                              # full test suite
perl memcached.pl --listen 127.0.0.1 --port 11311 --dbdir ./data
DOCKER_TESTS=1 prove -v t/09_container.t                 # container tests
docker build -t memcached-perl .
docker run --rm -p 11311:11311 --read-only --cap-drop=ALL \
  --security-opt=no-new-privileges -v mcdata:/data memcached-perl
```

## Architecture invariants
These must hold after every change.
1. **Writes only under --dbdir.** The server writes nothing to `/tmp`, the cwd, or anywhere else.
2. **No shared BerkeleyDB handles across fork().** Each worker opens its own env and db after `fork()`. The parent closes any handles it opened before entering the accept loop.
3. **Every check-then-write runs entirely inside `cds_lock`.** This covers add, replace, append, prepend, cas, incr/decr, lazy expiry deletion and CAS generation. Never read state outside the lock and then write based on it.
4. **CAS values come from a counter in a separate BDB database in the same env.** The counter is incremented under `cds_lock`, values are strictly increasing, and they survive restarts. No helper processes, no pipes.
5. **The stream stays in sync after every error.** A storage command whose `<bytes>` parsed always has its data block consumed, even when the command is rejected. After any error the connection must still accept the next command.
6. **Replies are byte-exact to protocol.txt.** Unknown commands get exactly `ERROR\r\n`. `noreply` suppresses every non-error reply, including the result of incr/decr.
7. **Input never produces Perl warnings,** whatever it is. `use strict; use warnings;` stay on.

## Code style
- Match the existing style: tabs, `m_<command>` handler subs, and a dispatch block in the worker loop.
- Prefer `defined()` checks over truthiness wherever a value can be `"0"` or `""`.
- Keep changes minimal and focused. Do not reformat untouched code.

## Testing rules
- Install Perl in the VM and test; then test inside Docker
- Tests run against a real `memcached.pl` process over TCP, started by `t/lib/TestServer.pm`. No mocks.
- The harness uses a free port and a `File::Temp` dbdir. It never uses 11311, `/tmp/memcache.db` or the repo directory.
- Every socket read has a hard timeout (5s by default). A timeout is a test failure, never a hang.
- No `sleep` for synchronisation. The only exception is exptime tests, which may sleep up to 2s.
- Assertions compare full replies byte-for-byte, including `\r\n`.
- The server's stderr must be empty at the end of every test file.
- Bug fixes are test-first. Write a test that fails on the current code, commit it, then fix the code.
- Never weaken, skip, `TODO` or delete an assertion to get a green run. If a test is wrong, explain why in the PR.
- The full suite must pass 3 consecutive times before a change is complete.

## Container rules
- The container runs rootless as numeric `USER 10001:10001`. Only package installation runs as root.
- `/data` is chowned to 10001 **before** `VOLUME /data`.
- App files are owned by root and are not writable by the runtime user.
- No capabilities, setcap, sudo or gosu. The image must work with `--read-only --cap-drop=ALL --security-opt=no-new-privileges`, and under rootless Podman.
- The image build runs `prove t/` as the unprivileged user. A failing test fails the build.

## Documentation
- The README lists only behaviour that a test covers.
- Example sessions in the README must be real transcripts, not hand-written approximations.

## Commits and PRs
- Use small, focused commits. Tests go in separate commits from fixes.
- The PR description lists each change, the tests covering it, and their fail-before/pass-after status.
- Do not commit `data/`, BDB environment files (`__db.*`) or build artifacts.
