# memcached.pl - Memcached Text Protocol Implementation in Perl

An implementation of the [memcached text protocol](http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt) in Perl with persistence using DB_File.

## Overview

**This implementation's primary purpose is to provide the `show_all` command**, which is not available in standard memcached. This allows inspection of all keys and values in the database, making it ideal for debugging, testing, and development purposes.

The implementation has been enhanced with complete protocol support, improved signal handling, and strict protocol compliance.

## Key Feature: show_all Command

The `show_all` command is the **main reason this implementation exists**. Standard memcached does not provide a way to list all keys in the database.

**Usage:**
```
show_all
```

**Response:**
Returns all keys and their values in the format:
```
VALUE <key> <flags> <bytes>\r\n<value>\r\n
...
END\r\n
```

This is the same format as the `get` command, making it compatible with standard memcached clients that parse VALUE responses.

## Additional Features

Beyond the core `show_all` functionality, this implementation includes:

- **Complete protocol support**: All commands described in protocol.txt are implemented
- **Improved signal handling**: Proper graceful shutdown and zombie process prevention
- **Protocol compliance**: Key validation, data block validation, expiration handling
- **Persistence**: Uses DB_File for data storage

## Design Choices

### Signal Handling

- **REAPER handler**: Uses `waitpid(-1, WNOHANG)` in a loop to reap all waiting child processes, preventing zombie accumulation
- **Graceful shutdown**: SIGTERM and SIGINT handlers close listening socket, signal child processes, and perform cleanup
- **SIGPIPE**: Set to 'IGNORE' to prevent crashes on client disconnection
- **Non-blocking accept**: Uses `select()` with timeout to allow periodic shutdown checks
- **Per-child signal handlers**: Connection children have their own signal handlers for clean termination

### Protocol Compliance

#### Expiration Time Handling
- Fixed expiration constant to correct value: `60 * 60 * 24 * 30` (2592000 seconds = 30 days)
- Expiration logic: values ≤ 30 days are treated as relative offsets; values > 30 days are treated as Unix timestamps

#### Key Validation
- Maximum length: 250 characters (per protocol)
- No control characters (ASCII 0-31, 127/DEL)
- No whitespace characters

#### Data Block Validation
- Reads exactly `<bytes> + 2` bytes from socket (data + `\r\n`)
- Validates data block ends with `\r\n`
- Returns appropriate CLIENT_ERROR on malformed data

#### append/prepend Commands
- **Fixed**: Now correctly accept only `<key> <bytes> [noreply]` (not `<key> <flags> <exptime> <bytes> [noreply]`)
- Preserves existing key's flags and exptime when updating value

### Command Implementation

#### Fully Implemented Commands
- `set <key> <flags> <exptime> <bytes> [noreply]` - Store data
- `add <key> <flags> <exptime> <bytes> [noreply]` - Store if not exists
- `replace <key> <flags> <exptime> <bytes> [noreply]` - Store if exists
- `append <key> <bytes> [noreply]` - Append data (fixed protocol compliance)
- `prepend <key> <bytes> [noreply]` - Prepend data (fixed protocol compliance)
- `cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]` - Check-and-set
- `get <key>*` - Retrieve data
- `gets <key>*` - Retrieve data with CAS unique
- `delete <key> [noreply]` - Delete key
- `incr <key> <value> [noreply]` - Increment numeric value
- `decr <key> <value> [noreply]` - Decrement numeric value
- `touch <key> <exptime> [noreply]` - Update expiration time
- `stats [args]` - Server statistics
- `flush_all [delay] [noreply]` - Invalidate all items
- `version` - Server version
- `verbosity <level>` - Set verbosity (dummy implementation)
- `quit` - Close connection
- `slabs reassign <source> <dest>` - Memory redistribution (stub)
- `slabs automove <1|0>` - Auto slab movement (stub)

#### Command-Specific Notes
- **flush_all**: Accepts optional delay parameter for protocol compliance, but performs immediate invalidation (simplified implementation)
- **stats**: Returns comprehensive statistics including general, settings, items, and slabs
- **slabs commands**: Stub implementations that return OK without actual slab management

### Error Handling

- **ERROR**\r\n: Returned for nonexistent commands
- **CLIENT_ERROR** <error>\r\n: Returned for client input errors (malformed commands, invalid keys, etc.)
- **SERVER_ERROR** <error>\r\n: Returned for server errors (unimplemented features)

### Persistence

- Uses `DB_File` with `O_RDWR|O_CREAT` and mode `0666`
- Database file: `$dbdir/memcache.db` (default: `./memcache.db`)
- Data stored using `Storable::freeze` for serialization
- CAS unique values persisted via separate child process

### Process Architecture

- **Main process**: Accepts connections, forks child processes
- **Connection children**: Handle client requests, exit after completion
- **CAS child**: Dedicated process for generating unique CAS values
- All child processes tracked in `%children` hash for cleanup

## Usage

```bash
# Start server with default settings
perl memcached.pl

# Start with custom options
perl memcached.pl --listen 0.0.0.0 --port 11211 --dbdir /var/lib/memcached

# Connect with telnet
 telnet localhost 11311
```

## Command Line Options

- `--listen <host>`: Listen address (default: 127.0.0.1)
- `--port <port>`: Listen port (default: 11311)
- `--dbdir <dir>`: Database directory (default: .)

## Known Limitations

1. **Simplified flush_all**: Delay parameter accepted but not implemented; all items are invalidated immediately
2. **Slab management**: slabs reassign and automove are stub implementations
3. **No binary protocol**: Only text protocol is supported
4. **No authentication**: No SASL or other authentication mechanisms
5. **Single-threaded**: Uses fork() per connection (not thread-based like official memcached)

## Development Notes

- The `show_all` command (documented above) is the primary feature of this implementation
- **Testing**: Use `telnet` or any memcached client that supports the text protocol

## License

MIT License - see LICENSE file for details.

## Original Author

Yiorgos Adamopoulos - [blog.postmaster.gr](http://blog.postmaster.gr/2012/03/20/memcached-pl-an-incomplete-implementation-in-perl-with-persistence/)

## Contributions

Signal handling improvements, protocol compliance fixes, and command implementations added by Mistral Vibe.
