#!/usr/bin/perl

use strict;
use warnings;
use lib 't/lib';
use TestServer;
use Test::More;

my $server = TestServer->new();
my $sock = $server->connect();

# Helper to verify connection stays healthy after bad input
sub assert_connection_alive {
    my ($msg) = @_;
    print $sock "version\r\n";
    my $line = read_line($sock);
    like($line, qr/^VERSION /, "$msg - connection stays responsive");
}

# 1. Invalid key validation: whitespace, control chars, key too long (>250) or empty
print $sock "set invalid key 0 0 3\r\nfoo\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "key with space rejected");
assert_connection_alive("after space in key");

print $sock "set key\x07bad 0 0 3\r\nfoo\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "key with control char rejected");
assert_connection_alive("after control char in key");

my $long_key = "k" x 251;
print $sock "set $long_key 0 0 3\r\nfoo\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "key > 250 bytes rejected");
assert_connection_alive("after long key");

# 2. Invalid flags: non-numeric or negative or > 2^32-1
print $sock "set key1 -1 0 3\r\nfoo\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "negative flags rejected");
assert_connection_alive("after negative flags");

print $sock "set key1 abc 0 3\r\nfoo\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "non-numeric flags rejected");
assert_connection_alive("after non-numeric flags");

print $sock "set key1 4294967296 0 3\r\nfoo\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "flags > 2^32-1 rejected");
assert_connection_alive("after overflow flags");

# 3. Invalid exptime: non-integer
print $sock "set key1 0 abc 3\r\nfoo\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "non-numeric exptime rejected");
assert_connection_alive("after invalid exptime");

# 4. Invalid bytes: negative or non-numeric
print $sock "set key1 0 0 -5\r\nfoo\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "negative bytes rejected");
assert_connection_alive("after negative bytes");

# 5. Invalid CAS unique / incr delta: negative or non-numeric
print $sock "cas key1 0 0 3 -1\r\nfoo\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "negative CAS ID rejected");
assert_connection_alive("after negative cas");

print $sock "incr key1 -5\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "negative incr delta rejected");
assert_connection_alive("after negative incr delta");

# 6. Discard data block when command line format is invalid but bytes is valid
print $sock "set invalid key 0 0 5\r\nhello\r\n";
is(read_line($sock), "CLIENT_ERROR bad command line format\r\n", "invalid command line format error returned");
assert_connection_alive("data block discarded correctly when command line invalid");

# 7. Bad data chunk trailer test (item 7)
print $sock "set b 0 0 1\r\nxyz\r\n";
is(read_line($sock), "CLIENT_ERROR bad data chunk\r\n", "bad data chunk trailer rejected");
assert_connection_alive("connection resynced after bad trailer chunk");

# Verify 'b' was not stored
print $sock "get b\r\n";
is(read_line($sock), "END\r\n", "key 'b' was not stored after bad chunk error");

# 8. Unknown commands and whitespace handling
print $sock "unknowncommand\r\n";
is(read_line($sock), "ERROR\r\n", "unknown command returns ERROR");

print $sock "   set spacekey 0 0 3   \r\nbar\r\n";
is(read_line($sock), "STORED\r\n", "command line with leading/trailing whitespace accepted");

print $sock "get spacekey\r\n";
is(read_line($sock), "VALUE spacekey 0 3\r\n", "get spacekey header");
is(read_line($sock), "bar\r\n", "get spacekey body");
is(read_line($sock), "END\r\n", "get spacekey END");

$server->stop();
done_testing();
