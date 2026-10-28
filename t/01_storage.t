#!/usr/bin/perl

use strict;
use warnings;
use lib 't/lib';
use TestServer;
use Test::More;

my $server = TestServer->new();
my $sock = $server->connect();
ok($sock, "Connected to test server");

# Basic set/get
print $sock "set key1 0 0 5\r\nhello\r\n";
is(read_line($sock), "STORED\r\n", "set key1 STORED");

print $sock "get key1\r\n";
is(read_line($sock), "VALUE key1 0 5\r\n", "get key1 header");
is(read_line($sock), "hello\r\n", "get key1 body");
is(read_line($sock), "END\r\n", "get key1 END");

# gets with CAS
print $sock "gets key1\r\n";
my $line = read_line($sock);
like($line, qr/^VALUE key1 0 5 (\d+)\r\n$/, "gets key1 returns CAS ID");
is(read_line($sock), "hello\r\n", "gets key1 body");
is(read_line($sock), "END\r\n", "gets key1 END");

# Multi-key get
print $sock "set key2 10 0 3\r\nfoo\r\n";
is(read_line($sock), "STORED\r\n", "set key2 STORED");

print $sock "get key1 key2 missing_key\r\n";
is(read_line($sock), "VALUE key1 0 5\r\n", "multi-get key1 header");
is(read_line($sock), "hello\r\n", "multi-get key1 body");
is(read_line($sock), "VALUE key2 10 3\r\n", "multi-get key2 header");
is(read_line($sock), "foo\r\n", "multi-get key2 body");
is(read_line($sock), "END\r\n", "multi-get END");

# Key "0" test
print $sock "set 0 0 0 1\r\nx\r\n";
is(read_line($sock), "STORED\r\n", "set key 0 STORED");

print $sock "get 0 key1\r\n";
is(read_line($sock), "VALUE 0 0 1\r\n", "get key 0 header");
is(read_line($sock), "x\r\n", "get key 0 body");
is(read_line($sock), "VALUE key1 0 5\r\n", "get key1 header after 0");
is(read_line($sock), "hello\r\n", "get key1 body");
is(read_line($sock), "END\r\n", "get after key 0 END");

# Empty values
print $sock "set empty_key 0 0 0\r\n\r\n";
is(read_line($sock), "STORED\r\n", "set empty_key STORED");

print $sock "get empty_key\r\n";
is(read_line($sock), "VALUE empty_key 0 0\r\n", "get empty_key header");
is(read_line($sock), "\r\n", "get empty_key empty body");
is(read_line($sock), "END\r\n", "get empty_key END");

# Binary payloads containing \r\n inside the data
my $binary_data = "a\r\nb\r\nc";
my $len = length($binary_data);
print $sock "set bin_key 123 0 $len\r\n$binary_data\r\n";
is(read_line($sock), "STORED\r\n", "set bin_key STORED");

print $sock "get bin_key\r\n";
is(read_line($sock), "VALUE bin_key 123 $len\r\n", "get bin_key header");
my $got_bin = read_bytes($sock, $len);
is($got_bin, $binary_data, "get bin_key binary body matched");
is(read_line($sock), "\r\n", "get bin_key trailing crlf after body");
is(read_line($sock), "END\r\n", "get bin_key END");

# add / replace / append / prepend
print $sock "add key2 0 0 3\r\nbar\r\n";
is(read_line($sock), "NOT_STORED\r\n", "add fails for existing key2");

print $sock "add add_key 0 0 3\r\nbar\r\n";
is(read_line($sock), "STORED\r\n", "add succeeds for new add_key");

print $sock "replace missing_rep 0 0 3\r\nabc\r\n";
is(read_line($sock), "NOT_STORED\r\n", "replace fails for missing key");

print $sock "replace key2 0 0 3\r\nbar\r\n";
is(read_line($sock), "STORED\r\n", "replace succeeds for existing key2");

print $sock "append key2 0 0 3\r\nbaz\r\n";
is(read_line($sock), "STORED\r\n", "append succeeds");

print $sock "get key2\r\n";
is(read_line($sock), "VALUE key2 0 6\r\n", "get key2 header after append");
is(read_line($sock), "barbaz\r\n", "get key2 body after append");
is(read_line($sock), "END\r\n", "get key2 END after append");

print $sock "prepend key2 0 0 3\r\nfoo\r\n";
is(read_line($sock), "STORED\r\n", "prepend succeeds");

print $sock "get key2\r\n";
is(read_line($sock), "VALUE key2 0 9\r\n", "get key2 header after prepend");
is(read_line($sock), "foobarbaz\r\n", "get key2 body after prepend");
is(read_line($sock), "END\r\n", "get key2 END after prepend");

$server->stop();
done_testing();
