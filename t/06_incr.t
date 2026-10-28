#!/usr/bin/perl

use strict;
use warnings;
use lib 't/lib';
use TestServer;
use Test::More;

my $server = TestServer->new();
my $sock = $server->connect();

# Setup key
print $sock "set num 0 0 2\r\n10\r\n";
is(read_line($sock), "STORED\r\n", "set num 10");

# incr / decr basic
print $sock "incr num 5\r\n";
is(read_line($sock), "15\r\n", "incr 10 + 5 = 15");

print $sock "decr num 3\r\n";
is(read_line($sock), "12\r\n", "decr 15 - 3 = 12");

# decr below 0 floors at 0
print $sock "decr num 20\r\n";
is(read_line($sock), "0\r\n", "decr 12 - 20 floors at 0");

# 64-bit unsigned wrap at 2^64 (18446744073709551615)
print $sock "set maxnum 0 0 20\r\n18446744073709551615\r\n";
is(read_line($sock), "STORED\r\n", "set maxnum to 2^64 - 1");

print $sock "incr maxnum 1\r\n";
is(read_line($sock), "0\r\n", "incr 2^64 - 1 + 1 wraps to 0");

print $sock "incr maxnum 5\r\n";
is(read_line($sock), "5\r\n", "incr wrapped 0 + 5 = 5");

# Non-numeric value stored
print $sock "set str 0 0 5\r\nhello\r\n";
is(read_line($sock), "STORED\r\n", "set non-numeric str");

print $sock "incr str 1\r\n";
is(read_line($sock), "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n", "incr on non-numeric value rejected");

print $sock "decr str 1\r\n";
is(read_line($sock), "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n", "decr on non-numeric value rejected");

# NOT_FOUND for missing key
print $sock "incr missing_num 1\r\n";
is(read_line($sock), "NOT_FOUND\r\n", "incr missing key returns NOT_FOUND");

print $sock "decr missing_num 1\r\n";
is(read_line($sock), "NOT_FOUND\r\n", "decr missing key returns NOT_FOUND");

# Flags and exptime preservation, new CAS assigned
print $sock "set flagged 12345 0 2\r\n10\r\n";
is(read_line($sock), "STORED\r\n", "set flagged");

print $sock "gets flagged\r\n";
my $fhead1 = read_line($sock);
my ($cas1) = ($fhead1 =~ /^VALUE flagged 12345 2 (\d+)\r\n$/);
read_line($sock); read_line($sock);

print $sock "incr flagged 5\r\n";
is(read_line($sock), "15\r\n", "incr flagged");

print $sock "gets flagged\r\n";
my $fhead2 = read_line($sock);
my ($cas2) = ($fhead2 =~ /^VALUE flagged 12345 2 (\d+)\r\n$/);
read_line($sock); read_line($sock);

cmp_ok($cas2, '>', $cas1, "incr assigned a new greater CAS ID");

$server->stop();
done_testing();
