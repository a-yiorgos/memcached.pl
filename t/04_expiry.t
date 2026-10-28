#!/usr/bin/perl

use strict;
use warnings;
use lib 't/lib';
use TestServer;
use Test::More;
use Time::HiRes qw(sleep);

my $server = TestServer->new();
my $sock = $server->connect();

# Relative exptime = 1 second
print $sock "set exp_rel 0 1 5\r\nhello\r\n";
is(read_line($sock), "STORED\r\n", "set exp_rel 1s");

print $sock "get exp_rel\r\n";
is(read_line($sock), "VALUE exp_rel 0 5\r\n", "get exp_rel before expiry header");
is(read_line($sock), "hello\r\n", "get exp_rel before expiry body");
is(read_line($sock), "END\r\n", "get exp_rel before expiry END");

sleep(2);

print $sock "get exp_rel\r\n";
is(read_line($sock), "END\r\n", "get exp_rel after expiry returns END");

# Absolute exptime (> 30 days unix timestamp)
my $future_time = time() + 1;
print $sock "set exp_abs 0 $future_time 5\r\nworld\r\n";
is(read_line($sock), "STORED\r\n", "set exp_abs absolute time");

sleep(2);

print $sock "get exp_abs\r\n";
is(read_line($sock), "END\r\n", "get exp_abs after absolute expiry returns END");

# Negative exptime (immediately expired)
print $sock "set exp_neg 0 -100 5\r\nquick\r\n";
is(read_line($sock), "STORED\r\n", "set exp_neg STORED");

print $sock "get exp_neg\r\n";
is(read_line($sock), "END\r\n", "get exp_neg returns END immediately");

# Interaction with add, replace, append, incr on expired keys
print $sock "set k_add 0 1 3\r\n111\r\n";
is(read_line($sock), "STORED\r\n", "set k_add 1s");
sleep(2);
print $sock "add k_add 0 0 3\r\n222\r\n";
is(read_line($sock), "STORED\r\n", "add on expired key succeeds");

print $sock "set k_rep 0 1 3\r\n111\r\n";
is(read_line($sock), "STORED\r\n", "set k_rep 1s");
sleep(2);
print $sock "replace k_rep 0 0 3\r\n222\r\n";
is(read_line($sock), "NOT_STORED\r\n", "replace on expired key fails (NOT_STORED)");

print $sock "set k_app 0 1 3\r\n111\r\n";
is(read_line($sock), "STORED\r\n", "set k_app 1s");
sleep(2);
print $sock "append k_app 0 0 3\r\n222\r\n";
is(read_line($sock), "NOT_FOUND\r\n", "append on expired key fails (NOT_FOUND)");

print $sock "set k_inc 0 1 1\r\n5\r\n";
is(read_line($sock), "STORED\r\n", "set k_inc 1s");
sleep(2);
print $sock "incr k_inc 1\r\n";
is(read_line($sock), "NOT_FOUND\r\n", "incr on expired key fails (NOT_FOUND)");

$server->stop();
done_testing();
