#!/usr/bin/perl

use strict;
use warnings;
use lib 't/lib';
use TestServer;
use Test::More;

my $server = TestServer->new();

my $s1 = $server->connect();
my $s2 = $server->connect();

print $s1 "set caskey 0 0 5\r\nval01\r\n";
is(read_line($s1), "STORED\r\n", "set caskey");

print $s1 "gets caskey\r\n";
my $line1 = read_line($s1);
like($line1, qr/^VALUE caskey 0 5 (\d+)\r\n$/, "gets caskey on s1");
my ($cas1) = ($line1 =~ /^VALUE caskey 0 5 (\d+)\r\n$/);
read_line($s1); # val01
read_line($s1); # END

print $s2 "gets caskey\r\n";
my $line2 = read_line($s2);
like($line2, qr/^VALUE caskey 0 5 (\d+)\r\n$/, "gets caskey on s2");
my ($cas2) = ($line2 =~ /^VALUE caskey 0 5 (\d+)\r\n$/);
read_line($s2); # val01
read_line($s2); # END

is($cas1, $cas2, "CAS tokens match before update");

# cas happy path on s1
print $s1 "cas caskey 0 0 5 $cas1\r\nval02\r\n";
is(read_line($s1), "STORED\r\n", "cas update on s1 succeeds");

# cas with stale token on s2 returns EXISTS
print $s2 "cas caskey 0 0 5 $cas2\r\nval03\r\n";
is(read_line($s2), "EXISTS\r\n", "cas update on s2 with stale token fails with EXISTS");

# cas on non-existent key returns NOT_FOUND
print $s1 "cas missing_key 0 0 5 12345\r\nhello\r\n";
is(read_line($s1), "NOT_FOUND\r\n", "cas on non-existent key returns NOT_FOUND");

# Monotonically increasing CAS values across separate operations/connections
print $s1 "set k1 0 0 1\r\na\r\n";
read_line($s1);
print $s1 "gets k1\r\n";
my ($cas_a) = (read_line($s1) =~ /^VALUE k1 0 1 (\d+)\r\n$/);
read_line($s1); read_line($s1);

print $s2 "set k2 0 0 1\r\nb\r\n";
read_line($s2);
print $s2 "gets k2\r\n";
my ($cas_b) = (read_line($s2) =~ /^VALUE k2 0 1 (\d+)\r\n$/);
read_line($s2); read_line($s2);

cmp_ok($cas_b, '>', $cas_a, "CAS value strictly increased on subsequent operation");

$server->stop();
done_testing();
