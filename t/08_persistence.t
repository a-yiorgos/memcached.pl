#!/usr/bin/perl

use strict;
use warnings;
use lib 't/lib';
use TestServer;
use Test::More;

my $server = TestServer->new();

# 1. Set key, restart with SIGTERM, verify data persisted and new CAS > old CAS
my $sock = $server->connect();
print $sock "set term_key 0 0 5\r\nterm1\r\n";
is(read_line($sock), "STORED\r\n", "set term_key STORED");

print $sock "gets term_key\r\n";
my $thead1 = read_line($sock);
my ($tcas1) = ($thead1 =~ /^VALUE term_key 0 5 (\d+)\r\n$/);
read_line($sock); read_line($sock);
close($sock);

$server->restart('TERM');

$sock = $server->connect();
print $sock "get term_key\r\n";
is(read_line($sock), "VALUE term_key 0 5\r\n", "term_key header after SIGTERM restart");
is(read_line($sock), "term1\r\n", "term_key body after SIGTERM restart");
is(read_line($sock), "END\r\n", "term_key END after SIGTERM restart");

print $sock "set term_key2 0 0 5\r\nterm2\r\n";
is(read_line($sock), "STORED\r\n", "set term_key2 STORED after SIGTERM");

print $sock "gets term_key2\r\n";
my $thead2 = read_line($sock);
my ($tcas2) = ($thead2 =~ /^VALUE term_key2 0 5 (\d+)\r\n$/);
read_line($sock); read_line($sock);
close($sock);

cmp_ok($tcas2, '>', $tcas1, "CAS ID after SIGTERM restart is greater than pre-restart CAS ID");

# 2. Set key, restart with SIGKILL, verify data persisted and new CAS > old CAS
$sock = $server->connect();
print $sock "set kill_key 0 0 5\r\nkill1\r\n";
is(read_line($sock), "STORED\r\n", "set kill_key STORED");

print $sock "gets kill_key\r\n";
my $khead1 = read_line($sock);
my ($kcas1) = ($khead1 =~ /^VALUE kill_key 0 5 (\d+)\r\n$/);
read_line($sock); read_line($sock);
close($sock);

$server->restart('KILL');

$sock = $server->connect();
print $sock "get kill_key\r\n";
is(read_line($sock), "VALUE kill_key 0 5\r\n", "kill_key header after SIGKILL restart");
is(read_line($sock), "kill1\r\n", "kill_key body after SIGKILL restart");
is(read_line($sock), "END\r\n", "kill_key END after SIGKILL restart");

print $sock "set kill_key2 0 0 5\r\nkill2\r\n";
is(read_line($sock), "STORED\r\n", "set kill_key2 STORED after SIGKILL");

print $sock "gets kill_key2\r\n";
my $khead2 = read_line($sock);
my ($kcas2) = ($khead2 =~ /^VALUE kill_key2 0 5 (\d+)\r\n$/);
read_line($sock); read_line($sock);
close($sock);

cmp_ok($kcas2, '>', $kcas1, "CAS ID after SIGKILL restart is greater than pre-restart CAS ID");

$server->stop();
done_testing();
