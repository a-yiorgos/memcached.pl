#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use IO::Socket::INET;
use Time::HiRes qw(usleep);

my $port = 11311;
my $server_pid = fork();

if (!defined $server_pid) {
	plan skip_all => "Cannot fork server for testing: $!";
	exit 0;
}

if ($server_pid == 0) {
	# Run memcached.pl in child process
	my $lib_dir = $ENV{HOME} ? "$ENV{HOME}/perl5/lib/perl5" : "/usr/local/share/perl";
	exec($^X, "-I$lib_dir", "memcached.pl");
	exit 0;
}

# Give server time to start up and bind socket
usleep(300000);

# Helper function to connect to server
sub connect_server {
	my $sock;
	for my $i (1..10) {
		$sock = IO::Socket::INET->new(
			PeerAddr => '127.0.0.1',
			PeerPort => $port,
			Proto    => 'tcp',
			Timeout  => 2,
		);
		last if $sock;
		usleep(100000);
	}
	return $sock;
}

my $sock = connect_server();
if (!$sock) {
	kill 'TERM', $server_pid;
	waitpid($server_pid, 0);
	plan skip_all => "Could not connect to memcached server on port $port";
	exit 0;
}

# Define test plan
plan tests => 21;

# Test 1: version command
print $sock "version\r\n";
my $line = <$sock>;
like($line, qr/^VERSION memcached\.pl/, "version command returns VERSION string");

# Test 2: set key
print $sock "set key1 0 0 5\r\nhello\r\n";
$line = <$sock>;
is($line, "STORED\r\n", "set key1 STORED");

# Test 3: get key
print $sock "get key1\r\n";
my $vhead = <$sock>;
my $vbody = <$sock>;
my $vend  = <$sock>;
is($vhead, "VALUE key1 0 5\r\n", "get key1 header");
is($vbody, "hello\r\n", "get key1 body");
is($vend, "END\r\n", "get key1 END");

# Test 4: gets key (with cas)
print $sock "gets key1\r\n";
$vhead = <$sock>;
$vbody = <$sock>;
$vend  = <$sock>;
like($vhead, qr/^VALUE key1 0 5 (\d+)\r\n$/, "gets key1 returns CAS ID");
my ($cas_id) = ($vhead =~ /^VALUE key1 0 5 (\d+)\r\n$/);
is($vbody, "hello\r\n", "gets key1 body");
is($vend, "END\r\n", "gets key1 END");

# Test 5: cas update with wrong CAS ID
print $sock "cas key1 0 0 5 99999\r\nworld\r\n";
$line = <$sock>;
is($line, "EXISTS\r\n", "cas with wrong CAS ID fails with EXISTS");

# Test 6: cas update with correct CAS ID
print $sock "cas key1 0 0 5 $cas_id\r\nworld\r\n";
$line = <$sock>;
is($line, "STORED\r\n", "cas with correct CAS ID succeeds");

# Test 7: add existing key
print $sock "add key1 0 0 5\r\nhello\r\n";
$line = <$sock>;
is($line, "NOT_STORED\r\n", "add fails for existing key");

# Test 8: add new key
print $sock "add key2 0 0 3\r\nfoo\r\n";
$line = <$sock>;
is($line, "STORED\r\n", "add succeeds for new key");

# Test 9: append
print $sock "append key2 0 0 3\r\nbar\r\n";
$line = <$sock>;
is($line, "STORED\r\n", "append succeeds");

print $sock "get key2\r\n";
$vhead = <$sock>;
$vbody = <$sock>;
$vend  = <$sock>;
is($vbody, "foobar\r\n", "get key2 after append");

# Test 10: prepend
print $sock "prepend key2 0 0 3\r\npre\r\n";
$line = <$sock>;
is($line, "STORED\r\n", "prepend succeeds");

print $sock "get key2\r\n";
$vhead = <$sock>;
$vbody = <$sock>;
$vend  = <$sock>;
is($vbody, "prefoobar\r\n", "get key2 after prepend");

# Test 11: incr and decr
print $sock "set numkey 0 0 1\r\n5\r\n";
<$sock>; # STORED

print $sock "incr numkey 3\r\n";
$line = <$sock>;
is($line, "8\r\n", "incr numkey 5 + 3 = 8");

print $sock "decr numkey 2\r\n";
$line = <$sock>;
is($line, "6\r\n", "decr numkey 8 - 2 = 6");

# Test 12: delete
print $sock "delete key1\r\n";
$line = <$sock>;
is($line, "DELETED\r\n", "delete key1 succeeds");

# Test 13: flush_all
print $sock "flush_all\r\n";
$line = <$sock>;
is($line, "OK\r\n", "flush_all returns OK");

print $sock "get key2\r\n";
$vend = <$sock>;
is($vend, "END\r\n", "get after flush_all returns empty END");

# Test 14: quit
print $sock "quit\r\n";
close($sock);

# Clean up server
kill 'TERM', $server_pid;
waitpid($server_pid, 0);

# Remove test database files
unlink 'memcache.db';
unlink glob('__db.*');
unlink glob('/tmp/__db.*');
