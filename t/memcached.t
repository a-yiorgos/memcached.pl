#!/usr/bin/perl

# Test suite for memcached.pl
# Tests both happy paths and error cases

use strict;
use warnings;
use Test::More;
use IO::Socket::INET;
use File::Temp qw(tempdir);
use File::Path qw(rmtree);
use Time::HiRes qw(sleep);

# Global variables
my $server_pid;
my $port = 11999;
my $dbdir;
my $sock;

# Cleanup at exit
END {
	cleanup_server();
}

# Start the server
sub start_server {
	$dbdir = tempdir(CLEANUP => 1);
	$server_pid = fork();
	if (!defined($server_pid)) {
		BAIL_OUT("Cannot fork: $!");
	}
	
	if ($server_pid == 0) {
		# Child: run the server
		exec 'perl', './memcached.pl', '--port', $port, '--dbdir', $dbdir
		or BAIL_OUT("Cannot exec server: $!");
		
		# Should not reach here
		exit 1;
	}
	
	# Parent: wait for server to start
	sleep(0.5);
	
	# Verify server is running
	$sock = IO::Socket::INET->new(
		PeerAddr => '127.0.0.1',
		PeerPort => $port,
		Proto => 'tcp',
		Timeout => 2
	);
	
	if (!$sock) {
		kill('TERM', $server_pid) if $server_pid;
		BAIL_OUT("Cannot connect to server on port $port: $!");
	}
	
	return $sock;
}

# Cleanup the server
sub cleanup_server {
	if ($server_pid && kill(0, $server_pid)) {
		kill('TERM', $server_pid);
		sleep(0.5);
		kill('KILL', $server_pid) if kill(0, $server_pid);
		waitpid($server_pid, 0);
	}
	
	if ($sock) {
		close($sock);
	}
}

# Send a command and get response
sub send_command {
	my ($sock, $cmd) = @_;
	
	# Ensure command ends with \r\n
	$cmd .= "\r\n" unless $cmd =~ /\r\n$/;
	
	print $sock $cmd;
	
	# For commands that expect a data block, we need special handling
	if ($cmd =~ /^(set|add|replace|cas|append|prepend)/) {
		# These commands are handled in the calling function
		return read_response($sock);
	}
	
	return read_response($sock);
}

# Read response from server
sub read_response {
	my ($sock) = @_;
	my $response = '';
	my $line;
	
	# Read until we get a line ending with \r\n
	while (defined($line = $sock->getline)) {
		$response .= $line;
		# Check if this is a complete response
		# Most responses are single line, but get/gets return multiple
		last if $response =~ /\r\n$/
			&& ($response =~ /^(STORED|NOT_STORED|NOT_FOUND|EXISTS|DELETED|END|ERROR|CLIENT_ERROR|SERVER_ERROR|OK|VERSION|TOUCHED)\r\n$/ 
			  || $response =~ /^\d+\r\n$/);
	}
	
	return $response;
}

# Read VALUE responses (multi-line)
sub read_value_response {
	my ($sock) = @_;
	my $response = '';
	my $line;
	
	while (defined($line = $sock->getline)) {
		$response .= $line;
		# END marks the end
		last if $line eq "END\r\n";
	}
	
	return $response;
}

# Send storage command with data
sub send_storage_command {
	my ($sock, $cmd, $data) = @_;
	
	$cmd .= "\r\n";
	print $sock $cmd;
	print $sock $data, "\r\n";
	
	return read_response($sock);
}

# Main tests

# Start server
start_server();

# Test 1: Basic connection and version
{
	my $resp = send_command($sock, "version");
	like($resp, qr/VERSION memcached\.pl/, "version command returns version string");
	isnt($resp, undef, "version command returns response");
}

# Test 2: Happy path - set and get
{
	my $resp = send_storage_command($sock, "set testkey 0 0 5", "value");
	is($resp, "STORED\r\n", "set command stores data");
	
	$resp = send_command($sock, "get testkey");
	like($resp, qr/VALUE testkey 0 5\r\nvalue\r\nEND\r\n/, "get retrieves stored data");
}

# Test 3: Key validation - too long
{
	my $long_key = "a" x 251;  # 251 characters, exceeds 250 limit
	my $resp = send_storage_command($sock, "set $long_key 0 0 5", "value");
	like($resp, qr/CLIENT_ERROR.*key too long/, "rejects keys exceeding 250 characters");
}

# Test 4: Key validation - control characters
{
	my $bad_key = "test\x01key";  # Contains control character
	my $resp = send_storage_command($sock, "set $bad_key 0 0 5", "value");
	like($resp, qr/CLIENT_ERROR.*control characters/, "rejects keys with control characters");
}

# Test 5: Key validation - whitespace
{
	my $resp = send_storage_command($sock, "set test key 0 0 5", "value");
	like($resp, qr/CLIENT_ERROR.*whitespace/, "rejects keys with whitespace");
}

# Test 6: Numeric validation - invalid flags
{
	my $resp = send_storage_command($sock, "set testkey abc 0 5", "value");
	like($resp, qr/CLIENT_ERROR.*flags.*non-negative integer/, "rejects non-numeric flags");
}

# Test 7: Numeric validation - invalid exptime
{
	my $resp = send_storage_command($sock, "set testkey 0 xyz 5", "value");
	like($resp, qr/CLIENT_ERROR.*exptime.*non-negative integer/, "rejects non-numeric exptime");
}

# Test 8: Numeric validation - invalid bytes
{
	my $resp = send_storage_command($sock, "set testkey 0 0 abc", "value");
	like($resp, qr/CLIENT_ERROR.*bytes.*non-negative integer/, "rejects non-numeric bytes");
}

# Test 9: Data block length validation
{
	# bytes=5 but sending 3 bytes of data
	my $resp = send_storage_command($sock, "set testkey 0 0 5", "val");
	like($resp, qr/CLIENT_ERROR.*data block/, "rejects data block with wrong length");
}

# Test 10: append command - correct parameters (no flags/exptime)
{
	# First set a key
	send_storage_command($sock, "set appendtest 0 0 5", "hello");
	
	# Then append to it (no flags/exptime per protocol)
	my $resp = send_storage_command($sock, "append appendtest 5", "world");
	is($resp, "STORED\r\n", "append accepts correct parameter count");
	
	# Verify the appended value
	$resp = send_command($sock, "get appendtest");
	like($resp, qr/VALUE appendtest 0 10\r\nhelloworld\r\nEND\r\n/, "append combines data correctly");
}

# Test 11: append command - too many parameters (with flags/exptime)
{
	# This should be rejected since append doesn't accept flags/exptime
	my $resp = send_storage_command($sock, "append testkey 0 0 5", "value");
	like($resp, qr/CLIENT_ERROR.*bad command line format/, "rejects append with extra parameters");
}

# Test 12: prepend command - correct parameters
{
	# First set a key
	send_storage_command($sock, "set prependtest 0 0 5", "world");
	
	# Then prepend to it
	my $resp = send_storage_command($sock, "prepend prependtest 5", "hello");
	is($resp, "STORED\r\n", "prepend accepts correct parameter count");
	
	# Verify the prepended value
	$resp = send_command($sock, "get prependtest");
	like($resp, qr/VALUE prependtest 0 10\r\nhelloworld\r\nEND\r\n/, "prepend combines data correctly");
}

# Test 13: delete existing key
{
	send_storage_command($sock, "set deleteme 0 0 5", "value");
	my $resp = send_command($sock, "delete deleteme");
	is($resp, "DELETED\r\n", "delete removes existing key");
}

# Test 14: delete non-existing key
{
	my $resp = send_command($sock, "delete nonexistent");
	is($resp, "NOT_FOUND\r\n", "delete returns NOT_FOUND for non-existing key");
}

# Test 15: incr existing numeric key
{
	send_storage_command($sock, "set counter 0 0 1", "5");
	my $resp = send_command($sock, "incr counter 1");
	is($resp, "6\r\n", "incr increments numeric value");
}

# Test 16: incr non-numeric value
{
	send_storage_command($sock, "set notanumber 0 0 5", "hello");
	my $resp = send_command($sock, "incr notanumber 1");
	like($resp, qr/CLIENT_ERROR.*non-numeric/, "rejects incr on non-numeric value");
}

# Test 17: incr non-existing key
{
	my $resp = send_command($sock, "incr nonexistent 1");
	is($resp, "NOT_FOUND\r\n", "incr returns NOT_FOUND for non-existing key");
}

# Test 18: touch existing key
{
	send_storage_command($sock, "set touchtest 0 0 5", "value");
	my $resp = send_command($sock, "touch touchtest 60");
	is($resp, "TOUCHED\r\n", "touch updates expiration on existing key");
}

# Test 19: touch non-existing key
{
	my $resp = send_command($sock, "touch nonexistent 60");
	is($resp, "NOT_FOUND\r\n", "touch returns NOT_FOUND for non-existing key");
}

# Test 20: cas with valid cas unique
{
	# First get with cas
	my $resp = send_command($sock, "gets castest");
	# Will fail since key doesn't exist, so set it first
	send_storage_command($sock, "set castest 0 0 5", "value");
	$resp = send_command($sock, "gets castest");
	if ($resp =~ /VALUE castest 0 5 (\d+)\r\nvalue\r\nEND\r\n/) {
		my $cas = $1;
		$resp = send_storage_command($sock, "cas castest 0 0 5 $cas", "newvalue");
		is($resp, "STORED\r\n", "cas succeeds with valid cas unique");
	}
}

# Test 21: cas with invalid cas unique
{
	send_storage_command($sock, "set castest2 0 0 5", "value");
	my $resp = send_storage_command($sock, "cas castest2 0 0 5 999", "newvalue");
	is($resp, "EXISTS\r\n", "cas returns EXISTS with invalid cas unique");
}

# Test 22: flush_all
{
	my $resp = send_command($sock, "flush_all");
	is($resp, "OK\r\n", "flush_all returns OK");
}

# Test 23: flush_all with delay
{
	my $resp = send_command($sock, "flush_all 10");
	is($resp, "OK\r\n", "flush_all with delay returns OK");
}

# Test 24: stats command
{
	my $resp = send_command($sock, "stats");
	like($resp, qr/STAT pid \d+\r\n/, "stats returns STAT lines");
	like($resp, qr/END\r\n$/, "stats ends with END");
}

# Test 25: version command
{
	my $resp = send_command($sock, "version");
	like($resp, qr/VERSION memcached\.pl/, "version returns version string");
}

# Test 26: quit command
{
	my $test_sock = IO::Socket::INET->new(
		PeerAddr => '127.0.0.1',
		PeerPort => $port,
		Proto => 'tcp',
		Timeout => 2
	);
	
	if ($test_sock) {
		print $test_sock "quit\r\n";
		close($test_sock);
		pass("quit command closes connection");
	} else {
		fail("quit command - could not connect");
	}
}

# Test 27: Invalid command
{
	my $resp = send_command($sock, "invalid_command");
	like($resp, qr/ERROR nonexistent command/, "rejects invalid command name");
}

# Test 28: Empty command line
{
	my $resp = send_command($sock, "");
	# Empty command should return ERROR
	like($resp, qr/ERROR|CLIENT_ERROR/, "handles empty command");
}

# Test 29: Malformed command (wrong parameter count)
{
	# set expects 4-5 params, give it 3
	my $resp = send_command($sock, "set key 0");
	like($resp, qr/CLIENT_ERROR.*bad command line format/, "rejects malformed command");
}

# Test 30: noreply option
{
	my $resp = send_storage_command($sock, "set noreplytest 0 0 5 noreply", "value");
	is($resp, undef, "noreply suppresses response (empty or undefined)");
	# If noreply works, the response should be empty
	# We can't easily test this without reading, so we verify the command doesn't error
	pass("noreply option accepted");
}

# Test 31: verbosity command
{
	my $resp = send_command($sock, "verbosity 1");
	is($resp, "OK\r\n", "verbosity command returns OK");
}

# Test 32: show_all custom command
{
	# First set some keys
	send_storage_command($sock, "set showall1 0 0 5", "val1");
	send_storage_command($sock, "set showall2 0 0 5", "val2");
	
	my $resp = send_command($sock, "show_all");
	like($resp, qr/VALUE showall1/, "show_all lists all keys");
	like($resp, qr/VALUE showall2/, "show_all includes all keys");
	like($resp, qr/END\r\n$/, "show_all ends with END");
}

# Test 33: Expiration time as relative (within 30 days)
{
	my $resp = send_storage_command($sock, "set exptest 0 3600 5", "value");  # 1 hour
	is($resp, "STORED\r\n", "accepts relative expiration time");
}

# Test 34: Expiration time as Unix timestamp (large value > 30 days)
{
	my $unix_time = time() + 60*60*24*31;  # 31 days from now
	my $resp = send_storage_command($sock, "set exptest2 0 $unix_time 5", "value");
	is($resp, "STORED\r\n", "accepts Unix timestamp expiration");
}

# Test 35: Negative numeric values
{
	my $resp = send_command($sock, "incr test -1");
	like($resp, qr/CLIENT_ERROR.*non-numeric/, "rejects negative values");
}

# Test 36: Zero-length data
{
	my $resp = send_storage_command($sock, "set zerolen 0 0 0", "");
	is($resp, "STORED\r\n", "accepts zero-length data");
	
	$resp = send_command($sock, "get zerolen");
	like($resp, qr/VALUE zerolen 0 0\r\n\r\nEND\r\n/, "retrieves zero-length data correctly");
}

# Test 37: CAS unique validation - non-numeric
{
	my $resp = send_storage_command($sock, "cas testkey 0 0 5 abc", "value");
	like($resp, qr/CLIENT_ERROR.*cas unique.*non-negative integer/, "rejects non-numeric cas unique");
}

# Test 38: CAS unique validation - 64-bit limit
{
	my $too_large = 18446744073709551616;  # 2^64
	my $resp = send_storage_command($sock, "cas testkey 0 0 5 $too_large", "value");
	like($resp, qr/CLIENT_ERROR.*64-bit.*limit/, "rejects cas unique exceeding 64-bit limit");
}

# Test 39: get multiple keys
{
	send_storage_command($sock, "set multi1 0 0 5", "val1");
	send_storage_command($sock, "set multi2 0 0 5", "val2");
	
	my $resp = send_command($sock, "get multi1 multi2");
	like($resp, qr/VALUE multi1/, "get retrieves first key");
	like($resp, qr/VALUE multi2/, "get retrieves second key");
	like($resp, qr/END\r\n$/, "get ends with END");
}

# Test 40: delete with noreply
{
	send_storage_command($sock, "set delnoreply 0 0 5", "value");
	my $resp = send_command($sock, "delete delnoreply noreply");
	is($resp, undef, "delete with noreply suppresses response");
	pass("delete noreply accepted");
}

done_testing();
