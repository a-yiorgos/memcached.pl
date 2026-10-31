#!/usr/bin/perl

# An (incomplete) implementation of the memcached text protocol
# http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
# in Perl with persistence using DB_File.
#
# Useful for debugging and / or testing purposes, not for production.

use strict;
use warnings;
use DB_File;
use IO::Socket;
use Storable qw(freeze thaw);
use POSIX qw(:sys_wait_h);
use Fcntl qw(O_RDWR O_CREAT);
use Getopt::Long;

# Global variables.
our (
	$pid,
	$month,
	$db,
	%cache,
	$sock,
	$nsock,
	$cmd,
	@line,
	$FROM_PARENT,
	$TO_CHILD,
	$FROM_CHILD,
	$TO_PARENT,
	$cas_pid,
	%children,
	$shutdown,
	$shutdown_lock,
	$MAX_KEY_LENGTH,
	$port,
	$listen,
	$dbdir
);

# Initialize global variables
$shutdown_lock = 0;
$MAX_KEY_LENGTH = 250;

# Signal handler for forked children.
# Properly reaps all waiting child processes to prevent zombies.
# Uses minimal operations to avoid async-signal-safety issues.
sub REAPER {
	local $!;
	my $waitpid;
	# Reap all waiting children non-blocking
	while (($waitpid = waitpid(-1, WNOHANG)) > 0) {
		delete $children{$waitpid};
	}
	$SIG{CHLD} = \&REAPER;
}

# Signal handler for graceful shutdown.
# Sets flag and closes socket to unblock accept().
# Minimal operations for async-signal-safety.
sub SHUTDOWN {
	$shutdown = 1;
	# Close listening socket to unblock accept() in main loop
	if ($sock) {
		my $fileno = $sock->fileno;
		close($sock) if defined($fileno) && $fileno >= 0;
	}
	$SIG{TERM} = \&SHUTDOWN;
	$SIG{INT} = \&SHUTDOWN;
}

# Set SIGPIPE to IGNORE - built-in Perl constant for signal safety
$SIG{PIPE} = 'IGNORE';

# Helper function to check if DB operations are allowed
# Returns 1 if allowed, 0 if shutdown in progress
sub db_allowed {
	return !$shutdown_lock;
}

# Validate a key per protocol requirements.
# Returns (1, undef) on success, (0, "error message") on failure.
sub validate_key {
	my ($key) = @_;
	
	# Check key length (max 250 characters)
	if (length($key) > $MAX_KEY_LENGTH) {
		return (0, "key too long");
	}
	
	# Check for control characters (ASCII < 32) or DEL (127)
	if ($key =~ /[\x00-\x1F\x7F]/) {
		return (0, "key contains control characters");
	}
	
	# Check for whitespace
	if ($key =~ /\s/) {
		return (0, "key contains whitespace");
	}
	
	return (1, undef);
}

# Validate a numeric parameter (unsigned integer).
# Returns (1, undef) on success, (0, "error message") on failure.
# Protocol: flags, bytes, cas unique, exptime must be non-negative integers.
sub validate_numeric {
	my ($value, $param_name) = @_;
	
	# Check it's a non-negative integer (digits only, no sign, no decimal point)
	if (!defined($value) || $value !~ m/^\d+$/) {
		return (0, "$param_name must be a non-negative integer");
	}
	
	# For 64-bit values (cas unique), check it fits in 64 bits
	if ($param_name eq 'cas unique') {
		# 2^64-1 = 18446744073709551615
		if ($value > 18446744073709551615) {
			return (0, "$param_name exceeds 64-bit unsigned integer limit");
		}
	}
	
	return (1, undef);
}

# Read a data block from socket and validate its length.
# Returns the data on success, dies on error.
sub read_data_block {
	my ($sock, $expected_bytes) = @_;
	
	# Read exactly $expected_bytes + 2 (for \r\n)
	my $data;
	my $bytes_read = read($sock, $data, $expected_bytes + 2);
	
	if (!defined($bytes_read)) {
		die "Failed to read data block: $!";
	}
	
	if ($bytes_read < $expected_bytes + 2) {
		die "Incomplete data block: expected " . ($expected_bytes + 2) . " bytes, got $bytes_read";
	}
	
	# Verify it ends with \r\n
	if (substr($data, -2) ne "\r\n") {
		die "Data block not properly terminated with \\r\\n";
	}
	
	# Return data without the trailing \r\n
	return substr($data, 0, $expected_bytes);
}

# Request a new CAS value.
sub set_casunique {
	my $casunique;

	print $TO_CHILD "get\n";
	$casunique = <$FROM_CHILD>;
	chomp($casunique);

	return $casunique;
}

# set a key value.
sub m_set {
	my $noreply;
	my $len;
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);

	$len = scalar @line;
	if (($len == 4) or ($len == 5)) {
		$key = shift(@line);
		$flags = shift(@line);
		$exptime = shift(@line);
		$bytes = shift(@line);
		if ($len == 5) {
			$noreply = shift(@line);
			if ($noreply ne "noreply") {
				undef($noreply);
			}
		}
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	# Validate key
	my ($key_valid, $key_error) = validate_key($key);
	if (!$key_valid) {
		print $nsock "CLIENT_ERROR $key_error\r\n";
		return;
	}

	# Validate numeric parameters per protocol
	my ($flags_valid, $flags_error) = validate_numeric($flags, "flags");
	if (!$flags_valid) {
		print $nsock "CLIENT_ERROR $flags_error\r\n";
		return;
	}
	
	my ($exptime_valid, $exptime_error) = validate_numeric($exptime, "exptime");
	if (!$exptime_valid) {
		print $nsock "CLIENT_ERROR $exptime_error\r\n";
		return;
	}
	
	my ($bytes_valid, $bytes_error) = validate_numeric($bytes, "bytes");
	if (!$bytes_valid) {
		print $nsock "CLIENT_ERROR $bytes_error\r\n";
		return;
	}

	# Read and validate data block
	$val = eval { read_data_block($nsock, $bytes) };
	if ($@) {
		print $nsock "CLIENT_ERROR $@\r\n";
		return;
	}

	# Per protocol: if exptime > 0 and <= 30 days, treat as relative offset from now.
	if (($exptime != 0) and ($exptime <= $month)) {
		$exptime += time();
	}

	$casunique = set_casunique();
	$data = freeze [$flags, $exptime, $bytes, $casunique, $val];

	$lock = $db->lock();
	$cache{$key} = $data;
	$lock->unlock();
	print $nsock "STORED\r\n" unless(defined($noreply));

	return;
}

# check and set a key value.
sub m_cas {
	my $noreply;
	my $len;
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);
	my ($oflags, $oexptime, $obytes, $ocasunique, $oval);

	$len = scalar @line;
	if (($len == 5) or ($len == 6)) {
		$key = shift(@line);
		$flags = shift(@line);
		$exptime = shift(@line);
		$bytes = shift(@line);
		$casunique = shift(@line);
		if ($len == 6) {
			$noreply = shift(@line);
			if ($noreply ne "noreply") {
				undef($noreply);
			}
		}
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	# Validate key
	my ($key_valid, $key_error) = validate_key($key);
	if (!$key_valid) {
		print $nsock "CLIENT_ERROR $key_error\r\n";
		return;
	}

	# Validate numeric parameters per protocol
	my ($flags_valid, $flags_error) = validate_numeric($flags, "flags");
	if (!$flags_valid) {
		print $nsock "CLIENT_ERROR $flags_error\r\n";
		return;
	}
	
	my ($exptime_valid, $exptime_error) = validate_numeric($exptime, "exptime");
	if (!$exptime_valid) {
		print $nsock "CLIENT_ERROR $exptime_error\r\n";
		return;
	}
	
	my ($bytes_valid, $bytes_error) = validate_numeric($bytes, "bytes");
	if (!$bytes_valid) {
		print $nsock "CLIENT_ERROR $bytes_error\r\n";
		return;
	}

	# Validate cas unique for CAS operations (64-bit unsigned integer)
	my ($cas_valid, $cas_error) = validate_numeric($casunique, "cas unique");
	if (!$cas_valid) {
		print $nsock "CLIENT_ERROR $cas_error\r\n";
		return;
	}

	# Read and validate data block
	$val = eval { read_data_block($nsock, $bytes) };
	if ($@) {
		print $nsock "CLIENT_ERROR $@\r\n";
		return;
	}

	# Check whether the key has actually expired first
	if (defined($cache{$key})) {
		my $ref = thaw $cache{$key};
		($oflags, $oexptime, $obytes, $ocasunique, $oval) = @$ref;
		if (($oexptime != 0) and ($oexptime < time())) {
			$lock = $db->lock();
			delete $cache{$key};
			$lock->unlock();
		}
	}

	if (!defined($cache{$key})) {
		print $nsock "NOT_FOUND\r\n";
		return;
	}

	if ($casunique != $ocasunique) {
		print $nsock "EXISTS\r\n";
		return;
	}

	# Per protocol: if exptime > 0 and <= 30 days, treat as relative offset from now.
	if (($exptime != 0) and ($exptime <= $month)) {
		$exptime += time();
	}

	$casunique = set_casunique();
	$data = freeze [$flags, $exptime, $bytes, $casunique, $val];

	$lock = $db->lock();
	$cache{$key} = $data;
	$lock->unlock();
	print $nsock "STORED\r\n" unless(defined($noreply));

	return;
}

# get a key value.
sub m_get {
	my $mode = shift(@_);
	my $len;
	my $lock;
	my $key;
	my ($flags, $exptime, $bytes, $casunique, $val);

	$len = scalar @line;
	if ($len == 0) {
		print $nsock "CLIENT_ERROR no keys specified\r\n";
		return;
	}

	# get the keys
	while ($key = shift(@line)) {
		# Validate key
		my ($key_valid, $key_error) = validate_key($key);
		if (!$key_valid) {
			print $nsock "CLIENT_ERROR $key_error\r\n";
			next;
		}
		if (defined($cache{$key})) {
			# Check whether the key has actually expired first
			my $ref = thaw $cache{$key};
			($flags, $exptime, $bytes, $casunique, $val) = @$ref;
			if (($exptime != 0) and ($exptime < time())) {
				$lock = $db->lock();
				delete $cache{$key};
				$lock->unlock();
			} else {
				if ($mode eq "get") {
					print $nsock "VALUE $key $flags $bytes\r\n";
				} elsif ($mode eq "gets") {
					print $nsock "VALUE $key $flags $bytes $casunique\r\n";
				} else {
					print $nsock "CLIENT_ERROR cannot $mode\r\n";
					return;
				}
				print $nsock $val, "\r\n";
			}
		}
	}

	print $nsock "END\r\n";
	return;
}

# add / replace a key if it does not already exist
sub m_add {
	my $noreply;
	my $mode = shift(@_);
	my $len;
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);
	my ($oflags, $oexptime, $obytes, $ocasunique, $oval);

	$len = scalar @line;
	if (($len == 4) or ($len == 5)) {
		$key = shift(@line);
		$flags = shift(@line);
		$exptime = shift(@line);
		$bytes = shift(@line);
		if ($len == 5) {
			$noreply = shift(@line);
			if ($noreply ne "noreply") {
				undef($noreply);
			}
		}
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	# Validate key
	my ($key_valid, $key_error) = validate_key($key);
	if (!$key_valid) {
		print $nsock "CLIENT_ERROR $key_error\r\n";
		return;
	}

	# Validate numeric parameters per protocol
	my ($flags_valid, $flags_error) = validate_numeric($flags, "flags");
	if (!$flags_valid) {
		print $nsock "CLIENT_ERROR $flags_error\r\n";
		return;
	}
	
	my ($exptime_valid, $exptime_error) = validate_numeric($exptime, "exptime");
	if (!$exptime_valid) {
		print $nsock "CLIENT_ERROR $exptime_error\r\n";
		return;
	}
	
	my ($bytes_valid, $bytes_error) = validate_numeric($bytes, "bytes");
	if (!$bytes_valid) {
		print $nsock "CLIENT_ERROR $bytes_error\r\n";
		return;
	}

	# Read and validate data block
	$val = eval { read_data_block($nsock, $bytes) };
	if ($@) {
		print $nsock "CLIENT_ERROR $@\r\n";
		return;
	}

	# Check whether the key has actually expired first.
	if (defined($cache{$key})) {
		my $ref = thaw $cache{$key};
		($oflags, $oexptime, $obytes, $ocasunique, $oval) = @$ref;
		if (($oexptime != 0) and ($oexptime < time())) {
			$lock = $db->lock();
			delete $cache{$key};
			$lock->unlock();
		}
	}

	if (($mode eq "add") and (defined($cache{$key}))) {
		print $nsock "NOT_STORED\r\n" unless(defined($noreply));
		return;
	}

	if (($mode eq "replace") and (!defined($cache{$key}))) {
		print $nsock "NOT_STORED\r\n" unless(defined($noreply));
		return;
	}

	# Per protocol: if exptime > 0 and <= 30 days, treat as relative offset from now.
	if (($exptime != 0) and ($exptime <= $month)) {
		$exptime += time();
	}

	$casunique = set_casunique();
	$data = freeze [$flags, $exptime, $bytes, $casunique, $val];

	$lock = $db->lock();
	$cache{$key} = $data;
	$lock->unlock();
	print $nsock "STORED\r\n" unless(defined($noreply));

	return;
}

# delete a key if it exists.
sub m_delete {
	my $noreply;
	my $len;
	my $lock;
	my $key;

	$len = scalar @line;
	if (($len == 1) or ($len == 2)) {
		$key = shift(@line);
		if ($len == 2) {
			$noreply = shift(@line);
			if ($noreply ne "noreply") {
				undef($noreply);
				print $nsock "CLIENT_ERROR bad command line format. Usage: delete <key> [noreply]\r\n";
				return;
			}
		}
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	# Validate key
	my ($key_valid, $key_error) = validate_key($key);
	if (!$key_valid) {
		print $nsock "CLIENT_ERROR $key_error\r\n";
		return;
	}

	if (!defined($cache{$key})) {
		print $nsock "NOT_FOUND\r\n";
		return;
	}

	$lock = $db->lock();
	delete $cache{$key};
	$lock->unlock();
	print $nsock "DELETED\r\n" unless defined($noreply);

	return;
}

# flush all keys from the database
# Protocol: flush_all [delay] [noreply]
# If delay is provided, items are invalidated after that many seconds.
# For this simple implementation, we don't support delayed invalidation,
# but we accept the parameter for protocol compliance.
sub m_flush_all {
	my $delay;
	my $noreply;
	my $len;
	my $lock;
	my $key;

	$len = scalar @line;
	if ($len == 0) {
		# flush_all with no arguments
		$delay = 0;
	} elsif ($len == 1) {
		# flush_all <delay>
		$delay = shift(@line);
		# Validate delay is a non-negative integer
		if ($delay !~ m/^\d+$/) {
			print $nsock "CLIENT_ERROR invalid delay\r\n";
			return;
		}
	} elsif ($len == 2) {
		# flush_all <delay> noreply or flush_all <delay> <noreply>
		$delay = shift(@line);
		$noreply = shift(@line);
		if ($noreply ne "noreply") {
			# Second argument is not "noreply", treat as invalid
			print $nsock "CLIENT_ERROR bad command line format\r\n";
			return;
		}
		# Validate delay
		if ($delay !~ m/^\d+$/) {
			print $nsock "CLIENT_ERROR invalid delay\r\n";
			return;
		}
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	# For this implementation, we immediately invalidate all items
	# regardless of delay (protocol allows this interpretation).
	# A full implementation would track expiration times.
	$lock = $db->lock();
	foreach $key (keys %cache) {
		delete $cache{$key};
	}
	$lock->unlock();
	
	print $nsock "OK\r\n" unless defined($noreply);

	return;
}

# Stats command - returns server statistics
# Protocol: stats [args]
# For this implementation, we return basic statistics.
sub m_stats {
	my $args = shift(@line) if @line;
	my $lock;
	
	if (defined($args) && $args eq "settings") {
		# Return basic settings
		print $nsock "STAT maxbytes 1048576\r\n";
		print $nsock "STAT maxconns 1024\r\n";
		print $nsock "STAT tcpport $port\r\n";
		print $nsock "STAT udpport 0\r\n";
		print $nsock "END\r\n";
		return;
	} elsif (defined($args) && $args eq "items") {
		# Return basic item statistics
		$lock = $db->lock();
		my $count = scalar keys %cache;
		$lock->unlock();
		print $nsock "STAT items:0:number $count\r\n";
		print $nsock "END\r\n";
		return;
	} elsif (defined($args) && $args eq "slabs") {
		# Return basic slab statistics (simplified)
		$lock = $db->lock();
		my $count = scalar keys %cache;
		$lock->unlock();
		print $nsock "STAT 0:chunk_size 96\r\n";
		print $nsock "STAT 0:chunks_per_page 10922\r\n";
		print $nsock "STAT 0:total_pages 1\r\n";
		print $nsock "STAT 0:total_chunks $count\r\n";
		print $nsock "STAT 0:used_chunks $count\r\n";
		print $nsock "STAT 0:free_chunks 0\r\n";
		print $nsock "STAT active_slabs 1\r\n";
		print $nsock "STAT total_malloced 96\r\n";
		print $nsock "END\r\n";
		return;
	} elsif (defined($args)) {
		# Unknown stats argument
		print $nsock "END\r\n";
		return;
	}
	
	# General statistics (no arguments)
	$lock = $db->lock();
	my $curr_items = scalar keys %cache;
	$lock->unlock();
	
	print $nsock "STAT pid $$
\n";
	print $nsock "STAT uptime " . (time() - $^T) . "\r\n";
	print $nsock "STAT time " . time() . "\r\n";
	print $nsock "STAT version memcached.pl v0.0\r\n";
	print $nsock "STAT pointer_size " . (64) . "\r\n";
	print $nsock "STAT rusage_user 0 0\r\n";
	print $nsock "STAT rusage_system 0 0\r\n";
	print $nsock "STAT curr_items $curr_items\r\n";
	print $nsock "STAT total_items $curr_items\r\n";
	print $nsock "STAT bytes 0\r\n";
	print $nsock "STAT curr_connections 1\r\n";
	print $nsock "STAT total_connections 1\r\n";
	print $nsock "STAT connection_structures 1\r\n";
	print $nsock "STAT reserved_fds 20\r\n";
	print $nsock "STAT cmd_get 0\r\n";
	print $nsock "STAT cmd_set 0\r\n";
	print $nsock "STAT cmd_flush 0\r\n";
	print $nsock "STAT cmd_touch 0\r\n";
	print $nsock "STAT get_hits 0\r\n";
	print $nsock "STAT get_misses 0\r\n";
	print $nsock "STAT delete_misses 0\r\n";
	print $nsock "STAT delete_hits 0\r\n";
	print $nsock "STAT incr_misses 0\r\n";
	print $nsock "STAT incr_hits 0\r\n";
	print $nsock "STAT decr_misses 0\r\n";
	print $nsock "STAT decr_hits 0\r\n";
	print $nsock "STAT cas_misses 0\r\n";
	print $nsock "STAT cas_hits 0\r\n";
	print $nsock "STAT cas_badval 0\r\n";
	print $nsock "STAT touch_hits 0\r\n";
	print $nsock "STAT touch_misses 0\r\n";
	print $nsock "STAT auth_cmds 0\r\n";
	print $nsock "STAT auth_errors 0\r\n";
	print $nsock "STAT evictions 0\r\n";
	print $nsock "STAT reclaimed 0\r\n";
	print $nsock "STAT bytes_read 0\r\n";
	print $nsock "STAT bytes_written 0\r\n";
	print $nsock "STAT limit_maxbytes 1048576\r\n";
	print $nsock "STAT threads 1\r\n";
	print $nsock "END\r\n";

	return;
}

# Slabs reassign command
# Protocol: slabs reassign <source class> <dest class>
# For this implementation, we return OK as we don't have slab classes.
sub m_slabs_reassign {
	my $len;
	my $source;
	my $dest;

	$len = scalar @line;
	if ($len == 2) {
		$source = shift(@line);
		$dest = shift(@line);
		# Validate both are integers
		if ($source !~ m/^\d+$/ || $dest !~ m/^\d+$/) {
			print $nsock "CLIENT_ERROR invalid slab class\r\n";
			return;
		}
		print $nsock "OK\r\n";
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
	}

	return;
}

# Slabs automove command
# Protocol: slabs automove <1|0>
# For this implementation, we return OK as we don't have slab classes.
sub m_slabs_automove {
	my $len;
	my $mode;

	$len = scalar @line;
	if ($len == 1) {
		$mode = shift(@line);
		if ($mode eq "1" || $mode eq "0") {
			print $nsock "OK\r\n";
		} else {
			print $nsock "CLIENT_ERROR invalid mode\r\n";
		}
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
	}

	return;
}

# show all keys in database
sub m_show_all {
	my $lock;
	my $key;
	my ($flags, $exptime, $bytes, $casunique, $val);

	$lock = $db->lock();
	foreach $key (keys %cache) {
		if (defined($cache{$key})) {
			# Check whether the key has actually expired first
			my $ref = thaw $cache{$key};
			($flags, $exptime, $bytes, $casunique, $val) = @$ref;
			if (($exptime != 0) and ($exptime < time())) {
				delete $cache{$key};
			} else {
				print $nsock "VALUE $key $flags $bytes\r\n";
				print $nsock $val, "\r\n";
			}
		}
	}
	$lock->unlock();
	print $nsock "END\r\n";

	return;
}

# append / prepend data to key value
# Per protocol: append/prepend do NOT accept flags or exptime parameters
sub m_append {
	my $noreply;
	my $len;
	my $mode = shift(@_);
	my $lock;
	my $key;
	my ($data, $bytes, $casunique, $val);
	my ($oflags, $oexptime, $obytes, $ocasunique, $oval);

	$len = scalar @line;
	# Protocol: append/prepend only accept: <key> <bytes> [noreply]
	if (($len == 2) or ($len == 3)) {
		$key = shift(@line);
		$bytes = shift(@line);
		if ($len == 3) {
			$noreply = shift(@line);
			if ($noreply ne "noreply") {
				undef($noreply);
			}
		}
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	# Validate key
	my ($key_valid, $key_error) = validate_key($key);
	if (!$key_valid) {
		print $nsock "CLIENT_ERROR $key_error\r\n";
		return;
	}

	# Validate bytes parameter per protocol (append/prepend don't accept flags/exptime)
	my ($bytes_valid, $bytes_error) = validate_numeric($bytes, "bytes");
	if (!$bytes_valid) {
		print $nsock "CLIENT_ERROR $bytes_error\r\n";
		return;
	}

	# Read and validate data block
	$val = eval { read_data_block($nsock, $bytes) };
	if ($@) {
		print $nsock "CLIENT_ERROR $@\r\n";
		return;
	}

	if (defined($cache{$key})) {
		# Check whether the key has actually expired first
		my $ref = thaw $cache{$key};
		($oflags, $oexptime, $obytes, $ocasunique, $oval) = @$ref;
		if (($oexptime != 0) and ($oexptime < time())) {
			$lock = $db->lock();
			delete $cache{$key};
			$lock->unlock();
			print $nsock "NOT_FOUND\r\n" unless defined($noreply);
			return;
		}
	} else {
		print $nsock "NOT_FOUND\r\n" unless defined($noreply);
		return;
	}

	# Per protocol: append/prepend ignore new flags and exptime.
	# Keep the existing key's flags and exptime.
	$casunique = set_casunique();
	if ($mode eq "append") {
		$val = $oval . $val;
	} elsif ($mode eq "prepend") {
		$val = $val . $oval;
	} else {
		print $nsock "CLIENT_ERROR cannot $mode\r\n";
		return;
	}

	# Preserve existing flags and exptime from the key
	$data = freeze [$oflags, $oexptime, length($val), $casunique, $val];
	$lock = $db->lock();
	$cache{$key} = $data;
	$lock->unlock();
	print $nsock "STORED\r\n" unless defined($noreply);

	return;
}

# verbosity dummy function
sub m_verbosity {
	my $noreply;

	$noreply = $line[$#line];
	if ($noreply ne "noreply") {
		undef($noreply);
	}

	print $nsock "OK\r\n" unless defined($noreply);

	return;
}

# increment / decrement the numeric value of a key
sub m_incr {
	my $noreply;
	my $len;
	my $num;
	my $mode = shift(@_);
	my $lock;
	my $key;
	my ($flags, $exptime, $bytes, $casunique, $val);

	$len = scalar @line;
	if (($len == 2) or ($len == 3)) {
		$key = shift(@line);
		$num = shift(@line);
		if ($len == 3) {
			$noreply = shift(@line);
			if ($noreply ne "noreply") {
				undef($noreply);
			}
		}
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	# Validate key
	my ($key_valid, $key_error) = validate_key($key);
	if (!$key_valid) {
		print $nsock "CLIENT_ERROR $key_error\r\n";
		return;
	}

	if (!($num =~ m/^\d+$/)) {
		print $nsock "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n";
		return;
	}

	# Check whether the key has actually expired first
	if (defined($cache{$key})) {
		my $ref = thaw $cache{$key};
		($flags, $exptime, $bytes, $casunique, $val) = @$ref;
		if (($exptime != 0) and ($exptime < time())) {
			$lock = $db->lock();
			delete $cache{$key};
			$lock->unlock();
		}
	}

	if (!defined($cache{$key})) {
		print $nsock "NOT_FOUND\r\n";
		return;
	}

	if (!($val =~ m/^\d+$/)) {
		print $nsock "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n";
		return;
	}

	if ($mode eq "incr") {
		$val += $num;
	} elsif ($mode eq "decr") {
		$val -= $num;
		$val = 0 unless ($val > 0);
	} else {
		print $nsock "CLIENT_ERROR cannot $mode\r\n";
		return;
	}

	$casunique = set_casunique();
	$lock = $db->lock();
	$cache{$key} = freeze [$flags, $exptime, $bytes, $casunique, $val];
	$lock->unlock();
	print $nsock $val, "\r\n";

	return;
}

# Update expiration time of an existing item without fetching.
sub m_touch {
	my $noreply;
	my $len;
	my $lock;
	my $key;
	my $exptime;
	my ($oflags, $oexptime, $obytes, $ocasunique, $oval);

	$len = scalar @line;
	if (($len == 2) or ($len == 3)) {
		$key = shift(@line);
		$exptime = shift(@line);
		if ($len == 3) {
			$noreply = shift(@line);
			if ($noreply ne "noreply") {
				undef($noreply);
			}
		}
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	# Validate key
	my ($key_valid, $key_error) = validate_key($key);
	if (!$key_valid) {
		print $nsock "CLIENT_ERROR $key_error\r\n";
		return;
	}

	# Validate exptime is a non-negative integer
	if ($exptime !~ m/^\d+$/) {
		print $nsock "CLIENT_ERROR invalid exptime\r\n";
		return;
	}

	# Check if key exists
	if (!defined($cache{$key})) {
		print $nsock "NOT_FOUND\r\n" unless defined($noreply);
		return;
	}

	# Get existing data
	my $ref = thaw $cache{$key};
	($oflags, $oexptime, $obytes, $ocasunique, $oval) = @$ref;

	# Apply expiration time rules
	if ($exptime != 0 && $exptime <= $month) {
		$exptime += time();
	}

	# Update with new expiration time, preserving all other fields
	$lock = $db->lock();
	$cache{$key} = freeze [$oflags, $exptime, $obytes, $ocasunique, $oval];
	$lock->unlock();
	
	print $nsock "TOUCHED\r\n" unless defined($noreply);

	return;
}

# Set signal handlers.
$SIG{CHLD} = \&REAPER;
$SIG{TERM} = \&SHUTDOWN;
$SIG{INT} = \&SHUTDOWN;
# SIGPIPE already set to 'IGNORE' above

# Track shutdown state
$shutdown = 0;

# EINTR constant
use Errno qw(EINTR);

# One month in seconds (30 days = 60*60*24*30 seconds).
# Per protocol: if exptime > this value, treat as Unix time; otherwise as relative offset.
$month = 60 * 60 * 24 * 30;

# Parse command line options
$listen = '127.0.0.1';
$port = 11311;
$dbdir = '.';
GetOptions(
	'listen=s' => \$listen,
	'port=i'   => \$port,
	'dbdir=s'  => \$dbdir,
) or die "Invalid command line options\n";

# Open the data store using DB_File
$db = tie %cache, 'DB_File', 
	"$dbdir/memcache.db",
	O_RDWR|O_CREAT,
	0666
	or die "cannot open database: $!\n";

# Create a process that produces casunique values.
pipe($FROM_PARENT, $TO_CHILD) or die "cannot pipe: $!\n";
pipe($FROM_CHILD, $TO_PARENT) or die "cannot pipe: $!\n";
$TO_CHILD->autoflush();
$TO_PARENT->autoflush();

$cas_pid = fork();
die "Cannot fork: $!" unless defined($cas_pid);
if ($cas_pid == 0) {
	# I am a child and my job is to produce unique CAS numbers. I do this
	# by incrementing a counter. Since we have persistence, then
	# $casunique is persistent too, so we have to iterate among stored 
	# keys to identify the largest value.
	my $key;
	my ($flags, $exptime, $bytes, $ocasunique, $oval);
	my $local_casunique = 1;

	# Set up signal handler for graceful shutdown of CAS child
	$SIG{TERM} = sub { exit 0 };
	$SIG{INT} = sub { exit 0 };

	foreach $key (keys %cache) {
		my $ref = thaw $cache{$key};
		($flags, $exptime, $bytes, $ocasunique, $oval) = @$ref;
		$local_casunique = $ocasunique unless ($local_casunique >= $ocasunique);
	}
	print "cas unique value starts from $local_casunique\n";

	while (<$FROM_PARENT>) {
		$local_casunique++;
		print $TO_PARENT $local_casunique, "\n";
	}
	
	exit 0;
}

# Create a listener.
$sock = IO::Socket::INET->new(
	LocalHost => "$listen:$port",
	Proto => 'tcp',
	Listen => 10,
	ReuseAddr => 1) or die "cannot open socket: $!\n";

# So now we are ready to handle incoming connections.
# Use non-blocking accept with timeout to allow shutdown check
$sock->blocking(0);

while (!$shutdown) {
	# Accept a connection with EINTR handling.
	# Use select with timeout to allow periodic shutdown checks
	my $rin = '';
	vec($rin, $sock->fileno, 1) = 1;
	my $nfound = select($rin, undef, undef, 0.5);
	
	if ($nfound > 0) {
		$nsock = $sock->accept();
		next unless $nsock;
	} elsif ($nfound == 0) {
		# Timeout - check shutdown flag
		next;
	} else {
		# Error on select
		if ($! != EINTR) {
			warn "select error: $!";
			last;
		}
		next;
	}

	# Fork a child.
	$pid = fork();
	die "Cannot fork: $!" unless defined($pid);
	if ($pid == 0) {
		# I am a child and I am waiting for input.
		# Set up signal handlers for graceful shutdown of connection child
		$SIG{TERM} = sub { close($nsock); exit 0 };
		$SIG{INT} = sub { close($nsock); exit 0 };
		$SIG{PIPE} = 'IGNORE';
		
		# Note: For the moment we are not checking whether 
		# commands sent by clients are terminated with \r\n.
		while (my $line = <$nsock>) {
			print $line;
			@line = split(/\s+/, $line);
			$cmd = shift(@line);
			if ($cmd eq "quit") {
				close($nsock);
				exit 0;
			} elsif ($cmd eq "version") {
				print $nsock "VERSION memcached.pl v0.0\r\n";
			} elsif ($cmd eq "set") {
				m_set();
			} elsif ($cmd eq "get") {
				m_get("get");
			} elsif ($cmd eq "add") {
				m_add("add");
			} elsif ($cmd eq "replace") {
				m_add("replace");
			} elsif ($cmd eq "append") {
				m_append("append");
			} elsif ($cmd eq "prepend") {
				m_append("prepend");
			} elsif ($cmd eq "cas") {
				m_cas();
			} elsif ($cmd eq "gets") {
				m_get("gets");
			} elsif ($cmd eq "delete") {
				m_delete();
			} elsif ($cmd eq "incr") {
				m_incr("incr");
			} elsif ($cmd eq "decr") {
				m_incr("decr");
			} elsif ($cmd eq "touch") {
				m_touch();
			} elsif ($cmd eq "stats") {
				m_stats();
			} elsif ($cmd eq "slabs") {
				# Handle slabs subcommands
				my $subcmd = shift(@line);
				if ($subcmd eq "reassign") {
					m_slabs_reassign();
				} elsif ($subcmd eq "automove") {
					m_slabs_automove();
				} else {
					print $nsock "CLIENT_ERROR unknown slabs command\r\n";
				}
			} elsif ($cmd eq "flush_all") {
				m_flush_all();
			} elsif ($cmd eq "show_all") {
				m_show_all();
			} elsif ($cmd eq "verbosity") {
				m_verbosity();
			} else {
				print $nsock "ERROR nonexistent command name.\r\n";
			}
		}
		
		close($nsock);
		exit 0;
	}
	
	# I am the parent - track child process and clear our socket reference
	$children{$pid} = 1;
	$nsock = undef;
}

# Cleanup on shutdown
SHUTDOWN: {
	# Signal we're shutting down to prevent new DB operations
	$shutdown_lock = 1;
	
	# Close listening socket
	if ($sock) {
		my $fileno = $sock->fileno;
		close($sock) if defined($fileno) && $fileno >= 0;
	}
	
	# Signal CAS child to exit if still running
	if ($cas_pid && kill(0, $cas_pid)) {
		close($TO_CHILD) if $TO_CHILD && defined($TO_CHILD->fileno);
		close($FROM_CHILD) if $FROM_CHILD && defined($FROM_CHILD->fileno);
		kill('TERM', $cas_pid);
		
		# Wait for CAS child with timeout
		my $wait_count = 0;
		while ($wait_count < 10 && kill(0, $cas_pid)) {
			waitpid($cas_pid, WNOHANG);
			$wait_count++;
			sleep 1;
		}
		# If still alive after timeout, force kill
		if (kill(0, $cas_pid)) {
			kill('KILL', $cas_pid);
			waitpid($cas_pid, 0);
		}
	}
	
	# Signal all active connection children to exit
	for my $child_pid (keys %children) {
		kill('TERM', $child_pid) if kill(0, $child_pid);
	}
	
	# Reap all remaining children with timeout
	my $reap_count = 0;
	while ($reap_count < 10) {
		my $waitpid;
		last unless ($waitpid = waitpid(-1, WNOHANG)) > 0;
		delete $children{$waitpid};
		$reap_count++;
		# If no more children, break early
		last unless %children;
		sleep 1;
	}
	
	# Force kill any remaining stubborn children
	for my $child_pid (keys %children) {
		kill('KILL', $child_pid) if kill(0, $child_pid);
		waitpid($child_pid, 0);
		delete $children{$child_pid};
	}
	
	# Close database after all children have exited
	if ($db) {
		$db->sync();
		untie %cache;
	}
}

# Normal exit - cleanup already handled in SHUTDOWN block
__END__

Copyright (c) 2012 Yiorgos Adamopoulos

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
