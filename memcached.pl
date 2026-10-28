#!/usr/bin/perl

# An implementation of the memcached text protocol
# (http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt)
# in Perl with persistence using BerkeleyDB.
#
# Useful for debugging and / or testing purposes, not for production.

use strict;
use warnings;

use BerkeleyDB;
use IO::Socket;
use FreezeThaw qw(freeze thaw);
use POSIX qw(:sys_wait_h);

# Global variables.
our (
	$pid,
	$month,
	$env,
	$db,
	%cache,
	$sock,
	$nsock,
	$cmd,
	@line
);

# Signal handler for forked children.
sub REAPER {
	while (waitpid(-1, WNOHANG) > 0) {}
	$SIG{CHLD} = \&REAPER;
}

# Request a new CAS value.
sub set_casunique {
	my $casunique;

	print TO_CHILD "get\n";
	$casunique = <FROM_CHILD>;
	if (defined $casunique) {
		chomp($casunique);
	} else {
		$casunique = 1;
	}

	return $casunique;
}

# Read exact payload bytes from socket.
sub read_bytes {
	my ($bytes) = @_;
	my $data = '';
	my $read_len = 0;

	while ($read_len < $bytes) {
		my $buffer;
		my $n = read($nsock, $buffer, $bytes - $read_len);
		if (!defined $n || $n == 0) {
			return undef; # Socket closed or error
		}
		$data .= $buffer;
		$read_len += $n;
	}

	# Read trailing \r\n
	my $crlf = '';
	read($nsock, $crlf, 2);

	return $data;
}

# set a key value.
sub m_set {
	my $noreply;
	my $len = @line;
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);

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
		print $nsock "ERROR\r\n";
		return;
	}

	if (!defined($bytes) || $bytes !~ /^\d+$/) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	$val = read_bytes($bytes);
	if (!defined $val) {
		print $nsock "CLIENT_ERROR bad data chunk\r\n";
		return;
	}

	# Is $exptime < $month?
	if (($exptime != 0) and ($exptime < $month)) {
		$exptime += time();
	}

	$casunique = set_casunique();
	$data = freeze $flags, $exptime, $bytes, $casunique, $val;

	$lock = $db->cds_lock();
	$cache{$key} = $data;
	$lock->cds_unlock();
	print $nsock "STORED\r\n" unless(defined($noreply));

	return;
}

# check and set a key value.
sub m_cas {
	my $noreply;
	my $len = @line;
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);
	my ($oflags, $oexptime, $obytes, $ocasunique, $oval);

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
		print $nsock "ERROR\r\n";
		return;
	}

	if (!defined($bytes) || $bytes !~ /^\d+$/) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	$val = read_bytes($bytes);
	if (!defined $val) {
		print $nsock "CLIENT_ERROR bad data chunk\r\n";
		return;
	}

	# Check whether the key has actually expired first
	if (defined($cache{$key})) {
		($oflags, $oexptime, $obytes, $ocasunique, $oval) = thaw $cache{$key};
		if (($oexptime != 0) and ($oexptime < time())) {
			$lock = $db->cds_lock();
			delete $cache{$key};
			$lock->cds_unlock();
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

	# Is $exptime < $month?
	if (($exptime != 0) and ($exptime < $month)) {
		$exptime += time();
	}

	$casunique = set_casunique();
	$data = freeze $flags, $exptime, $bytes, $casunique, $val;

	$lock = $db->cds_lock();
	$cache{$key} = $data;
	$lock->cds_unlock();
	print $nsock "STORED\r\n" unless(defined($noreply));

	return;
}

# get a key value.
sub m_get {
	my $mode = shift(@_);
	my $len = @line;
	my $lock;
	my $key;
	my ($flags, $exptime, $bytes, $casunique, $val);

	if ($len == 0) {
		print $nsock "ERROR\r\n";
		return;
	}

	# get the keys
	while ($key = shift(@line)) {
		if (defined($cache{$key})) {
			# Check whether the key has actually expired first
			($flags, $exptime, $bytes, $casunique, $val) = thaw $cache{$key};
			if (($exptime != 0) and ($exptime < time())) {
				$lock = $db->cds_lock();
				delete $cache{$key};
				$lock->cds_unlock();
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
	my $len = @line;
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);
	my ($oflags, $oexptime, $obytes, $ocasunique, $oval);

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
		print $nsock "ERROR\r\n";
		return;
	}

	if (!defined($bytes) || $bytes !~ /^\d+$/) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	$val = read_bytes($bytes);
	if (!defined $val) {
		print $nsock "CLIENT_ERROR bad data chunk\r\n";
		return;
	}

	# Check whether the key has actually expired first.
	if (defined($cache{$key})) {
		($oflags, $oexptime, $obytes, $ocasunique, $oval) = thaw $cache{$key};
		if (($oexptime != 0) and ($oexptime < time())) {
			$lock = $db->cds_lock();
			delete $cache{$key};
			$lock->cds_unlock();
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

	# Is $exptime < $month?
	if (($exptime != 0) and ($exptime < $month)) {
		$exptime += time();
	}

	$casunique = set_casunique();
	$data = freeze $flags, $exptime, $bytes, $casunique, $val;

	$lock = $db->cds_lock();
	$cache{$key} = $data;
	$lock->cds_unlock();
	print $nsock "STORED\r\n" unless(defined($noreply));

	return;
}

# delete a key if it exists.
sub m_delete {
	my $noreply;
	my $len = @line;
	my $lock;
	my $key;

	if (($len == 1) or ($len == 2)) {
		$key = shift(@line);
		if ($len == 2) {
			$noreply = shift(@line);
			if ($noreply ne "noreply") {
				undef($noreply);
				print $nsock "CLIENT_ERROR bad command line format.  Usage: delete \<key\> [noreply]\r\n";
				return;
			}
		}
	} else {
		print $nsock "ERROR\r\n";
		return;
	}

	if (!defined($cache{$key})) {
		print $nsock "NOT_FOUND\r\n";
		return;
	}

	$lock = $db->cds_lock();
	delete $cache{$key};
	$lock->cds_unlock();
	print $nsock "DELETED\r\n" unless defined($noreply);

	return;
}

# flush all keys from the database
sub m_flush_all {
	my $lock;
	my $key;

	$lock = $db->cds_lock();
	foreach $key (keys %cache) {
		delete $cache{$key};
	}
	$lock->cds_unlock();
	print $nsock "OK\r\n";

	return;
}

# show all keys in database
sub m_show_all {
	my $lock;
	my $key;
	my ($flags, $exptime, $bytes, $casunique, $val);

	$lock = $db->cds_lock();
	foreach $key (keys %cache) {
		if (defined($cache{$key})) {
			# Check whether the key has actually expired first
			($flags, $exptime, $bytes, $casunique, $val) = thaw $cache{$key};
			if (($exptime != 0) and ($exptime < time())) {
				delete $cache{$key};
			} else {
				print $nsock "VALUE $key $flags $bytes\r\n";
				print $nsock $val, "\r\n";
			}
		}
	}
	$lock->cds_unlock();
	print $nsock "END\r\n";

	return;
}

# append / prepend data to key value
sub m_append {
	my $noreply;
	my $len = @line;
	my $mode = shift(@_);
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);
	my ($oflags, $oexptime, $obytes, $ocasunique, $oval);

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
		print $nsock "ERROR\r\n";
		return;
	}

	if (!defined($bytes) || $bytes !~ /^\d+$/) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	$val = read_bytes($bytes);
	if (!defined $val) {
		print $nsock "CLIENT_ERROR bad data chunk\r\n";
		return;
	}

	if (defined($cache{$key})) {
		# Check whether the key has actually expired first
		($oflags, $oexptime, $obytes, $ocasunique, $oval) = thaw $cache{$key};
		if (($oexptime != 0) and ($oexptime < time())) {
			$lock = $db->cds_lock();
			delete $cache{$key};
			$lock->cds_unlock();
			print $nsock "NOT_FOUND\r\n" unless defined($noreply);
			return;
		}
	} else {
		print $nsock "NOT_FOUND\r\n" unless defined($noreply);
		return;
	}

	# Is $exptime < $month?
	if (($exptime != 0) and ($exptime < $month)) {
		$exptime += time();
	}

	$casunique = set_casunique();
	if ($mode eq "append") {
		$val = $oval . $val;
	} elsif ($mode eq "prepend") {
		$val = $val . $oval;
	} else {
		print $nsock "CLIENT_ERROR cannot $mode\r\n";
		return;
	}

	$casunique = set_casunique();
	$data = freeze $oflags, $oexptime, length($val), $casunique, $val;
	$lock = $db->cds_lock();
	$cache{$key} = $data;
	$lock->cds_unlock();
	print $nsock "STORED\r\n" unless defined($noreply);

	return;
}

# verbosity dummy function
sub m_verbosity {
	my $noreply;

	if (@line) {
		$noreply = $line[$#line];
		if ($noreply ne "noreply") {
			undef($noreply);
		}
	}

	print $nsock "OK\r\n" unless defined($noreply);

	return;
}

# increment / decrement the numeric value of a key
sub m_incr {
	my $noreply;
	my $len = @line;
	my $num;
	my $mode = shift(@_);
	my $lock;
	my $key;
	my ($flags, $exptime, $bytes, $casunique, $val);

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
		print $nsock "ERROR\r\n";
		return;
	}

	if (!($num =~ m/^\d+$/)) {
		print $nsock "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n";
		return;
	}

	# Check whether the key has actually expired first
	if (defined($cache{$key})) {
		($flags, $exptime, $bytes, $casunique, $val) = thaw $cache{$key};
		if (($exptime != 0) and ($exptime < time())) {
			$lock = $db->cds_lock();
			delete $cache{$key};
			$lock->cds_unlock();
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
	$lock = $db->cds_lock();
	$cache{$key} = freeze $flags, $exptime, length($val), $casunique, $val;
	$lock->cds_unlock();
	print $nsock $val, "\r\n";

	return;
}

# Set the signal handler.
$SIG{CHLD} = \&REAPER;

# One month in seconds (30 days * 24 hours * 60 mins * 60 secs)
$month = 60 * 60 * 24 * 30;

# Create a database environment
$env = new BerkeleyDB::Env (
	-Home => '/tmp',
	-Flags  => DB_CREATE | DB_INIT_CDB | DB_INIT_MPOOL)
	or die "cannot open environment: $BerkeleyDB::Error\n";

# Open the data store.
$db = tie %cache, 'BerkeleyDB::Hash', 
	-Filename => 'memcache.db',
	-Flags => DB_CREATE ,
	-Env => $env
	or die "cannot open database: $BerkeleyDB::Error\n";

# Create pipes for CAS value production child process.
pipe(FROM_PARENT, TO_CHILD) or die "cannot pipe: $!\n";
pipe(FROM_CHILD, TO_PARENT) or die "cannot pipe: $!\n";
TO_CHILD->autoflush(1);
TO_PARENT->autoflush(1);

$pid = fork();
die "Cannot fork: $!" unless defined($pid);
if ($pid == 0) {
	# Child process: generates unique CAS numbers.
	close(FROM_CHILD);
	close(TO_CHILD);

	my $key;
	my ($flags, $exptime, $bytes, $casunique, $ocasunique, $val);
	my $cas_counter = 1;

	foreach $key (keys %cache) {
		if (defined $cache{$key}) {
			($flags, $exptime, $bytes, $ocasunique, $val) = thaw $cache{$key};
			if (defined $ocasunique && $ocasunique > $cas_counter) {
				$cas_counter = $ocasunique;
			}
		}
	}

	while (<FROM_PARENT>) {
		$cas_counter++;
		print TO_PARENT $cas_counter, "\n";
	}
	exit 0;
}

close(FROM_PARENT);
close(TO_PARENT);

# Create a listener socket.
$sock = new IO::Socket::INET (
	LocalHost => '127.0.0.1:11311',
	Proto => 'tcp',
	Listen => 10,
	ReuseAddr => 1) or die "cannot open socket: $!\n";

# Main connection acceptance loop
while (1) {
	$nsock = $sock->accept();
	next unless defined $nsock;

	$pid = fork();
	die "Cannot fork: $!" unless defined($pid);
	if ($pid == 0) {
		# Worker child process
		close($sock);

		while (my $raw_line = <$nsock>) {
			$raw_line =~ s/[\r\n]+$//;
			next if $raw_line eq '';

			@line = split(/\s+/, $raw_line);
			$cmd = shift(@line);
			next unless defined $cmd;

			if ($cmd eq "quit") {
				close($nsock);
				exit 0;
			} elsif ($cmd eq "version") {
				print $nsock "VERSION memcached.pl v0.1\r\n";
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
			} elsif ($cmd eq "stats") {
				print $nsock "SERVER_ERROR command not implemented.\r\n";
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

	close($nsock);
}

close($sock);
