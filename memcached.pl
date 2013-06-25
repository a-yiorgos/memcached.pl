#!/usr/bin/perl

# An (incomplete) implementation of the memcached text protocol
# http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
# in Perl with persistence using BerkeleyDB.
#
# Useful for debugging and / or testing purposes, not for production.

use BerkeleyDB;
use IO::Socket;
use FreezeThaw qw(freeze thaw);
use strict;

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

# Signal handler for forked children. It may need to improve as documented
# in perlipc.
sub REAPER {
	my $waitpid;

	$waitpid = wait();
	$SIG{CHLD} = \&REAPER;
}

# Request a new CAS value.
sub set_casunique {
	my $casunique;

	print TO_CHILD "get\n";
	$casunique = <FROM_CHILD>;
	chomp($casunique);

	return($casunique);
}

# set a key value.
sub m_set {
	my $noreply;
	my $len;
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);

	$len = @line;
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

	# A better check for the value input is needed.
	$val = <$nsock>;
	chop($val); chop($val);

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
	my $len;
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);
	my ($oflags, $oexptime, $obytes, $ocasunique, $oval);

	$len = @line;
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

	# A better check for the value input is needed.
	$val = <$nsock>;
	chop($val); chop($val);

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
	my $len;
	my $lock;
	my $key;
	my ($flags, $exptime, $bytes, $casunique, $val);

	$len = @line;
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
	my $len;
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);
	my ($oflags, $oexptime, $obytes, $ocasunique, $oval);

	$len = @line;
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

	# A better check for the value input is needed.
	$val = <$nsock>;
	chop($val); chop($val);

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
	my $len;
	my $lock;
	my $key;

	$len = @line;
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
	my $len;
	my $mode = shift(@_);
	my $lock;
	my $key;
	my ($data, $flags, $exptime, $bytes, $casunique, $val);
	my ($oflags, $oexptime, $obytes, $ocasunique, $oval);

	$len = @line;
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

	# A better check for the value input is needed.
	$val = <$nsock>;
	chop($val); chop($val);

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

	$data = freeze $flags, $exptime, length($val), $casunique, $val;
	$lock = $db->cds_lock();
	$cache{$key} = $data;
	$lock->cds_unlock();
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

	$len = @line;
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
	$cache{$key} = freeze $flags, $exptime, $bytes, $casunique, $val;
	$lock->cds_unlock();
	print $nsock $val, "\r\n";

	return;
}

# Set the signal handler.
$SIG{CHLD} = \&REAPER;

# One month in seconds.
$month = 68 * 60 * 24 * 30;

# We have to do some command line options processing here in the future.

# Create a database environment
$env = new BerkeleyDB::Env (
	-Home => '/tmp',
	-Flags  => DB_CREATE| DB_INIT_CDB | DB_INIT_MPOOL)
	or die "cannot open environment: $BerkeleyDB::Error\n";

# Open the data store. new or tie?
$db = tie %cache, 'BerkeleyDB::Hash', 
	-Filename => 'memcache.db',
	-Flags => DB_CREATE ,
	-Env => $env
	or die "cannot open database: $BerkeleyDB::Error\n";

# Create a process that produces casunique values.
pipe(FROM_PARENT, TO_CHILD) or die "cannot pipe: $!\n";
pipe(FROM_CHILD, TO_PARENT) or die "cannot pipe: $!\n";
TO_CHILD->autoflush();
TO_PARENT->autoflush();

$pid = fork();
die "Cannnot fork: $!" unless defined($pid);
if ($pid == 0) {
	# I am a child and my job is to produce unique CAS numbers. I do this
	# by incrementing a counter. Since we have persistence, then
	# $casunique is persistent too, so we have to iterate among stored 
	# keys to identify the largest value.
	my $key;
	my ($flags, $exptime, $bytes, $casunique, $ocasunique, $val);
	my $casunique = 1;

	foreach $key (keys %cache) {
		($flags, $exptime, $bytes, $ocasunique, $val) = thaw $cache{$key};
		$casunique = $ocasunique unless ($casunique >= $ocasunique);
	}
	print "cas unique value starts from $casunique\n";

	while (<FROM_PARENT>) {
		$casunique++;
		print TO_PARENT $casunique, "\n";
	}
}

# Create a listener.
$sock = new IO::Socket::INET (
	LocalHost => '127.0.0.1:11311',
	Proto => 'tcp',
	Listen => 10,
	ReuseAddr => 1) or die "cannot open socket: $!\n";

# So now we are ready to handle incoming connections.
while (1) {
	# Accept a connection.
	$nsock = $sock->accept();

	# Fork a child.
	$pid = fork();
	die "Cannnot fork: $!" unless defined($pid);
	if ($pid == 0) {
		# I am a child and I am waiting for input.
		# Note: For the moment we are not checking whether 
		# commands sent by clients are terminated with \r\n.
		while (<$nsock>) {
			print;
			@line = split(/\s+/);
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
	}

	# I am the parent.
}

# I never reach here.
close($sock);

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
