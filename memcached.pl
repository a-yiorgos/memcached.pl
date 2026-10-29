#!/usr/bin/perl

# An implementation of the memcached text protocol
# (http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt)
# in Perl with persistence using BerkeleyDB.

use strict;
use warnings;

use BerkeleyDB;
use IO::Socket;
use FreezeThaw qw(freeze thaw);
use POSIX qw(:sys_wait_h _exit);
use Getopt::Long;
use File::Path qw(make_path);
use Math::BigInt;
use Errno qw(EINTR);

# Options with defaults
my $listen_addr = '127.0.0.1';
my $port        = 11311;
my $dbdir       = './data';

GetOptions(
	'listen=s' => \$listen_addr,
	'port=i'   => \$port,
	'dbdir=s'  => \$dbdir,
) or die "Error in command line arguments\n";

make_path($dbdir) unless -d $dbdir;

# Ignore SIGPIPE and automatically reap child processes
$SIG{PIPE} = 'IGNORE';
$SIG{CHLD} = 'IGNORE';

$SIG{INT}  = sub { POSIX::_exit(0); };
$SIG{TERM} = sub { POSIX::_exit(0); };
$SIG{HUP}  = sub { POSIX::_exit(0); };
$SIG{QUIT} = sub { POSIX::_exit(0); };

# One month in seconds (30 days * 24 hours * 60 mins * 60 secs)
my $month = 60 * 60 * 24 * 30;

# Initialise DB in parent to set up environment and CAS counter
{
	my (%parent_cache, %parent_cas);
	my $p_env = new BerkeleyDB::Env(
		-Home  => $dbdir,
		-Flags => DB_CREATE | DB_INIT_CDB | DB_INIT_MPOOL
	) or die "cannot open environment: $BerkeleyDB::Error\n";

	my $p_db = tie %parent_cache, 'BerkeleyDB::Hash',
		-Filename => 'memcache.db',
		-Flags    => DB_CREATE,
		-Env      => $p_env
		or die "cannot open database: $BerkeleyDB::Error\n";

	my $p_cas_db = tie %parent_cas, 'BerkeleyDB::Hash',
		-Filename => 'cas.db',
		-Flags    => DB_CREATE,
		-Env      => $p_env
		or die "cannot open cas database: $BerkeleyDB::Error\n";

	my $max_cas = 0;
	foreach my $key (keys %parent_cache) {
		if (defined $parent_cache{$key}) {
			my ($flags, $exptime, $bytes, $casunique, $val) = thaw $parent_cache{$key};
			if (defined $casunique && $casunique > $max_cas) {
				$max_cas = $casunique;
			}
		}
	}

	my $cur_cas = $parent_cas{'counter'} // 0;
	if ($max_cas >= $cur_cas) {
		$parent_cas{'counter'} = $max_cas;
	}

	undef $p_db;
	untie %parent_cache;
	undef $p_cas_db;
	untie %parent_cas;
	undef $p_env;
}

# Validation helpers
sub is_valid_key {
	my ($k) = @_;
	return 0 unless defined $k;
	return ($k =~ /^[\x21-\x7e]{1,250}$/) ? 1 : 0;
}

sub is_valid_flags {
	my ($f) = @_;
	return 0 unless defined $f && $f =~ /^\d+$/;
	return (length($f) < 10 || ($f cmp "4294967295") <= 0) ? 1 : 0 if length($f) <= 10;
	return 0;
}

sub is_valid_exptime {
	my ($e) = @_;
	return 0 unless defined $e && $e =~ /^-?\d+$/;
	return 1;
}

sub is_valid_bytes {
	my ($b) = @_;
	return 0 unless defined $b && $b =~ /^\d+$/;
	return 1;
}

sub is_valid_uint64 {
	my ($v) = @_;
	return 0 unless defined $v && $v =~ /^\d+$/;
	return (length($v) < 20 || ($v cmp "18446744073709551615") <= 0) ? 1 : 0 if length($v) <= 20;
	return 0;
}

sub parse_exptime {
	my ($exptime) = @_;
	if ($exptime < 0) {
		return -1; # Expired
	} elsif ($exptime == 0) {
		return 0;
	} elsif ($exptime <= $month) {
		return time() + $exptime;
	} else {
		return $exptime;
	}
}

sub next_cas {
	my ($cas_store_ref) = @_;
	my $c = $cas_store_ref->{'counter'} // 0;
	$c++;
	$cas_store_ref->{'counter'} = $c;
	return $c;
}

sub read_bytes {
	my ($nsock, $bytes) = @_;
	my $data = '';
	my $read_len = 0;

	while ($read_len < $bytes) {
		my $buffer = '';
		my $n = read($nsock, $buffer, $bytes - $read_len);
		if (!defined $n || $n == 0) {
			return (undef, "EOF");
		}
		$data .= $buffer;
		$read_len += $n;
	}

	# Read trailing \r\n
	my $crlf = '';
	my $c_read = 0;
	while ($c_read < 2) {
		my $buf = '';
		my $n = read($nsock, $buf, 2 - $c_read);
		if (!defined $n || $n == 0) {
			return (undef, "EOF");
		}
		$crlf .= $buf;
		$c_read += $n;
	}

	if ($crlf ne "\r\n") {
		my $buf = '';
		while (read($nsock, $buf, 1)) {
			last if $buf eq "\n";
		}
		return (undef, "BAD_CHUNK");
	}

	return ($data, "OK");
}

sub handle_storage {
	my ($nsock, $db, $cache_ref, $cas_store_ref, $mode, $line_str) = @_;
	my @tokens = split(' ', $line_str, -1);

	my $noreply;
	if (@tokens > 0 && $tokens[-1] eq 'noreply') {
		$noreply = 1;
		pop @tokens;
	}

	my $bytes_to_read;
	my $negative_bytes;
	if (@tokens >= 4) {
		if (is_valid_bytes($tokens[-1])) {
			$bytes_to_read = $tokens[-1];
		} elsif ($tokens[-1] =~ /^-\d+$/) {
			$negative_bytes = 1;
		}
	}

	my $cmd_valid = (@tokens == 4);
	my ($key, $flags, $exptime, $bytes);
	if ($cmd_valid) {
		($key, $flags, $exptime, $bytes) = @tokens;
		$cmd_valid = (is_valid_key($key) && is_valid_flags($flags) && is_valid_exptime($exptime) && is_valid_bytes($bytes));
	}

	if ($negative_bytes) {
		my $discard = <$nsock>;
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	if (defined $bytes_to_read) {
		my ($val, $err) = read_bytes($nsock, $bytes_to_read);
		if (!defined $val) {
			if ($err eq "BAD_CHUNK") {
				print $nsock "CLIENT_ERROR bad data chunk\r\n";
			}
			return;
		}
		if (!$cmd_valid) {
			print $nsock "CLIENT_ERROR bad command line format\r\n";
			return;
		}

		my $real_exptime = parse_exptime($exptime);

		my $lock = $db->cds_lock();
		my $casunique = next_cas($cas_store_ref);
		if (defined $cache_ref->{$key}) {
			my ($oflags, $oexptime, $obytes, $ocas, $oval) = thaw $cache_ref->{$key};
			if ($oexptime != 0 && $oexptime < time()) {
				delete $cache_ref->{$key};
			}
		}

		if ($mode eq "add" && defined $cache_ref->{$key}) {
			undef $lock;
			print $nsock "NOT_STORED\r\n" unless $noreply;
			return;
		}
		if ($mode eq "replace" && !defined $cache_ref->{$key}) {
			undef $lock;
			print $nsock "NOT_STORED\r\n" unless $noreply;
			return;
		}

		my $data = freeze $flags, $real_exptime, $bytes, $casunique, $val;
		$cache_ref->{$key} = $data;
		undef $lock;
		print $nsock "STORED\r\n" unless $noreply;
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
	}
}

sub handle_cas {
	my ($nsock, $db, $cache_ref, $cas_store_ref, $line_str) = @_;
	my @tokens = split(' ', $line_str, -1);

	my $noreply;
	if (@tokens > 0 && $tokens[-1] eq 'noreply') {
		$noreply = 1;
		pop @tokens;
	}

	my $bytes_to_read;
	my $negative_bytes;
	if (@tokens >= 5) {
		if (is_valid_bytes($tokens[-2])) {
			$bytes_to_read = $tokens[-2];
		} elsif ($tokens[-2] =~ /^-\d+$/) {
			$negative_bytes = 1;
		}
	} elsif (@tokens >= 4) {
		if (is_valid_bytes($tokens[-1])) {
			$bytes_to_read = $tokens[-1];
		} elsif ($tokens[-1] =~ /^-\d+$/) {
			$negative_bytes = 1;
		}
	}

	my $cmd_valid = (@tokens == 5);
	my ($key, $flags, $exptime, $bytes, $casunique);
	if ($cmd_valid) {
		($key, $flags, $exptime, $bytes, $casunique) = @tokens;
		$cmd_valid = (is_valid_key($key) && is_valid_flags($flags) && is_valid_exptime($exptime) && is_valid_bytes($bytes) && is_valid_uint64($casunique));
	}

	if ($negative_bytes) {
		my $discard = <$nsock>;
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	if (defined $bytes_to_read) {
		my ($val, $err) = read_bytes($nsock, $bytes_to_read);
		if (!defined $val) {
			if ($err eq "BAD_CHUNK") {
				print $nsock "CLIENT_ERROR bad data chunk\r\n";
			}
			return;
		}
		if (!$cmd_valid) {
			print $nsock "CLIENT_ERROR bad command line format\r\n";
			return;
		}

		my $real_exptime = parse_exptime($exptime);

		my $lock = $db->cds_lock();
		my $new_cas = next_cas($cas_store_ref);
		my ($oflags, $oexptime, $obytes, $ocas, $oval);
		if (defined $cache_ref->{$key}) {
			($oflags, $oexptime, $obytes, $ocas, $oval) = thaw $cache_ref->{$key};
			if ($oexptime != 0 && $oexptime < time()) {
				delete $cache_ref->{$key};
			}
		}

		if (!defined $cache_ref->{$key}) {
			undef $lock;
			print $nsock "NOT_FOUND\r\n" unless $noreply;
			return;
		}

		if ($casunique ne $ocas) {
			undef $lock;
			print $nsock "EXISTS\r\n" unless $noreply;
			return;
		}

		my $data = freeze $flags, $real_exptime, $bytes, $new_cas, $val;
		$cache_ref->{$key} = $data;
		undef $lock;
		print $nsock "STORED\r\n" unless $noreply;
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
	}
}

sub handle_append_prepend {
	my ($nsock, $db, $cache_ref, $cas_store_ref, $mode, $line_str) = @_;
	my @tokens = split(' ', $line_str, -1);

	my $noreply;
	if (@tokens > 0 && $tokens[-1] eq 'noreply') {
		$noreply = 1;
		pop @tokens;
	}

	my $bytes_to_read;
	my $negative_bytes;
	if (@tokens >= 4) {
		if (is_valid_bytes($tokens[-1])) {
			$bytes_to_read = $tokens[-1];
		} elsif ($tokens[-1] =~ /^-\d+$/) {
			$negative_bytes = 1;
		}
	}

	my $cmd_valid = (@tokens == 4);
	my ($key, $flags, $exptime, $bytes);
	if ($cmd_valid) {
		($key, $flags, $exptime, $bytes) = @tokens;
		$cmd_valid = (is_valid_key($key) && is_valid_flags($flags) && is_valid_exptime($exptime) && is_valid_bytes($bytes));
	}

	if ($negative_bytes) {
		my $discard = <$nsock>;
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	if (defined $bytes_to_read) {
		my ($val, $err) = read_bytes($nsock, $bytes_to_read);
		if (!defined $val) {
			if ($err eq "BAD_CHUNK") {
				print $nsock "CLIENT_ERROR bad data chunk\r\n";
			}
			return;
		}
		if (!$cmd_valid) {
			print $nsock "CLIENT_ERROR bad command line format\r\n";
			return;
		}

		my $lock = $db->cds_lock();
		my $new_cas = next_cas($cas_store_ref);
		my ($oflags, $oexptime, $obytes, $ocas, $oval);
		if (defined $cache_ref->{$key}) {
			($oflags, $oexptime, $obytes, $ocas, $oval) = thaw $cache_ref->{$key};
			if ($oexptime != 0 && $oexptime < time()) {
				delete $cache_ref->{$key};
			}
		}

		if (!defined $cache_ref->{$key}) {
			undef $lock;
			print $nsock "NOT_FOUND\r\n" unless $noreply;
			return;
		}

		my $new_val = ($mode eq "append") ? $oval . $val : $val . $oval;
		my $data = freeze $oflags, $oexptime, length($new_val), $new_cas, $new_val;
		$cache_ref->{$key} = $data;
		undef $lock;
		print $nsock "STORED\r\n" unless $noreply;
	} else {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
	}
}

sub handle_get {
	my ($nsock, $db, $cache_ref, $mode, $line_str) = @_;
	my @keys = split(' ', $line_str, -1);

	if (@keys == 0) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	for my $k (@keys) {
		if (!is_valid_key($k)) {
			print $nsock "CLIENT_ERROR bad command line format\r\n";
			return;
		}
	}

	my $lock = $db->cds_lock();
	while (defined(my $key = shift(@keys))) {
		if (defined $cache_ref->{$key}) {
			my ($flags, $exptime, $bytes, $casunique, $val) = thaw $cache_ref->{$key};
			if ($exptime != 0 && $exptime < time()) {
				delete $cache_ref->{$key};
			} else {
				if ($mode eq "get") {
					print $nsock "VALUE $key $flags $bytes\r\n";
				} elsif ($mode eq "gets") {
					print $nsock "VALUE $key $flags $bytes $casunique\r\n";
				}
				print $nsock $val, "\r\n";
			}
		}
	}
	undef $lock;
	print $nsock "END\r\n";
}

sub handle_delete {
	my ($nsock, $db, $cache_ref, $line_str) = @_;
	my @tokens = split(' ', $line_str, -1);

	my $noreply;
	if (@tokens > 0 && $tokens[-1] eq 'noreply') {
		$noreply = 1;
		pop @tokens;
	}

	if (@tokens != 1) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	my $key = $tokens[0];

	if (!is_valid_key($key)) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	my $lock = $db->cds_lock();
	if (defined $cache_ref->{$key}) {
		my ($flags, $exptime, $bytes, $cas, $val) = thaw $cache_ref->{$key};
		if ($exptime != 0 && $exptime < time()) {
			delete $cache_ref->{$key};
		}
	}

	if (!defined $cache_ref->{$key}) {
		undef $lock;
		print $nsock "NOT_FOUND\r\n" unless $noreply;
		return;
	}

	delete $cache_ref->{$key};
	undef $lock;
	print $nsock "DELETED\r\n" unless $noreply;
}

sub handle_incr_decr {
	my ($nsock, $db, $cache_ref, $cas_store_ref, $mode, $line_str) = @_;
	my @tokens = split(' ', $line_str, -1);

	my $noreply;
	if (@tokens > 0 && $tokens[-1] eq 'noreply') {
		$noreply = 1;
		pop @tokens;
	}

	if (@tokens != 2) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	my ($key, $delta) = @tokens;

	if (!is_valid_key($key) || !is_valid_uint64($delta)) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	my $lock = $db->cds_lock();
	my $new_cas = next_cas($cas_store_ref);
	my ($flags, $exptime, $bytes, $cas, $val);
	if (defined $cache_ref->{$key}) {
		($flags, $exptime, $bytes, $cas, $val) = thaw $cache_ref->{$key};
		if ($exptime != 0 && $exptime < time()) {
			delete $cache_ref->{$key};
		}
	}

	if (!defined $cache_ref->{$key}) {
		undef $lock;
		print $nsock "NOT_FOUND\r\n" unless $noreply;
		return;
	}

	if ($val !~ /^\d+$/) {
		undef $lock;
		print $nsock "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n";
		return;
	}

	my $new_val;
	if (length($val) < 19 && length($delta) < 19) {
		if ($mode eq "incr") {
			my $nv = $val + $delta;
			if ($nv > 18446744073709551615) {
				my $b_val   = Math::BigInt->new($val);
				my $b_delta = Math::BigInt->new($delta);
				my $mod     = Math::BigInt->new("18446744073709551616");
				$b_val->badd($b_delta);
				$b_val->bmod($mod);
				$new_val = $b_val->bstr();
			} else {
				$new_val = "$nv";
			}
		} else {
			my $nv = $val - $delta;
			$new_val = ($nv < 0) ? "0" : "$nv";
		}
	} else {
		my $b_val   = Math::BigInt->new($val);
		my $b_delta = Math::BigInt->new($delta);
		my $mod     = Math::BigInt->new("18446744073709551616");
		if ($mode eq "incr") {
			$b_val->badd($b_delta);
			$b_val->bmod($mod);
		} else {
			$b_val->bsub($b_delta);
			if ($b_val->is_neg()) {
				$b_val = Math::BigInt->new(0);
			}
		}
		$new_val = $b_val->bstr();
	}

	$cache_ref->{$key} = freeze $flags, $exptime, length($new_val), $new_cas, $new_val;
	undef $lock;

	print $nsock "$new_val\r\n" unless $noreply;
}

sub handle_flush_all {
	my ($nsock, $db, $cache_ref, $line_str) = @_;
	my @tokens = split(' ', $line_str, -1);

	my $noreply;
	if (@tokens == 1 && $tokens[0] eq 'noreply') {
		$noreply = 1;
	} elsif (@tokens > 0) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	my $lock = $db->cds_lock();
	foreach my $key (keys %$cache_ref) {
		delete $cache_ref->{$key};
	}
	undef $lock;

	print $nsock "OK\r\n" unless $noreply;
}

sub handle_show_all {
	my ($nsock, $db, $cache_ref, $line_str) = @_;
	if (length $line_str > 0) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	my $lock = $db->cds_lock();
	foreach my $key (keys %$cache_ref) {
		if (defined $cache_ref->{$key}) {
			my ($flags, $exptime, $bytes, $casunique, $val) = thaw $cache_ref->{$key};
			if ($exptime != 0 && $exptime < time()) {
				delete $cache_ref->{$key};
			} else {
				print $nsock "VALUE $key $flags $bytes\r\n";
				print $nsock $val, "\r\n";
			}
		}
	}
	undef $lock;
	print $nsock "END\r\n";
}

sub handle_verbosity {
	my ($nsock, $line_str) = @_;
	my @tokens = split(' ', $line_str, -1);

	my $noreply;
	if (@tokens == 2 && $tokens[1] eq 'noreply') {
		$noreply = 1;
		pop @tokens;
	}

	if (@tokens != 1 || $tokens[0] !~ /^\d+$/) {
		print $nsock "CLIENT_ERROR bad command line format\r\n";
		return;
	}

	print $nsock "OK\r\n" unless $noreply;
}

# Create a listener socket.
my $sock = new IO::Socket::INET(
	LocalAddr => "$listen_addr:$port",
	Proto     => 'tcp',
	Listen    => 128,
	ReuseAddr => 1
) or die "cannot open socket on $listen_addr:$port: $!\n";

# Main connection acceptance loop
while (1) {
	my $nsock;
	do {
		$! = 0;
		$nsock = $sock->accept();
	} while (!defined $nsock && $!{EINTR});

	next unless defined $nsock;

	my $pid = fork();
	die "Cannot fork: $!" unless defined($pid);

	if ($pid == 0) {
		# Worker child process
		CORE::close($sock);
		$nsock->autoflush(1);

		# Open worker's own BerkeleyDB environment and databases
		my (%cache, %cas_store);
		my $env = new BerkeleyDB::Env(
			-Home  => $dbdir,
			-Flags => DB_CREATE | DB_INIT_CDB | DB_INIT_MPOOL
		) or die "cannot open environment in worker: $BerkeleyDB::Error\n";

		my $db = tie %cache, 'BerkeleyDB::Hash',
			-Filename => 'memcache.db',
			-Flags    => DB_CREATE,
			-Env      => $env
			or die "cannot open database in worker: $BerkeleyDB::Error\n";

		my $cas_db = tie %cas_store, 'BerkeleyDB::Hash',
			-Filename => 'cas.db',
			-Flags    => DB_CREATE,
			-Env      => $env
			or die "cannot open cas database in worker: $BerkeleyDB::Error\n";

		while (my $raw_line = <$nsock>) {
			$raw_line =~ s/[\r\n]+$//;

			my $trimmed = $raw_line;
			$trimmed =~ s/^\s+//;
			$trimmed =~ s/\s+$//;
			if ($trimmed eq '') {
				if (!defined $raw_line) {
					CORE::close($nsock);
					POSIX::_exit(0);
				}
				next;
			}

			my ($cmd, $rest) = split(/\s+/, $trimmed, 2);
			$rest //= '';

			if ($cmd eq "quit") {
				if (length $rest > 0) {
					print $nsock "CLIENT_ERROR bad command line format\r\n";
				} else {
					undef $db; untie %cache;
					undef $cas_db; untie %cas_store;
					undef $env;
					close($nsock);
					POSIX::_exit(0);
				}
			} elsif ($cmd eq "version") {
				if (length $rest > 0) {
					print $nsock "CLIENT_ERROR bad command line format\r\n";
				} else {
					print $nsock "VERSION memcached.pl v0.1\r\n";
				}
			} elsif ($cmd eq "set") {
				handle_storage($nsock, $db, \%cache, \%cas_store, "set", $rest);
			} elsif ($cmd eq "add") {
				handle_storage($nsock, $db, \%cache, \%cas_store, "add", $rest);
			} elsif ($cmd eq "replace") {
				handle_storage($nsock, $db, \%cache, \%cas_store, "replace", $rest);
			} elsif ($cmd eq "append") {
				handle_append_prepend($nsock, $db, \%cache, \%cas_store, "append", $rest);
			} elsif ($cmd eq "prepend") {
				handle_append_prepend($nsock, $db, \%cache, \%cas_store, "prepend", $rest);
			} elsif ($cmd eq "cas") {
				handle_cas($nsock, $db, \%cache, \%cas_store, $rest);
			} elsif ($cmd eq "get") {
				handle_get($nsock, $db, \%cache, "get", $rest);
			} elsif ($cmd eq "gets") {
				handle_get($nsock, $db, \%cache, "gets", $rest);
			} elsif ($cmd eq "delete") {
				handle_delete($nsock, $db, \%cache, $rest);
			} elsif ($cmd eq "incr") {
				handle_incr_decr($nsock, $db, \%cache, \%cas_store, "incr", $rest);
			} elsif ($cmd eq "decr") {
				handle_incr_decr($nsock, $db, \%cache, \%cas_store, "decr", $rest);
			} elsif ($cmd eq "flush_all") {
				handle_flush_all($nsock, $db, \%cache, $rest);
			} elsif ($cmd eq "show_all") {
				handle_show_all($nsock, $db, \%cache, $rest);
			} elsif ($cmd eq "verbosity") {
				handle_verbosity($nsock, $rest);
			} else {
				print $nsock "ERROR\r\n";
			}
		}

		undef $db; untie %cache;
		undef $cas_db; untie %cas_store;
		undef $env;
		CORE::close($nsock);
		POSIX::_exit(0);
	}

	CORE::close($nsock);
}

close($sock);
warn "Parent memcached.pl exited main loop!\n";
