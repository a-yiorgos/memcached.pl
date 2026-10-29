#!/usr/bin/perl

use strict;
use warnings;
use lib 't/lib';
use TestServer;
use Test::More;
use POSIX qw(:sys_wait_h _exit getpgrp);
use Time::HiRes qw(usleep time);
use BerkeleyDB;
use File::Temp qw(tempfile);
use File::stat;

END {
	# Kill any lingering memcached.pl processes spawned during tests
	if (opendir(my $dh, '/proc')) {
		while (my $entry = readdir($dh)) {
			next unless $entry =~ /^\d+$/;
			if ($entry != $$ && open(my $fh, '<', "/proc/$entry/cmdline")) {
				my $cmd = <$fh> // '';
				close($fh);
				if ($cmd =~ /memcached\.pl/) {
					kill 'KILL', $entry;
				}
			}
		}
		closedir($dh);
	}
}

sub get_pgid {
	my ($pid) = @_;
	return undef unless defined $pid;
	if (open(my $fh, '<', "/proc/$pid/stat")) {
		my $line = <$fh>;
		close($fh);
		if ($line && $line =~ /^\d+\s+\([^)]+\)\s+\S+\s+\d+\s+(\d+)/) {
			return $1;
		}
	}
	return undef;
}

sub force_kill_server {
	my ($server) = @_;
	return unless $server && $server->{pid};
	my $spid = $server->{pid};
	if (opendir(my $dh, '/proc')) {
		while (my $entry = readdir($dh)) {
			next unless $entry =~ /^\d+$/;
			if (open(my $fh, '<', "/proc/$entry/stat")) {
				my $line = <$fh>;
				close($fh);
				if ($line && $line =~ /^\d+\s+\([^)]+\)\s+\S+\s+(\d+)/) {
					if ($1 == $spid) {
						kill 'KILL', $entry;
					}
				}
			}
		}
		closedir($dh);
	}
	kill 'KILL', $spid;
	waitpid($spid, 0);
	$server->{pid} = undef;
}

sub find_worker_pid {
	my ($server_pid, $sock) = @_;
	my $sock_inode;
	if ($sock) {
		my $sstat = stat($sock);
		$sock_inode = $sstat ? $sstat->ino : undef;
	}

	my @child_pids;
	if (opendir(my $dh, '/proc')) {
		while (my $entry = readdir($dh)) {
			next unless $entry =~ /^\d+$/;
			if (open(my $fh, '<', "/proc/$entry/stat")) {
				my $line = <$fh>;
				close($fh);
				if ($line && $line =~ /^\d+\s+\([^)]+\)\s+\S+\s+(\d+)/) {
					my $ppid = $1;
					if ($ppid == $server_pid) {
						push @child_pids, $entry;
					}
				}
			}
		}
		closedir($dh);
	}

	if (defined $sock_inode && @child_pids) {
		for my $cpid (@child_pids) {
			if (opendir(my $fdh, "/proc/$cpid/fd")) {
				while (my $fentry = readdir($fdh)) {
					next if $fentry =~ /^\./;
					my $link = readlink("/proc/$cpid/fd/$fentry");
					if ($link && $link =~ /socket:\[$sock_inode\]/) {
						closedir($fdh);
						return $cpid;
					}
				}
				closedir($fdh);
			}
		}
	}

	if (@child_pids) {
		@child_pids = sort { $b <=> $a } @child_pids;
		return $child_pids[0];
	}

	return undef;
}

subtest "a) Slow reader" => sub {
	my $server = TestServer->new();
	my $setup_sock = $server->connect();
	my $big_val = "x" x 8388608; # 8 MB
	print $setup_sock "set big 0 0 8388608\r\n", $big_val, "\r\n";
	eval {
		my $res = read_line($setup_sock);
		is($res, "STORED\r\n", "stored 8MB value");
	};
	if ($@) {
		fail("Failed to store 8MB value: $@");
		force_kill_server($server);
		return;
	}

	# Client A sends get big and never reads response
	my $sock_a = $server->connect();
	print $sock_a "get big\r\n";
	usleep(100000);

	# Client B sends set x and get x within 5s
	my $sock_b = $server->connect();
	eval {
		print $sock_b "set x 0 0 1\r\n1\r\n";
		my $res_set = read_line($sock_b);
		is($res_set, "STORED\r\n", "Client B gets STORED while Client A slow reads");

		print $sock_b "get x\r\n";
		my $head = read_line($sock_b);
		is($head, "VALUE x 0 1\r\n", "Client B get x header");
		my $val = read_line($sock_b);
		is($val, "1\r\n", "Client B get x body");
		my $end = read_line($sock_b);
		is($end, "END\r\n", "Client B get x END");
	};
	if ($@) {
		fail("Client B timed out or failed: $@");
	}

	close($sock_a) if $sock_a;
	close($sock_b) if $sock_b;
	close($setup_sock) if $setup_sock;
	force_kill_server($server);
};

subtest "b) Killed worker" => sub {
	my $server = TestServer->new();
	my $setup_sock = $server->connect();
	my $big_val = "x" x 8388608; # 8 MB
	print $setup_sock "set big 0 0 8388608\r\n", $big_val, "\r\n";
	eval {
		my $res = read_line($setup_sock);
		is($res, "STORED\r\n", "stored 8MB value");
	};
	if ($@) {
		fail("Failed to store 8MB value: $@");
		force_kill_server($server);
		return;
	}

	# Client A sends get big
	my $sock_a = $server->connect();
	print $sock_a "get big\r\n";
	usleep(200000);

	# SIGKILL worker serving Client A
	my $wpid = find_worker_pid($server->{pid}, $sock_a);
	ok($wpid, "Found worker PID $wpid for Client A");
	if ($wpid) {
		kill 'KILL', $wpid;
		waitpid($wpid, 0);
	}

	# Client B sends set x and gets STORED within 5s
	my $sock_b = $server->connect();
	eval {
		print $sock_b "set x 0 0 1\r\n1\r\n";
		my $res_set = read_line($sock_b);
		is($res_set, "STORED\r\n", "Client B gets STORED after worker SIGKILL");
	};
	if ($@) {
		fail("Client B timed out or failed after worker killed: $@");
	}

	close($sock_a) if $sock_a;
	close($sock_b) if $sock_b;
	close($setup_sock) if $setup_sock;
	force_kill_server($server);
};

subtest "c) Stale lock recovery" => sub {
	my $server = TestServer->new();
	my $setup_sock = $server->connect();
	print $setup_sock "set prev 0 0 3\r\nold\r\n";
	eval {
		my $res = read_line($setup_sock);
		is($res, "STORED\r\n", "stored pre-stop key");
	};
	close($setup_sock) if $setup_sock;

	# Stop server
	force_kill_server($server);

	# In a forked child, open BerkeleyDB::Env on dbdir, tie memcache.db, take cds_lock, then SIGKILL self
	my $cpid = fork();
	if (!defined $cpid) {
		die "fork failed: $!\n";
	}
	if ($cpid == 0) {
		my %cache;
		my $env = new BerkeleyDB::Env(
			-Home  => $server->dbdir,
			-Flags => DB_CREATE | DB_INIT_CDB | DB_INIT_MPOOL
		);
		if ($env) {
			my $db = tie %cache, 'BerkeleyDB::Hash',
				-Filename => 'memcache.db',
				-Flags    => DB_CREATE,
				-Env      => $env;
			if ($db) {
				my $lock = $db->cds_lock();
				kill 'KILL', $$;
			}
		}
		POSIX::_exit(1);
	}
	waitpid($cpid, 0);

	# Restart server on same dbdir
	$server->restart();

	# Server must accept connections, return STORED for a set within 5s, and return pre-stop keys
	my $sock = $server->connect();
	if (!$sock) {
		fail("Server failed to accept connections after restart");
		force_kill_server($server);
		return;
	}

	eval {
		print $sock "set newkey 0 0 1\r\n1\r\n";
		my $res = read_line($sock);
		is($res, "STORED\r\n", "set newkey returned STORED after stale lock recovery");

		print $sock "get prev\r\n";
		my $head = read_line($sock);
		is($head, "VALUE prev 0 3\r\n", "pre-stop key header intact");
		my $val = read_line($sock);
		is($val, "old\r\n", "pre-stop key body intact");
		my $end = read_line($sock);
		is($end, "END\r\n", "pre-stop key END intact");
	};
	if ($@) {
		fail("Failed after server restart: $@");
	}

	close($sock) if $sock;
	force_kill_server($server);
};

subtest "d) Second instance" => sub {
	my $server = TestServer->new();

	my $port2 = TestServer::free_port();
	my ($err_fh, $err_file) = tempfile(UNLINK => 1);
	close($err_fh);

	my $p2 = fork();
	if (!defined $p2) {
		die "fork failed: $!\n";
	}
	if ($p2 == 0) {
		open(STDERR, '>', $err_file) or POSIX::_exit(1);
		exec($^X, "memcached.pl", "--listen", "127.0.0.1", "--port", $port2, "--dbdir", $server->dbdir) or POSIX::_exit(1);
	}

	my $exited = 0;
	my $exit_code = -1;
	my $start = time();
	while (time() - $start < 5) {
		my $res = waitpid($p2, WNOHANG);
		if ($res > 0) {
			$exited = 1;
			$exit_code = $? >> 8;
			last;
		}
		usleep(50000);
	}

	if (!$exited) {
		kill 'KILL', $p2;
		waitpid($p2, 0);
	}

	ok($exited, "Second instance exited within 5s");
	is($exit_code, 1, "Second instance exit code was 1");

	open(my $efh, '<', $err_file);
	my $err_content = do { local $/; <$efh> } // '';
	close($efh);

	my $dbdir = $server->dbdir;
	my $expected_msg = "another instance is using $dbdir";
	like($err_content, qr/\Q$expected_msg\E/, "Stderr contains 'another instance is using $dbdir'");

	# Verify first server keeps serving
	my $sock = $server->connect();
	if ($sock) {
		eval {
			print $sock "set first 0 0 1\r\n1\r\n";
			my $res = read_line($sock);
			is($res, "STORED\r\n", "First server continues serving requests");
		};
		if ($@) {
			fail("First server failed after second instance attempt: $@");
		}
		close($sock);
	} else {
		fail("First server connect failed after second instance attempt");
	}

	force_kill_server($server);
};

subtest "e) Shutdown" => sub {
	my $server = TestServer->new();
	my $sock1 = $server->connect();
	my $sock2 = $server->connect();

	my $spid = $server->{pid};
	ok($spid, "Server PID exists: $spid");

	# Get pgid of server while server is still running
	my $pgid = get_pgid($spid);

	if (!defined $pgid || $pgid == getpgrp()) {
		fail("Server process group was not set via setpgid(0,0)");
		force_kill_server($server);
		close($sock1) if $sock1;
		close($sock2) if $sock2;
		return;
	}

	# Send SIGTERM to server pid
	kill 'TERM', $spid;
	waitpid($spid, 0);

	my $pgid_alive = 1;
	my $start = time();
	while (time() - $start < 5) {
		$! = 0;
		my $cnt = kill(0, -$pgid);
		if ($cnt == 0) {
			$pgid_alive = 0;
			last;
		}
		usleep(50000);
	}

	is($pgid_alive, 0, "No process with PGID $pgid remains after SIGTERM");

	$server->{pid} = undef; # Prevent TestServer stop() from signaling again
	close($sock1) if $sock1;
	close($sock2) if $sock2;
	force_kill_server($server);
};

done_testing();
