#!/usr/bin/perl

use strict;
use warnings;
use lib 't/lib';
use TestServer;
use Test::More;
use POSIX qw(:sys_wait_h);
use Time::HiRes qw(time sleep);

my $start_time = time();

my $server = TestServer->new();

# Initialize counter 'ctr' to 0
my $sock = $server->connect();
print $sock "set ctr 0 0 1\r\n0\r\n";
is(read_line($sock), "STORED\r\n", "initialized counter key ctr");

# Initialize key for CAS race
print $sock "set race_key 0 0 4\r\ninit\r\n";
is(read_line($sock), "STORED\r\n", "initialized race_key");

print $sock "gets race_key\r\n";
my $race_head = read_line($sock);
my ($race_cas) = ($race_head =~ /^VALUE race_key 0 4 (\d+)\r\n$/);
read_line($sock); read_line($sock);
close($sock);

ok($race_cas, "got initial CAS for race_key: $race_cas");

# 10 forked clients
my $num_clients = 10;
my $iterations  = 20;
my @pids;

for my $i (1..$num_clients) {
    my $pid = fork();
    if (!defined $pid) {
        die "Cannot fork client process $i: $!";
    }
    if ($pid == 0) {
        my $csock = $server->connect();
        if (!$csock) {
            exit 1;
        }

        # Task 1: 20 incr ctr 1
        for my $j (1..$iterations) {
            print $csock "incr ctr 1\r\n";
            read_line($csock, 30);
        }

        # Task 2: set and gets on client's own key
        my $my_key = "client_key_$i";
        my $my_val = "val_$i";
        my $vlen   = length($my_val);
        print $csock "set $my_key 0 0 $vlen\r\n$my_val\r\n";
        read_line($csock, 30);

        print $csock "gets $my_key\r\n";
        my $ghead = read_line($csock, 30);
        my ($my_cas) = ($ghead =~ /^VALUE $my_key 0 $vlen (\d+)\r\n$/);
        read_line($csock, 30); read_line($csock, 30);

        # Write CAS result to temporary file
        open(my $fh, '>', "t_cas_client_$i.tmp") or exit 1;
        print $fh "$my_cas\n";
        close($fh);

        # Task 3: 5 clients race cas on race_key with initial token
        if ($i <= 5) {
            my $rval = "val_$i";
            my $rlen = length($rval);
            print $csock "cas race_key 0 0 $rlen $race_cas\r\n$rval\r\n";
            my $res = read_line($csock, 30);
            open(my $rfh, '>', "t_race_client_$i.tmp") or exit 1;
            print $rfh $res;
            close($rfh);
        }

        close($csock);
        exit 0;
    }
    push @pids, $pid;
}

# Wait for all children with timeout
my $timeout = 60;
while (@pids && (time() - $start_time) < $timeout) {
    @pids = grep {
        my $res = waitpid($_, WNOHANG);
        if ($res > 0) {
            0; # process finished
        } else {
            1; # process still running
        }
    } @pids;
    sleep(0.02);
}

is(scalar(@pids), 0, "All 10 client worker processes finished without hanging");

# Verify final ctr value == num_clients * iterations
my $expected_ctr = $num_clients * $iterations;
$sock = $server->connect();
if (!$sock) {
    if (-f $server->{stderr_file}) {
        open(my $efh, '<', $server->{stderr_file});
        my $err = do { local $/; <$efh> };
        close($efh);
        diag("SERVER STDERR AT FAIL:\n$err");
    }
    die "Server connect failed!";
}
print $sock "get ctr\r\n";
read_line($sock); # header
my $final_ctr = read_line($sock);
read_line($sock); # END
chomp $final_ctr;
$final_ctr =~ s/\r$//;
is($final_ctr, "$expected_ctr", "Final counter value after increments is exactly $expected_ctr");

# Collect collected CAS values and verify uniqueness
my %cas_seen;
for my $i (1..$num_clients) {
    my $fn = "t_cas_client_$i.tmp";
    if (-f $fn) {
        open(my $fh, '<', $fn);
        my $cas_val = <$fh>;
        close($fh);
        unlink $fn;
        if (defined $cas_val) {
            chomp $cas_val;
            $cas_seen{$cas_val}++;
        }
    }
}

is(scalar(keys %cas_seen), $num_clients, "All collected CAS values across clients are unique");

# Verify CAS race results (exactly 1 STORED, 4 EXISTS)
my $stored_count = 0;
my $exists_count = 0;
for my $i (1..5) {
    my $fn = "t_race_client_$i.tmp";
    if (-f $fn) {
        open(my $fh, '<', $fn);
        my $res = <$fh>;
        close($fh);
        unlink $fn;
        if (defined $res) {
            if ($res eq "STORED\r\n") {
                $stored_count++;
            } elsif ($res eq "EXISTS\r\n") {
                $exists_count++;
            }
        }
    }
}

is($stored_count, 1, "CAS race on same key: exactly 1 client got STORED");
is($exists_count, 4, "CAS race on same key: exactly 4 clients got EXISTS");

my $total_elapsed = time() - $start_time;
cmp_ok($total_elapsed, '<', 60, "Concurrency test suite completed in under 60 seconds");

$server->stop();
done_testing();
