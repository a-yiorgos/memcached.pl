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

# 20 forked clients
my $num_clients = 20;
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

        # Task 1: 200 incr ctr 1
        for my $j (1..200) {
            print $csock "incr ctr 1\r\n";
            read_line($csock);
        }

        # Task 2: set and gets on client's own key
        my $my_key = "client_key_$i";
        print $csock "set $my_key 0 0 5\r\nval_$i\r\n";
        read_line($csock);

        print $csock "gets $my_key\r\n";
        my $ghead = read_line($csock);
        my ($my_cas) = ($ghead =~ /^VALUE $my_key 0 5 (\d+)\r\n$/);
        read_line($csock); read_line($csock);

        # Write CAS result to temporary file/pipe or exit code, or print to a standard pattern
        # Write to temporary file for parent collection
        open(my $fh, '>', "t_cas_client_$i.tmp") or exit 1;
        print $fh "$my_cas\n";
        close($fh);

        # Task 3: 10 clients race cas on race_key with initial token
        if ($i <= 10) {
            print $csock "cas race_key 0 0 5 $race_cas\r\nval_$i\r\n";
            my $res = read_line($csock);
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
my $timeout = 25;
my $elapsed = time() - $start_time;
while (@pids && (time() - $start_time) < $timeout) {
    @pids = grep {
        my $res = waitpid($_, WNOHANG);
        if ($res > 0) {
            0; # process finished
        } else {
            1; # process still running
        }
    } @pids;
    sleep(0.05);
}

is(scalar(@pids), 0, "All 20 client worker processes finished without hanging");

# Verify final ctr value == 4000
$sock = $server->connect();
print $sock "get ctr\r\n";
read_line($sock); # header
my $final_ctr = read_line($sock);
read_line($sock); # END
chomp $final_ctr;
$final_ctr =~ s/\r$//;
is($final_ctr, "4000", "Final counter value after 20*200 increments is exactly 4000");

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

# Verify CAS race results (exactly 1 STORED, 9 EXISTS)
my $stored_count = 0;
my $exists_count = 0;
for my $i (1..10) {
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
is($exists_count, 9, "CAS race on same key: exactly 9 clients got EXISTS");

my $total_elapsed = time() - $start_time;
cmp_ok($total_elapsed, '<', 30, "Concurrency test suite completed in under 30 seconds");

$server->stop();
done_testing();
