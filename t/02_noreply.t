#!/usr/bin/perl

use strict;
use warnings;
use lib 't/lib';
use TestServer;
use Test::More;

my $server = TestServer->new();

sub verify_noreply {
    my ($cmd, $desc) = @_;
    my $sock = $server->connect();
    print $sock $cmd;
    print $sock "version\r\n";
    my $line = read_line($sock);
    like($line, qr/^VERSION /, "$desc - next response is VERSION string without intermediate output");
    close($sock);
}

# Initial seed
my $sock = $server->connect();
print $sock "set nr_key 0 0 5\r\nhello\r\n";
is(read_line($sock), "STORED\r\n", "seed nr_key");
print $sock "set nr_num 0 0 1\r\n5\r\n";
is(read_line($sock), "STORED\r\n", "seed nr_num");
close($sock);

verify_noreply("set nr_key 0 0 5 noreply\r\nhello\r\n", "set noreply");
verify_noreply("add nr_add 0 0 3 noreply\r\nfoo\r\n", "add noreply");
verify_noreply("replace nr_key 0 0 5 noreply\r\nworld\r\n", "replace noreply");
verify_noreply("append nr_key 0 0 3 noreply\r\n123\r\n", "append noreply");
verify_noreply("prepend nr_key 0 0 3 noreply\r\nabc\r\n", "prepend noreply");

# gets to obtain CAS
$sock = $server->connect();
print $sock "gets nr_key\r\n";
my $vhead = read_line($sock);
my ($cas) = ($vhead =~ /^VALUE nr_key \d+ \d+ (\d+)\r\n$/);
read_line($sock); # body
read_line($sock); # END
close($sock);

verify_noreply("cas nr_key 0 0 5 $cas noreply\r\nfinal\r\n", "cas noreply");
verify_noreply("incr nr_num 2 noreply\r\n", "incr noreply");
verify_noreply("decr nr_num 1 noreply\r\n", "decr noreply");
verify_noreply("delete nr_key noreply\r\n", "delete noreply");
verify_noreply("flush_all noreply\r\n", "flush_all noreply");
verify_noreply("verbosity 1 noreply\r\n", "verbosity noreply");

$server->stop();
done_testing();
