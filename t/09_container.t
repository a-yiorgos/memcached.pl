#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use IO::Socket::INET;
use Time::HiRes qw(sleep);

if (!$ENV{DOCKER_TESTS}) {
    plan skip_all => "DOCKER_TESTS=1 not set";
    exit 0;
}

my $engine = 'docker';
my $check_engine = `podman --version 2>/dev/null`;
if ($? == 0) {
    $engine = 'podman';
}

my $image_name = 'memcached-pl-test';

# 1. Build image
diag("Building $engine image...");
my $build_out = `$engine build -t $image_name . 2>&1`;
is($?, 0, "Image built successfully") or diag($build_out);

# 2. Check UID is 10001
my $uid = `$engine run --rm $image_name id -u`;
chomp $uid;
is($uid, '10001', "Container runs as UID 10001");

# 3. Create named volume for persistence
my $vol_name = 'memcached_pl_test_vol_' . int(rand(100000));
`$engine volume create $vol_name 2>&1`;

# 4. Run container rootless / read-only with security flags
my $host_port = 11311;
my $container_id = `$engine run -d -p $host_port:11311 --read-only --cap-drop=ALL --security-opt=no-new-privileges -v $vol_name:/data $image_name 2>&1`;
chomp $container_id;
ok($container_id, "Container started: $container_id");

# Connect and test version and set/get
sleep(2);

sub connect_container {
    my $sock;
    for (1..10) {
        $sock = IO::Socket::INET->new(
            PeerAddr => '127.0.0.1',
            PeerPort => $host_port,
            Proto    => 'tcp',
            Timeout  => 2,
        );
        last if $sock;
        sleep(0.5);
    }
    return $sock;
}

my $sock = connect_container();
ok($sock, "Connected to container over TCP");

if ($sock) {
    print $sock "version\r\n";
    my $vline = <$sock>;
    like($vline, qr/^VERSION /, "version command returned VERSION");

    print $sock "set docker_key 0 0 5\r\nhello\r\n";
    my $sline = <$sock>;
    is($sline, "STORED\r\n", "set key STORED in container");

    print $sock "get docker_key\r\n";
    my $ghead = <$sock>;
    my $gbody = <$sock>;
    my $gend  = <$sock>;
    is($ghead, "VALUE docker_key 0 5\r\n", "get header matched");
    is($gbody, "hello\r\n", "get body matched");
    is($gend, "END\r\n", "get END matched");
    close($sock);
}

# Restart container and verify persistence
`$engine restart $container_id 2>&1`;
sleep(2);

$sock = connect_container();
ok($sock, "Reconnected to container after restart");

if ($sock) {
    print $sock "get docker_key\r\n";
    my $ghead = <$sock>;
    my $gbody = <$sock>;
    my $gend  = <$sock>;
    is($ghead, "VALUE docker_key 0 5\r\n", "get header persisted after restart");
    is($gbody, "hello\r\n", "get body persisted after restart");
    is($gend, "END\r\n", "get END persisted after restart");
    close($sock);
}

# Cleanup container and volume
`$engine rm -f $container_id 2>&1`;
`$engine volume rm $vol_name 2>&1`;

done_testing();
