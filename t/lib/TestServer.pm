package TestServer;

use strict;
use warnings;
use IO::Socket::INET;
use IO::Select;
use File::Temp qw(tempdir tempfile);
use Test::More;
use POSIX qw(:sys_wait_h);
use Time::HiRes qw(usleep sleep);
use Exporter 'import';

our @EXPORT = qw(
    read_line
    read_bytes
);

our @EXPORT_OK = qw(
    read_line
    read_bytes
    free_port
);

my @INSTANCES;

END {
    for my $inst (@INSTANCES) {
        $inst->stop();
    }
}

sub free_port {
    my $sock = IO::Socket::INET->new(
        LocalAddr => '127.0.0.1',
        LocalPort => 0,
        Proto     => 'tcp',
        Listen    => 1,
        ReuseAddr => 1,
    ) or die "Cannot bind to find free port: $!";
    my $port = $sock->sockport;
    close($sock);
    return $port;
}

sub new {
    my ($class, %opts) = @_;

    my $port = $opts{port} || free_port();
    my $dbdir = $opts{dbdir};
    my $cleanup_dbdir = 0;
    if (!$dbdir) {
        $dbdir = tempdir(CLEANUP => 1);
        $cleanup_dbdir = 1;
    }

    my ($stderr_fh, $stderr_file) = tempfile(UNLINK => 1);
    close($stderr_fh);

    my $self = bless {
        port          => $port,
        dbdir         => $dbdir,
        cleanup_dbdir => $cleanup_dbdir,
        stderr_file   => $stderr_file,
        pid           => undef,
        stopped       => 0,
    }, $class;

    push @INSTANCES, $self;

    $self->_start_process();
    return $self;
}

sub _start_process {
    my ($self) = @_;

    my $pid = fork();
    if (!defined $pid) {
        die "Cannot fork memcached server process: $!";
    }

    if ($pid == 0) {
        open(STDERR, ">>", $self->{stderr_file}) or die "Cannot redirect STDERR: $!";
        STDERR->autoflush(1);

        my @cmd = ($^X, "memcached.pl", "--listen", "127.0.0.1", "--port", $self->{port}, "--dbdir", $self->{dbdir});
        exec(@cmd) or die "Failed to exec memcached.pl: $!";
    }

    $self->{pid} = $pid;

    # Wait up to 5s for port to accept connections
    my $connected = 0;
    my $start_time = time();
    while (time() - $start_time < 5) {
        my $sock = IO::Socket::INET->new(
            PeerAddr => '127.0.0.1',
            PeerPort => $self->{port},
            Proto    => 'tcp',
            Timeout  => 1,
        );
        if ($sock) {
            close($sock);
            $connected = 1;
            last;
        }
        usleep(50000);
    }
}

sub port  { $_[0]->{port} }
sub dbdir { $_[0]->{dbdir} }

sub connect {
    my ($self) = @_;
    my $sock;
    for my $attempt (1..20) {
        $sock = IO::Socket::INET->new(
            PeerAddr => '127.0.0.1',
            PeerPort => $self->{port},
            Proto    => 'tcp',
            Timeout  => 5,
        );
        last if $sock;
        usleep(50000);
    }
    if ($sock) {
        $sock->autoflush(1);
    }
    return $sock;
}

sub restart {
    my ($self, $signal) = @_;
    $signal ||= 'TERM';

    if ($self->{pid}) {
        kill $signal, $self->{pid};
        waitpid($self->{pid}, 0);
        $self->{pid} = undef;
    }

    usleep(100000);
    $self->_start_process();
}

sub stop {
    my ($self) = @_;
    return if $self->{stopped};
    $self->{stopped} = 1;

    if ($self->{pid}) {
        kill 'TERM', $self->{pid};
        my $kid;
        eval {
            local $SIG{ALRM} = sub { die "alarm\n" };
            alarm(3);
            $kid = waitpid($self->{pid}, 0);
            alarm(0);
        };
        if ($@ || $kid != $self->{pid}) {
            kill 'KILL', $self->{pid};
            waitpid($self->{pid}, 0);
        }
        $self->{pid} = undef;
    }

    $self->check_stderr();
}

sub check_stderr {
    my ($self) = @_;
    return unless -f $self->{stderr_file};
    open(my $fh, '<', $self->{stderr_file}) or return;
    my $content = do { local $/; <$fh> };
    close($fh);

    if (defined $content && length $content) {
        Test::More::fail("Server stderr output detected:\n$content");
    }
}

sub read_line {
    my ($sock, $timeout) = @_;
    $timeout ||= 5;
    my $sel = IO::Select->new($sock);
    my $line = '';
    while (1) {
        if (!$sel->can_read($timeout)) {
            Test::More::fail("Timeout (>${timeout}s) waiting for line from server");
            die "Timeout waiting for line\n";
        }
        my $buf = '';
        my $n = sysread($sock, $buf, 1);
        if (!defined $n || $n == 0) {
            last;
        }
        $line .= $buf;
        last if $buf eq "\n";
    }
    return $line;
}

sub read_bytes {
    my ($sock, $len, $timeout) = @_;
    $timeout ||= 5;
    my $sel = IO::Select->new($sock);
    my $data = '';
    while (length($data) < $len) {
        if (!$sel->can_read($timeout)) {
            Test::More::fail("Timeout (>${timeout}s) waiting for $len bytes from server");
            die "Timeout waiting for bytes\n";
        }
        my $buf = '';
        my $to_read = $len - length($data);
        my $n = sysread($sock, $buf, $to_read);
        if (!defined $n || $n == 0) {
            last;
        }
        $data .= $buf;
    }
    return $data;
}

1;
