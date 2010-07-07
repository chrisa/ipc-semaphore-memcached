use strict;
use warnings;
use Test::More qw/ no_plan /;
use Test::Memcached;
use IPC::Semaphore::Memcached;

my $memd = Test::Memcached->new;
$memd->start;

my $server = sprintf('127.0.0.1:%d', $memd->option('tcp_port'));

my $sem = IPC::Semaphore::Memcached->new(
        lockname => 'test',
        clientid => "pid$$",
        count    => 10,
        holdtime => 600,
        servers  => [ $server ],
);
ok($sem);

for (0..1000) {
        ok($sem->down);
        ok($sem->up);
}
