package IPC::Semaphore::Memcached;
use Moose;

our $VERSION = '0.01';

=head1 NAME

IPC::Semaphore::Memcached - cluster semaphore in memcached

=head1 SYNOPSIS

  my $sem = Semaphore->new(
    servers  => [ 'memcachehost:11211' ],
    lockname => 'test',
    clientid => "pid$$",
    count    => 10,
    holdtime => 600,
  );

  if ($sem->down) {
    ...
    $sem->up;
  }

=head1 DESCRIPTION

A cluster semaphore, backed by memcached. Intended for limiting access
to a resource to a finite number of cooperating clients.

The item stored in memcached is a small JSON document indicating the
current users of the semaphore and its parameters. A client updates
this document on "down" and "up" operations by first reading it, then
using the memcached CAS operation to safely replace it.

The semaphore is initialised in memcached by the first client to use
it, at which point its parameters are set, and will be respected by
subsequent clients.

The parameters which may be set are:

=over

=item holdtime

The maximum time the semaphore may be held by a client, in seconds.

=item count

The maximum number of holders.

=back

The same item in memcached is used repeatedly. You certainly want to
use a memcached configured to throw errors when it runs out of memory
- rather than silently dropping items.

=head1 METHODS

=cut

use Memcached::libmemcached qw/ :memcached_behavior /;
use Carp qw/ croak carp /;

our $MAX_CAS_TRIES = 10;

has 'servers'  => (isa => 'ArrayRef[Str]', required => 1, is => 'ro');
has 'clientid' => (isa => 'Str', required => 1, is => 'ro');
has 'lockname' => (isa => 'Str', required => 1, is => 'ro');
has 'count'    => (isa => 'Int', required => 1, is => 'ro', writer => '_set_count');
has 'holdtime' => (isa => 'Int', required => 1, is => 'ro', writer => '_set_holdtime');

has '_memc'  => (isa => 'Object', is => 'ro', lazy_build => 1);
has '_cas'   => (isa => 'Int', required => 0, is => 'rw');

sub _build__memc {
        my ($self) = @_;
        my $memc = Memcached::libmemcached->new;

        for my $server (@{ $self->servers }) {
                my ($host, $port) = $server =~ /^(.+):(\d+)$/;
                if ($host && $port) {
                        $memc->memcached_server_add($host, $port);
                }
        }
        $memc->memcached_behavior_set(MEMCACHED_BEHAVIOR_SUPPORT_CAS, 1);
        $memc->memcached_behavior_set(MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);

        # for access to the CAS unique value
        my $get_cb = sub {
                $self->_cas($_[2]);
                return ();
        };
        $memc->set_callback_coderefs(sub {()}, $get_cb);

        return $memc;
}

sub _memc_get {
        my $self = shift;
        my $retval;

        # We've turned on CAS, and that makes the 0.3102
        # Memcached-libmemcached warn on every get.
        {
                local $SIG{__WARN__} = sub {
                        my $warning = shift;
                        carp $warning unless index($warning, 'cas not fully supported') == 0;
                };

                $retval = $self->_memc->memcached_get(@_);
        }
        return $retval;
}

sub down {
        my ($self, $wait) = @_;
        my $tries = $MAX_CAS_TRIES;

        while ($tries > 0) {
                # fetch the current slots object
                my $slots = Semaphore::Slots->deserialize($self->_memc_get($self->lockname));

                # add us if possible
                unless ($slots->add($self->clientid, time())) {

                        # failing; no more slots at the moment
                        return 0;
                }

                # CAS in the object
                if ($self->_memc->memcached_cas($self->lockname, $slots->serialize, 0, 0, $self->_cas)) {

                        # worked, return from here
                        return 1;
                }

                # loop for retry.
                $tries--;
        }

        # no more goes, we didn't get a lock
        return 0;
}

sub up {
        my ($self) = @_;
        my $tries = $MAX_CAS_TRIES;

        while ($tries > 0) {
                # fetch the current slots object
                my $slots = Semaphore::Slots->deserialize($self->_memc_get($self->lockname));

                # remove us - always succeeds
                $slots->remove($self->clientid);

                # CAS in the object
                if ($self->_memc->memcached_cas($self->lockname, $slots->serialize, 0, 0, $self->_cas)) {

                        # worked, return from here
                        return 1;
                }

                # loop for retry.
                $tries--;
        }

        # no more goes, lock wasn't freed. unfortunate.
        return 0;
}

sub BUILD {
        my ($self) = @_;

        # make sure that the memcache document exists
        my $slots = Semaphore::Slots->new(
                max      => $self->count,
                holdtime => $self->holdtime,
        );

        if (!$self->_memc->memcached_add($self->lockname, $slots->serialize)) {
                if (my $json = $self->_memc_get($self->lockname)) {
                        $slots = Semaphore::Slots->deserialize($json);
                        if (defined $slots) {
                                $self->_set_count($slots->max);
                                $self->_set_holdtime($slots->holdtime);
                        }
                }
                else {
                        $slots = undef;
                }
        }

        croak "Can't initialise semaphore" unless defined $slots;
}

__PACKAGE__->meta->make_immutable;

package Semaphore::Slots;
use Moose;
use JSON::XS qw/ encode_json decode_json /;

has 'max'      => (isa => 'Int', required => 1, is => 'ro');
has 'holdtime' => (isa => 'Int', required => 1, is => 'ro');
has '_slots'   => (isa => 'HashRef[Str]', required => 0, is => 'rw', default => sub { { } });

sub serialize {
        my ($self) = @_;
        my $data = {
                max      => $self->max,
                holdtime => $self->holdtime,
                slots    => $self->_slots
        };
        return encode_json($data);
}

sub deserialize {
        my ($class, $json) = @_;
        return unless defined $json;
        my $data = decode_json($json);
        my $slots = $class->new(
                holdtime => $data->{holdtime},
                max      => $data->{max},
                _slots   => $data->{slots},
        );
        $slots->_expire_clients;
        return $slots;
}

sub _expire_clients {
        my ($self) = @_;

        my $slots = $self->_slots;
        my $time = time() - $self->holdtime;
        for my $client (keys %$slots) {
                if ($slots->{$client} < $time) {
                        delete $slots->{$client};
                }
        }
}

sub current {
        my ($self) = @_;
        return scalar keys %{ $self->_slots };
}

sub add {
        my ($self, $client, $time) = @_;
        if ($self->current < $self->max) {
                $self->_slots->{$client} = $time;
                return 1;
        }
        return 0;
}

sub remove {
        my ($self, $client) = @_;
        delete $self->_slots->{$client};
}

__PACKAGE__->meta->make_immutable;

__END__

=head1 AUTHOR

Chris Andrews <chris@nodnol.org>

=head1 COPYRIGHT

This program is Free software, you may redistribute it under the same
terms as Perl itself.

=cut
