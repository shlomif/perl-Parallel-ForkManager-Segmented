package Parallel::ForkManager::Segmented::Base;

use strict;
use warnings;
use autodie;
use 5.014;

sub new
{
    my $class = shift;

    my $self = bless {}, $class;

    $self->_init(@_);

    return $self;
}

sub _init
{
    my ( $self, $args ) = @_;

    return;
}

sub process_args
{
    my ( $self, $args ) = @_;

    my $WITH_PM   = !$args->{disable_fork};
    my $items     = $args->{items};
    my $stream_cb = $args->{stream_cb};
    my $cb        = $args->{process_item};
    my $batch_cb  = $args->{process_batch};

    if ( $stream_cb && $items )
    {
        die "Do not specify both stream_cb and items!";
    }
    if ( $batch_cb && $cb )
    {
        die "Do not specify both process_item and process_batch!";
    }
    $batch_cb //= sub {
        foreach my $item ( @{ shift() } )
        {
            $cb->($item);
        }
        return;
    };
    my $nproc      = $args->{nproc};
    my $batch_size = $args->{batch_size};

    # Return prematurely on empty input to avoid calling $ch with undef()
    # at least once.
    if ($items)
    {
        if ( not @$items )
        {
            return;
        }
        $stream_cb = sub {
            my ($args) = @_;
            my $size = $args->{size};

            return +{ items =>
                    scalar( @$items ? [ splice @$items, 0, $size ] : undef() ),
            };
        };
    }
    return +{
        WITH_PM    => $WITH_PM,
        batch_cb   => $batch_cb,
        batch_size => $batch_size,
        nproc      => $nproc,
        stream_cb  => $stream_cb,
    };
}

sub serial_run
{
    my ( $self, $processed ) = @_;
    my ( $WITH_PM, $batch_cb, $batch_size, $nproc, $stream_cb, ) =
        @{$processed}{qw/ WITH_PM batch_cb batch_size nproc stream_cb  /};

    while (
        defined( my $batch = $stream_cb->( { size => $batch_size } )->{items} )
        )
    {
        $batch_cb->($batch);
    }
    return;
}

1;

__END__

=encoding utf8

=head1 NAME

Parallel::ForkManager::Segmented::Base - base class for Parallel::ForkManager::Segmented

=head1 SYNOPSIS

    package Parallel::ForkManager::Segmented::Mine;

    use parent 'Parallel::ForkManager::Segmented::Base';

    sub run
    {
    }

=head1 DESCRIPTION

This module provides the new() and process_args() methods for L<Parallel::ForkManager::Segmented>
and for L<Parallel::Map::Segmented> .

=head1 METHODS

=head2 my $obj = Parallel::ForkManager::Segmented::Base->new;

Initializes a new object.

=head2 my \%ret = $obj->process_args(+{ %ARGS })

Process the arguments passed to run().

=head2 $obj->serial_run($process_args)

Implement a (possibly naïve) serial run.

Added in version 0.4.0.

=head1 SEE ALSO

=over 4

=item * L<Parallel::ForkManager::Segmented>

=item * L<Parallel::ForkManager>

=item * L<Parallel::Map::Segmented>

Based on L<IO::Async::Function> and L<Parallel::Map> - a less snowflake approach.

=item * L<https://perl-begin.org/uses/multitasking/>

=back

=cut
