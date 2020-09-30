package Parallel::ForkManager::Segmented;

use strict;
use warnings;
use 5.014;

use Parallel::ForkManager::Segmented::Base v0.4.0 ();
use parent 'Parallel::ForkManager::Segmented::Base';

use Parallel::ForkManager ();

sub run
{
    my ( $self, $args ) = @_;

    my $processed = $self->process_args($args);
    return if not $processed;
    my ( $WITH_PM, $batch_cb, $batch_size, $nproc, $stream_cb, ) =
        @{$processed}{qw/ WITH_PM batch_cb batch_size nproc stream_cb  /};

    return $self->serial_run($processed) if not $WITH_PM;

    my $pm;

    $pm = Parallel::ForkManager->new($nproc);
    my $batch = $stream_cb->( { size => 1 } )->{items};
    return if not defined $batch;
    $batch_cb->($batch);
ITEMS:
    while (
        defined( $batch = $stream_cb->( { size => $batch_size } )->{items} ) )
    {
        my $pid = $pm->start;

        if ($pid)
        {
            next ITEMS;
        }
        $batch_cb->($batch);
        $pm->finish;    # Terminates the child process
    }
    $pm->wait_all_children;
    return;
}

1;

__END__

=head1 NAME

Parallel::ForkManager::Segmented - use Parallel::ForkManager on batches /
segments of items.

=head1 SYNOPSIS

    use Parallel::ForkManager::Segmented ();
    use Path::Tiny qw/ path /;

    my $NUM    = 30;
    my $temp_d = Path::Tiny->tempdir;

    my @queue = ( 1 .. $NUM );
    my $proc  = sub {
        my $fn = shift;
        $temp_d->child($fn)->spew_utf8("Wrote $fn .\n");
        return;
    };
    Parallel::ForkManager::Segmented->new->run(
        {
            WITH_PM      => 1,
            items        => \@queue,
            nproc        => 3,
            batch_size   => 8,
            process_item => $proc,
        }
    );

=head1 DESCRIPTION

This module builds upon L<Parallel::ForkManager> allowing one to pass a
batch (or "segment") of several items for processing inside a worker. This
is done in order to hopefully reduce the forking/exiting overhead.

=head1 METHODS

=head2 my $obj = Parallel::ForkManager::Segmented->new;

Initializes a new object.

=head2 my \%ret = $obj->process_args(+{ %ARGS })

TBD.

=head2 $obj->run(+{ %ARGS });

Runs the processing. Accepts the following named arguments:

=over 4

=item * process_item

A reference to a subroutine that accepts one item and processes it.

=item * items

A reference to the array of items.

=item * stream_cb

A reference to a callback for returning new batches of items (cannot
be specified along with 'items'.)

Accepts a hash ref with the key 'size' specifying an integer of the maximal
item count.

Returns a hash ref with the key 'items' pointing to an array reference of
items or undef() upon end-of-stream.

E.g:

        $stream_cb = sub {
            my ($args) = @_;
            my $size = $args->{size};

            return +{ items =>
                    scalar( @$items ? [ splice @$items, 0, $size ] : undef() ),
            };
        };

Added at version 0.4.0.

=item * nproc

The number of child processes to use.

=item * batch_size

The number of items in each batch.

=item * disable_fork

Disable forking and use of L<Parallel::ForkManager> and process the items
serially.

=item * process_batch

[Added in v0.2.0.]

A reference to a subroutine that accepts a reference to an array of a whole batch
that is processed as a whole. If specified, C<process_item> is not used.

Example:

    use strict;
    use warnings;
    use Test::More tests => 30;
    use Parallel::ForkManager::Segmented ();
    use Path::Tiny qw/ path /;

    {
        my $NUM    = 30;
        my $temp_d = Path::Tiny->tempdir;

        my @queue = ( 1 .. $NUM );
        my $proc  = sub {
            foreach my $fn ( @{ shift(@_) } )
            {
                $temp_d->child($fn)->spew_utf8("Wrote $fn .\n");
            }
            return;
        };
        Parallel::ForkManager::Segmented->new->run(
            {
                WITH_PM       => 1,
                items         => \@queue,
                nproc         => 3,
                batch_size    => 8,
                process_batch => $proc,
            }
        );
        foreach my $i ( 1 .. $NUM )
        {
            # TEST*30
            is( $temp_d->child($i)->slurp_utf8, "Wrote $i .\n", "file $i", );
        }
    }

=back

=head1 SEE ALSO

=over 4

=item * L<Parallel::ForkManager>

=item * L<IO::Async::Function> - a less snowflake approach.

=item * L<https://perl-begin.org/uses/multitasking/>

=back

=cut
