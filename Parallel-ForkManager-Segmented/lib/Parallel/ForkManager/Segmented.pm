package Parallel::ForkManager::Segmented;

use strict;
use warnings;

use List::MoreUtils qw/ natatime /;
use Parallel::ForkManager ();

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

sub run
{
    my ( $self, $args ) = @_;

    my $WITH_PM    = !$args->{disable_fork};
    my $items      = $args->{items};
    my $cb         = $args->{process_item};
    my $nproc      = $args->{nproc};
    my $batch_size = $args->{batch_size};

    my $pm;

    if ($WITH_PM)
    {
        $pm = Parallel::ForkManager->new($nproc);
    }
    $cb->( shift @$items );
    my $it = natatime $batch_size, @$items;
ITEMS:
    while ( my @batch = $it->() )
    {
        if ($WITH_PM)
        {
            my $pid = $pm->start;

            if ($pid)
            {
                next ITEMS;
            }
        }
        foreach my $item (@batch)
        {
            $cb->($item);
        }
        if ($WITH_PM)
        {
            $pm->finish;    # Terminates the child process
        }
    }
    if ($WITH_PM)
    {
        $pm->wait_all_children;
    }
    return;
}

1;

__END__

=head1 NAME

Parallel::ForkManager::Segmented - use Parallel::ForkManager on batches / segments of items.

=head1 METHODS

=head2 my $obj = Parallel::ForkManager::Segmented->new;

Initializes a new object.

=head2 $obj->run(+{ %ARGS });

Runs the processing. Accepts the following named arguments:

=over 4

=item * process_item

A reference to a subroutine that accepts one item and processes it.

=item * items

A reference to the array of items.

=item * nproc

The number of child processes to use.

=item * batch_size

The number of items in each batch.

=item * disable_fork

Disable forking and use of L<Parallel::ForkManager> and process the items
serially.

=back

=cut
