package Parallel::ForkManager::Segmented;

use strict;
use warnings;
use 5.014;

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

=head1 SYNOPSIS

    #! /usr/bin/env perl

    use strict;
    use warnings;
    use 5.014;
    use Cwd ();

    use WML_Frontends::Wml::Runner ();
    use Parallel::ForkManager::Segmented ();

    my $UNCOND  = $ENV{UNCOND} // '';
    my $CMD     = shift @ARGV;
    my (@dests) = @ARGV;

    my $PWD       = Cwd::getcwd();
    my @WML_FLAGS = (
        qq%
    --passoption=2,-X3074 --passoption=2,-I../lib/ --passoption=3,-I../lib/ --passoption=3,-w -I../lib/ $ENV{LATEMP_WML_FLAGS} -p1-3,5,7 -DROOT~. -DLATEMP_THEME=sf.org1 -I $HOME/apps/wml
    % =~ /(\S+)/g
    );

    my $T2_SRC_DIR = 't2';
    my $T2_DEST    = "dest/$T2_SRC_DIR";

    chdir($T2_SRC_DIR);

    my $obj = WML_Frontends::Wml::Runner->new;

    sub is_newer
    {
        my $file1 = shift;
        my $file2 = shift;
        my @stat1 = stat($file1);
        my @stat2 = stat($file2);
        if ( !@stat2 )
        {
            return 1;
        }
        return ( $stat1[9] >= $stat2[9] );
    }

    my @queue;
    foreach my $lfn (@dests)
    {
        my $dest     = "$T2_DEST/$lfn";
        my $abs_dest = "$PWD/$dest";
        my $src      = "$lfn.wml";
        if ( $UNCOND or is_newer( $src, $abs_dest ) )
        {
            push @queue, [ [ $abs_dest, "-DLATEMP_FILENAME=$lfn", $src, ], $dest ];
        }
    }
    my $to_proc = [ map $_->[1], @queue ];
    my @FLAGS = ( @WML_FLAGS, '-o', );
    my $proc = sub {
        $obj->run_with_ARGV(
            {
                ARGV => [ @FLAGS, @{ shift(@_)->[0] } ],
            }
        ) and die "$!";
        return;
    };
    Parallel::ForkManager::Segmented->new->run(
        {
            WITH_PM      => 1,
            items        => \@queue,
            nproc        => 4,
            batch_size   => 8,
            process_item => $proc,
        }
    );
    system("cd $PWD && $CMD @{$to_proc}") and die "$!";

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
