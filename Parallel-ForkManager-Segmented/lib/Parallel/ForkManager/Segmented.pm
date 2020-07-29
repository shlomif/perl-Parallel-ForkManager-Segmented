package Parallel::ForkManager::Segmented;

use strict;
use warnings;
use 5.014;

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

    my $pm;

    if ($WITH_PM)
    {
        $pm = Parallel::ForkManager->new($nproc);
    }
    my $batch = $stream_cb->( { size => 1 } )->{items};
    return if not defined $batch;
    $batch_cb->($batch);
ITEMS:
    while (
        defined( $batch = $stream_cb->( { size => $batch_size } )->{items} ) )
    {
        if ($WITH_PM)
        {
            my $pid = $pm->start;

            if ($pid)
            {
                next ITEMS;
            }
        }
        $batch_cb->($batch);
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

Parallel::ForkManager::Segmented - use Parallel::ForkManager on batches /
segments of items.

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
    --passoption=2,-X3074 --passoption=2,-I../lib/ --passoption=3,-I../lib/
    --passoption=3,-w -I../lib/ $ENV{LATEMP_WML_FLAGS} -p1-3,5,7 -DROOT~.
    -DLATEMP_THEME=sf.org1 -I $HOME/apps/wml
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
            push @queue,
            [
                [ $abs_dest, "-DLATEMP_FILENAME=$lfn", $src, ],
                $dest,
            ];
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

=head1 DESCRIPTION

This module builds upon L<Parallel::ForkManager> allowing one to pass a
batch (or "segment") of several items for processing inside a worker. This
is done in order to hopefully reduce the forking/exiting overhead.

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

=cut
