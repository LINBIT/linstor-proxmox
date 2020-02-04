package LINBIT::PluginHelper;

use strict;
use warnings;

# use Data::Dumper;

sub get_images {
    my ( $storeid, $vmid, $vollist, $resources, $node_name, $storage_pool ) =
      @_;

    my $res = [];
    foreach my $name ( keys %$resources ) {
        # skip if not on this node
        next unless exists $resources->{$name}->{$node_name};

        next unless $name =~ /^vm-(\d+)-/;
        my $owner = $1;                 # aka "vmid"
        my $volid = "$storeid:$name";

        # filter, if we have been passed vmid or vollist
        next if defined $vmid and $vmid ne $owner;
        next
          if defined $vollist
          and 0 == ( scalar grep { $_ eq $volid } @$vollist );

        # expect exactly one volume
        # XXX warn for 0 or >= 2 volume resources?
        next
          unless exists $resources->{$name}->{$node_name}->{nr_vols}
          and $resources->{$name}->{$node_name}->{nr_vols} == 1;

        my $size_kib = $resources->{$name}->{$node_name}->{usable_size_kib};
        my $pool     = $resources->{$name}->{$node_name}->{storage_pool_name};

        # filter by storage_pool property, if set
        next if $storage_pool and $storage_pool ne $pool;

        push @$res,
          {
            format => 'raw',
            volid  => $volid,
            size   => $size_kib * 1024,
            vmid   => $owner,
          };
    }

    return $res;
}

sub get_status {
    my ( $storage_pools, $storage_pool, $node_name ) = @_;
    #     overall      , fitlter      , node_filter

    my ( $avail_kib, $total_kib );
    foreach my $name ( keys %$storage_pools ) {
        next if $storage_pool and $storage_pool ne $name;

        # skip if not on this node
        next unless exists $storage_pools->{$name}->{$node_name};

        # skip diskless pools. LINSTOR considers them having infinite space, so they have some MAX values.
        next if $storage_pools->{$name}->{$node_name}->{conf_as_diskless};

        $avail_kib += $storage_pools->{$name}->{$node_name}->{free_capacity_kib};
        $total_kib += $storage_pools->{$name}->{$node_name}->{total_capacity_kib};
    }

    return ( $total_kib, $avail_kib );
}

1;
