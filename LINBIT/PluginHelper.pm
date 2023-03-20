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
        next if $storage_pool and 0 == ( scalar grep { $_ eq $pool } @$storage_pool );

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

1;
