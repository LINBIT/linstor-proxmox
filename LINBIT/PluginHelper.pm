package LINBIT::PluginHelper;

use strict;
use warnings;

# use Data::Dumper;

sub get_images {
    my ( $storeid, $vmid, $vollist, $resources, $node_name, $rg,
        $resource_definitions )
      = @_;

    my $res = [];
    foreach my $name ( keys %$resources ) {

        # skip if not on this node
        next unless exists $resources->{$name}->{$node_name};

        # skip if not from this RG
        next unless $rg eq $resource_definitions->{$name}->{rg_name};

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
