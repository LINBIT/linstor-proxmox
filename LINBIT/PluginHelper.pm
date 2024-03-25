package LINBIT::PluginHelper;

use strict;
use warnings;
use Exporter 'import';
our @EXPORT_OK = qw(valid_legacy_name valid_uuid_name valid_snap_name valid_name get_images);

# use Data::Dumper;

sub valid_legacy_name {
    $_[0] =~ /^vm-\d+-disk-\d+\z/
}

sub valid_uuid_name {
    $_[0] =~ /^pm-[\da-f]{8}_\d+\z/
}

sub valid_snap_name {
    $_[0] =~ /^snap_.+_.+\z/
}

sub valid_name {
    valid_legacy_name $_[0] or valid_uuid_name $_[0]
}

sub get_images {
    my ( $storeid, $vmid, $vollist, $resources, $node_name, $rg,
        $resource_definitions )
      = @_;

    my $res = [];
    foreach my $linstor_name ( keys %$resources ) {

        # skip if not on this node
        next unless exists $resources->{$linstor_name}->{$node_name};

        # skip if not from this RG
        next unless $rg eq $resource_definitions->{$linstor_name}->{rg_name};

        my $owner;
        my $proxmox_name;
        if ($linstor_name =~ /^pm-[\da-f]{8}\z/) {
            $owner = $resource_definitions->{$linstor_name}->{vmid};
            $proxmox_name = $linstor_name . "_" . $owner;
            next unless valid_uuid_name($proxmox_name);
        }
        elsif ($linstor_name =~ /^vm-(\d+)-/) {
            $owner = $1;    # aka "vmid"
            $proxmox_name = $linstor_name;
        } else {
            next;
        }

        my $volid = "$storeid:$proxmox_name";

        # filter, if we have been passed vmid or vollist
        next if defined $vmid and $vmid ne $owner;
        next
          if defined $vollist
          and 0 == ( scalar grep { $_ eq $volid } @$vollist );

        # expect exactly one volume
        # XXX warn for 0 or >= 2 volume resources?
        next
          unless exists $resources->{$linstor_name}->{$node_name}->{nr_vols}
          and $resources->{$linstor_name}->{$node_name}->{nr_vols} == 1;

        my $size_kib = $resources->{$linstor_name}->{$node_name}->{usable_size_kib};

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
