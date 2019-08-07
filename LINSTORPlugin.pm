package PVE::Storage::Custom::LINSTORPlugin;

use strict;
use warnings;
use Carp qw( confess );
use IO::File;
use JSON::XS qw( decode_json );
use Data::Dumper;

use LINBIT::Linstor;
use LINBIT::PluginHelper;

use PVE::Tools qw(run_command trim);
use PVE::INotify;
use PVE::Storage;
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# Configuration

my $default_redundancy = 2;
my $default_controller = "localhost";
my $default_controller_vm = "";
# my $default_storagepool = "DfltStorPool";
my $default_storagepool = "drbdpool";

sub api {
   # PVE 5: APIVER 2
   # PVE 6: APIVER 3
   # we support both, we just have to be careful what we return
   # as for example PVE5 would not like a APIVER 3

   my $apiver = PVE::Storage::APIVER;

   if ($apiver >= 2 and $apiver <= 3) {
      return $apiver;
   }

   return 3;
}

# we have to name it drbd, there is a hardcoded 'drbd' in Plugin.pm
sub type {
    return 'drbd';
}

sub plugindata {
    return { content => [ { images => 1, rootdir => 1 }, { images => 1 } ], };
}

sub properties {
    return {
        redundancy => {
            description =>
"The redundancy count specifies the number of nodes to which the resource should be deployed. It must be at least 1 and at most the number of nodes in the cluster.",
            type    => 'integer',
            minimum => 1,
            maximum => 16,
            default => $default_redundancy,
        },
        controller => {
            description => "The IP of the active controller",
            type        => 'string',
            default     => $default_controller,
        },
        controllervm => {
            description => "The VM number (e.g., 101) of the LINSTOR controller. Set this if the controller is run in a VM itself",
            type        => 'string',
            default     => $default_controller_vm,
        },
        storagepool => {
             description => "The name of the LINSTOR storage pool to be used. Leave off if you want to use LINSTOR defaults.",
             type        => 'string',
             default     => $default_storagepool,
        },
    };
}

sub options {
    return {
        redundancy   => { optional => 1 },
        storagepool  => { optional => 1 },
        controller   => { optional => 1 },
        controllervm => { optional => 1 },
        content      => { optional => 1 },
        nodes        => { optional => 1 },
        disable      => { optional => 1 },
    };
}

# helpers

sub get_redundancy {
    my ($scfg) = @_;

    return $scfg->{redundancy} || $default_redundancy;
}

sub get_storagepool {
    my ($scfg) = @_;

    return $scfg->{storagepool} || $default_storagepool;
}

sub get_controller {
    my ($scfg) = @_;

    return $scfg->{controller} || $default_controller;
}

sub get_controller_vm {
    my ($scfg) = @_;

    return $scfg->{controllervm} || $default_controller_vm;
}

sub ignore_volume {
    my ($scfg, $volume) = @_;
	 my $controller_vm = get_controller_vm($scfg);

    # keep the '-', if controller_vm is not set, we want vm--
    return 1 if $volume =~ m/^vm-\Q$controller_vm\E-/;

    return undef;
}

sub volname_and_snap_to_snapname {
    my ( $volname, $snap ) = @_;
    return "snap_${volname}_${snap}";
}

# TODO: LINSTOR is synchronous enough, remove that soon.
sub wait_connect_resource {
    my ($resource) = @_;

    eval {
        run_command(
            [ 'drbdsetup', 'wait-connect-resource', $resource ],
            errmsg => "Could not wait until replication established for ($resource)",
            timeout => 30 # could use --wfc-timeout, but hey when we already do it proxmoxy...
        );
    }; if ($@) {
       warn $@;
       open(my $fh, '-|', 'drbdadm', 'dstate', $resource) or die $!;
       while (my $line = <$fh>) {
           die "wait-connect-resource failed AND none UpToDate" if ($line !~ m/UpToDate/);
       }
    }
}

sub get_dev_path {
    return "/dev/drbd/by-res/$_[0]/0";
}

sub linstor {
    my ($scfg) = @_;

    my $controller = get_controller($scfg);
    my $cli = REST::Client->new( { host => "http://$controller:3370" } );
    return LINBIT::Linstor->new( { cli => $cli } );
}

# Storage implementation
#
# For APIVER 2
sub map_volume {
    my ( $class, $storeid, $scfg, $volname, $snapname ) = @_;

    die "drbd snapshot is not implemented\n" if defined($snapname);

    return get_dev_path "$volname";
}

# For APIVER 2
sub unmap_volume {
    return 1;
}

sub parse_volname {
    my ( $class, $volname ) = @_;

    if ( $volname =~ m/^(vm-(\d+)-[a-z][a-z0-9\-\_\.]*[a-z0-9]+)$/ ) {
        return ( 'images', $1, $2, undef, undef, undef, 'raw' );
    }

    die "unable to parse lvm volume name '$volname'\n";
}

sub filesystem_path {
    my ( $class, $scfg, $volname, $snapname ) = @_;

    die "drbd snapshot is not implemented\n" if defined($snapname);

    my ( $vtype, $name, $vmid ) = $class->parse_volname($volname);

    my $path = get_dev_path "$volname";

    return wantarray ? ( $path, $vmid, $vtype ) : $path;
}

sub create_base {
    my ( $class, $storeid, $scfg, $volname ) = @_;

    die "can't create base images in drbd storage\n";
}

sub clone_image {
    my ( $class, $scfg, $storeid, $volname, $vmid, $snap ) = @_;

    die "can't clone images in drbd storage\n";
}

sub alloc_image {
    my ( $class, $storeid, $scfg, $vmid, $fmt, $name, $size ) = @_;

    # check if it is the controller, which always has exactly "disk-1"
    my $retname = $name;
    if ( !defined($name) ) {
        $retname = "vm-$vmid-disk-1";
    }
    return $retname if ignore_volume( $scfg, $retname );

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    die "illegal name '$name' - should be 'vm-$vmid-*'\n"
      if defined($name) && $name !~ m/^vm-$vmid-/;

    my $lsc       = linstor($scfg);
    my $resources = $lsc->get_resources();

    die "volume '$name' already exists\n"
      if defined($name) && exists $resources->{$name};

    if ( !defined($name) ) {
        for ( my $i = 1 ; $i < 100 ; $i++ ) {
            my $tn = "vm-$vmid-disk-$i";
            if ( !exists( $resources->{$tn} ) ) {
                $name = $tn;
                last;
            }
        }
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
      if !defined($name);

    eval {
        $lsc->create_resource( $name, $size, get_storagepool($scfg),
            get_redundancy($scfg) );
    };
    confess $@ if $@;

    return $name;
}

sub free_image {
    my ( $class, $storeid, $scfg, $volname, $isBase ) = @_;

   # die() does not really help in that case, the VM definition is still removed
   # so we could just return undef, still this looks a bit cleaner
    die "Not freeing contoller VM" if ignore_volume( $scfg, $volname );

    my $lsc = linstor($scfg);
    my $in_use = 1;

    foreach (0..9) {
      my $resources = $lsc->update_resources();
      $in_use = $resources->{$volname}->{in_use};
      last if (! $in_use);
      sleep(1);
    }

    warn "Resource $volname still in use after giving it some time" if ($in_use);

    # yolo, what else should we do...
    eval { $lsc->delete_resource($volname); };
    confess $@ if $@;

    return undef;
}

sub list_images {
    my ( $class, $storeid, $scfg, $vmid, $vollist, $cache ) = @_;

    my $nodename = PVE::INotify::nodename();

    $cache->{"linstor:resources"} = linstor($scfg)->get_resources()
      unless $cache->{"linstor:resources"};

    # TODO:
    # Currently we have/expect one resource per volume per proxmox disk image,
    # we do not (yet) use or expect multi-volume resources, even thought it may
    # be useful to have all vm images in one "consistency group".

    my $resources = $cache->{"linstor:resources"};

    return LINBIT::PluginHelper::get_images( $storeid, $vmid, $vollist,
        $resources, $nodename, get_storagepool($scfg) );
}

sub status {
    my ( $class, $storeid, $scfg, $cache ) = @_;
    my $nodename = PVE::INotify::nodename();

    $cache->{"linstor:storagepools"} = linstor($scfg)->get_storagepools()
      unless $cache->{"linstor:storagepools"};
    my $storagepools = $cache->{"linstor:storagepools"};

    my ( $total, $avail ) =
      LINBIT::PluginHelper::get_status( $storagepools, get_storagepool($scfg),
        $nodename );
    return undef unless $total;

    # they want it in bytes
    $total *= 1024;
    $avail *= 1024;
    return ( $total, $avail, $total - $avail, 1 );
}

sub activate_storage {
    my ( $class, $storeid, $scfg, $cache ) = @_;

    return undef;
}

sub deactivate_storage {
    my ( $class, $storeid, $scfg, $cache ) = @_;

    return undef;
}

sub activate_volume {
    my ( $class, $storeid, $scfg, $volname, $snapname, $cache ) = @_;

    die "Snapshot not implemented on DRBD\n" if $snapname;

    return undef if ignore_volume( $scfg, $volname );

    my $nodename = PVE::INotify::nodename();

    eval { linstor($scfg)->activate_resource( $volname, $nodename ); };
    confess $@ if $@;

    wait_connect_resource($volname);

    system ('blockdev --setrw ' . get_dev_path $volname);

    return undef;
}

sub deactivate_volume {
    my ( $class, $storeid, $scfg, $volname, $snapname, $cache ) = @_;

    die "Snapshot not implemented on DRBD\n" if $snapname;

    return undef if ignore_volume( $scfg, $volname );

    my $nodename = PVE::INotify::nodename();

# deactivate_resource only removes the assignment if diskless, so this could be a single call.
# We do all this unnecessary dance to print the NOTICE.
    my $lsc = linstor($scfg);
    my $was_diskless_client =
      $lsc->resource_exists_intentionally_diskless( $volname, $nodename );

    if ($was_diskless_client) {
        print "\nNOTICE\n"
          . "  Intentionally removing diskless assignment ($volname) on ($nodename).\n"
          . "  It will be re-created when the resource is actually used on this node.\n";

        eval { $lsc->deactivate_resource( $volname, $nodename ); };
        confess $@ if $@;
    }

    return undef;
}

sub volume_resize {
    my ( $class, $scfg, $storeid, $volname, $size, $running ) = @_;

    my $size_kib = ( $size / 1024 );

    eval { linstor($scfg)->resize_resource( $volname, $size_kib ); };
    confess $@ if $@;

    # TODO: remove, temporary fix for non-synchronous LINSTOR resize
    sleep(10);

    return 1;
}

sub volume_snapshot {
    my ( $class, $scfg, $storeid, $volname, $snap ) = @_;
    my $snapname = volname_and_snap_to_snapname( $volname, $snap );

    eval { linstor($scfg)->create_snapshot( $volname, $snapname ); };
    confess $@ if $@;

    return 1;
}

sub volume_snapshot_rollback {
    my ( $class, $scfg, $storeid, $volname, $snap ) = @_;

    die "DRBD snapshot rollback is not implemented, please use 'linstor' to recover your data, use 'qm unlock' to unlock your VM";
}

sub volume_snapshot_delete {
    my ( $class, $scfg, $storeid, $volname, $snap ) = @_;
    my $snapname = volname_and_snap_to_snapname( $volname, $snap );

    eval { linstor($scfg)->delete_snapshot( $volname, $snapname ); };
    confess $@ if $@;

    return 1;
}

sub volume_has_feature {
    my ( $class, $scfg, $feature, $storeid, $volname, $snapname, $running ) =
      @_;

    my $features = {
        copy     => { base    => 1, current => 1 },
        snapshot => { current => 1 },
    };

    my ( $vtype, $name, $vmid, $basename, $basevmid, $isBase ) =
      $class->parse_volname($volname);

    my $key = undef;
    if ($snapname) {
        $key = 'snap';
    }
    else {
        $key = $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};

    return undef;
}

1;
# vim: set et sw=4 :
