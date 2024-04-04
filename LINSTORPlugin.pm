package PVE::Storage::Custom::LINSTORPlugin;

use strict;
use warnings;
use Carp qw( confess );
use IO::File;
use JSON::XS qw( decode_json );
use Data::Dumper;
use REST::Client;
use Storable qw(lock_store lock_retrieve);
use UUID;

use LINBIT::Linstor;
use LINBIT::PluginHelper
  qw(valid_legacy_name valid_uuid_name valid_snap_name valid_name get_images);

use PVE::Tools qw(run_command trim);
use PVE::INotify;
use PVE::Storage;
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

my $PLUGIN_VERSION = '8.0.0-rc.1';

# Configuration

my $default_controller = "localhost";
my $default_resourcegroup = "drbdgrp";
my $default_prefer_local_storage = "yes";
my $default_exact_size = "no";
my $default_status_cache = 60;
my $default_apicrt = undef;
my $default_apikey = undef;
my $default_apica = undef;

sub api {
   # PVE 5:   APIVER  2
   # PVE 6:   APIVER  3
   # PVE 6:   APIVER  4 e6f4eed43581de9b9706cc2263c9631ea2abfc1a / volume_has_feature
   # PVE 6:   APIVER  5 a97d3ee49f21a61d3df10d196140c95dde45ec27 / allow rename
   # PVE 6:   APIVER  6 8f26b3910d7e5149bfa495c3df9c44242af989d5 / prune_backups (fine, we don't support that content type)
   # PVE 6:   APIVER  7 2c036838ed1747dabee1d2c79621c7d398d24c50 / volume_snapshot_needs_fsfreeze (guess we are fine, upstream only implemented it for RDBPlugin; we are not that different to let's say LVM in this regard)
   # PVE 6:   APIVER  8 343ca2570c3972f0fa1086b020bc9ab731f27b11 / prune_backups (fine again, see APIVER 6)
   # PVE 7:   APIVER  9 3cc29a0487b5c11592bf8b16e96134b5cb613237 / resets APIAGE! changes volume_import/volume_import_formats
   # PVE 7.1: APIVER 10 a799f7529b9c4430fee13e5b939fe3723b650766 / rm/add volume_snapshot_{list,info} (not used); blockers to volume_rollback_is_possible (not used)
   #
   # we support all (not all features), we just have to be careful what we return
   # as for example PVE5 would not like a APIVER 3

   my $tested_apiver = 10;

   my $apiver = PVE::Storage::APIVER;
   my $apiage = PVE::Storage::APIAGE;

   # the plugin supports multiple PVE generations, currently we did not break anything, tell them what they want to hear if possible
   if ($apiver >= 2 and $apiver <= $tested_apiver) {
      return $apiver;
   }

   # if we are still in the APIAGE, we can still report what we have
   if ($apiver - $apiage < $tested_apiver) {
      return $tested_apiver;
   }

   # fallback that worked a very very long time ago, nowadays useless, as the core does APIVER - APIAGE checking
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
        controller => {
            description => "The IP of the LINSTOR controller (',' separated list allowed)",
            type        => 'string',
            default     => $default_controller,
        },
        resourcegroup => {
             description => "The name of a LINSTOR resource group which defines the deployment of new VMs.",
             type        => 'string',
             default     => $default_resourcegroup,
        },
        preferlocal => {
             description => "Prefer to create local storage (yes/no)",
             type        => 'string',
             default     => $default_prefer_local_storage,
        },
        exactsize => {
             description => "Set size in DRBD res files. This allows online moving from another storage (e.g., LVM), but should only be set temporarily (yes/no)",
             type        => 'string',
             default     => $default_exact_size,
        },
        statuscache => {
             description => "Time in seconds status information is cached, 0 means no extra cache",
             type        => 'integer',
             minimum     => 0,
             maximum     => 2*60*60,
             default     => $default_status_cache,
        },
        apicrt => {
             description => "Path to the client certificate.",
             type        => 'string',
             default     => $default_apicrt,
        },
        apikey => {
             description => "Path to the client private key",
             type        => 'string',
             default     => $default_apikey,
        },
        apica => {
             description => "Path to the CA certificate",
             type        => 'string',
             default     => $default_apica,
        },

    };
}

sub options {
    return {
        controller    => { optional => 1 },
        resourcegroup => { optional => 0 },
        preferlocal   => { optional => 1 },
        exactsize     => { optional => 1 },
        statuscache   => { optional => 1 },
        content       => { optional => 1 },
        disable       => { optional => 1 },
        nodes         => { optional => 1 },
        apicrt        => { optional => 1 },
        apikey        => { optional => 1 },
        apica         => { optional => 1 },
    };
}

# helpers

sub cache_needs_update {
    my ($cache_file, $max_cache_age) = @_;
    my $mtime = (stat($cache_file))[9] || 0;

    return time - $mtime >= $max_cache_age
}

sub get_status_cache {
    my ($scfg) = @_;

    return $scfg->{statuscache} || $default_status_cache;
}

sub get_resource_group {
    my ($scfg) = @_;

    return $scfg->{resourcegroup} || $default_resourcegroup;
}

sub get_controllers {
    my ($scfg) = @_;

    return $scfg->{controller} || $default_controller;
}

sub get_preferred_local_node {
    my ($scfg) = @_;

    my $pref = $scfg->{preferlocal} || $default_prefer_local_storage;

    if ( lc $pref eq 'yes' ) {
        return PVE::INotify::nodename();
    }

    return undef;
}

sub get_exact_size {
    my ($scfg) = @_;

    my $pref = $scfg->{exactsize} || $default_exact_size;

    return lc $pref eq 'yes';
}

sub get_apicrt {
  my ($cfg) = @_;

  return $cfg->{apicrt} || $default_apicrt;
}

sub get_apikey {
  my ($cfg) = @_;

  return $cfg->{apikey} || $default_apikey;
}

sub get_apica {
  my ($cfg) = @_;

  return $cfg->{apica} || $default_apica;
}

sub linstor {
    my ($scfg) = @_;

    my @controllers = split( /,/, get_controllers($scfg) );

    my $apicrt = get_apicrt($scfg);
    my $apikey = get_apikey($scfg);
    my $apica = get_apica($scfg);

    my $proto = "http";
    my $port = "3370";

    # If cert an key are configured, change protocol and port
    if ( defined $apicrt and defined $apikey ) {
      $proto = "https";
      $port = "3371";
    }

    foreach my $controller (@controllers) {
        $controller = trim($controller);
        my $cli = REST::Client->new( {
          host => "${proto}://${controller}:${port}",
          cert => $apicrt,
          key => $apikey,
          ca => $apica,
        } );

        $cli->addHeader('User-Agent', 'linstor-proxmox/' . $PLUGIN_VERSION);
        return LINBIT::Linstor->new( { cli => $cli } )
          if $cli->GET('/health')->responseCode() eq '200';
    }

    die("could not connect to any LINSTOR controller");
}

sub volname_and_snap_to_snapname {
    my ( $volname, $snap ) = @_;
    return "snap_${volname}_${snap}";
}


sub uuid_strip_vmid {
    my ($volname) = @_;

    die "Not a valid uuid volume name ('$volname')"
      if !valid_uuid_name($volname);

    return substr( $volname, 0, 3 + 8 );
}

sub pm_name_to_linstor_name {
    my ($volname) = @_;

    # here we expect contexts that only have lecacy/uuid names, but not snapshot context
    die "Not a valid volume name ('$volname')" if !valid_name($volname);

    if ( valid_uuid_name($volname) ) {
        return uuid_strip_vmid($volname);
    }
    elsif ( valid_legacy_name($volname) ) {
        return $volname;
    }
    else {
        die "pm_name_to_linstor_name: '$volname' not valid";
    }
}

sub get_dev_path {
    my ($volname) = @_;

    # we have to be a bit careful here, this one is called from contexts where the volname can be a snapname
    die "Not a valid volume name ('$volname')"
      if !valid_name($volname)
      and !valid_snap_name($volname);

    # snapshots and legacy names already have their final name
    $volname = uuid_strip_vmid($volname) if valid_uuid_name($volname);

    return "/dev/drbd/by-res/$volname/0";
}


# Storage implementation
#
# For APIVER 2
sub map_volume {
    my ( $class, $storeid, $scfg, $volname, $snap ) = @_;

    my $linstor_name = pm_name_to_linstor_name($volname);
    $volname = volname_and_snap_to_snapname( $linstor_name, $snap )
      if defined($snap);

    return get_dev_path($volname);
}

# For APIVER 2
sub unmap_volume {
    return 1;
}

sub parse_volname {
    my ( $class, $volname ) = @_;

    if ( $volname =~ m/^(vm-(\d+)-[a-z][a-z0-9\-\_\.]*[a-z0-9]+)$/ ) {
        return ( 'images', $1, $2, undef, undef, undef, 'raw' );
    } elsif ( $volname =~ m/^(pm-[\da-f]{8}_(\d+))$/ ) {
        return ( 'images', $1, $2, undef, undef, undef, 'raw' );
    }

    die "unable to parse PVE volume name '$volname'\n";
}

sub filesystem_path {
    my ( $class, $scfg, $volname, $snapname ) = @_;

    die "filesystem_path: snapshot is not implemented ($snapname)\n" if defined($snapname);

    my ( $vtype, $name, $vmid ) = $class->parse_volname($volname);

    my $path = get_dev_path($volname);

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

    my $min_kib = 5*1024;
    $size = $min_kib unless $size > $min_kib;

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    my $linstor_name;
    my $proxmox_name;

    my $lsc       = linstor($scfg);
    my $resources = $lsc->get_resources();

    # pvesm defines that '' is used to flag that a name should be generated
    # so we don't use 'defined()', but a plain 'if':
    if ($name) {
        if (valid_uuid_name($name)) {
            $proxmox_name = $name;
            $linstor_name = uuid_strip_vmid($proxmox_name);
        } elsif (valid_legacy_name($name)) {
            $proxmox_name = $name;
            $linstor_name = $proxmox_name;
        }
        else {
            die "allocated name ('$name') has to be a valid UUID or legacy name";
        }

        die "volume '$linstor_name' already exists\n"
          if exists $resources->{$linstor_name};
    }
    else {
        for ( my $i = 1 ; $i < 100 ; $i++ ) {
            my $tn = "pm-" . lc substr( UUID::uuid(), 0, 8 );
            if ( !exists( $resources->{$tn} ) ) {
                $linstor_name = $tn;
                $proxmox_name = "${tn}_${vmid}";
                last;
            }
        }
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
      if !defined($proxmox_name) or !defined($linstor_name);

    eval {
        my $res_grp         = get_resource_group($scfg);
        my $local_node_name = get_preferred_local_node($scfg);
        my $exact_size      = get_exact_size($scfg);
        if ( defined($local_node_name) ) {
            print "\nNOTICE\n"
              . "  Trying to create diskful resource ($linstor_name) on ($local_node_name).\n";
        }
        $lsc->create_resource( $linstor_name, $size, $res_grp, $local_node_name, $exact_size );
        $lsc->set_vmid( $linstor_name, $vmid ); # does not hurt, even vor legacy names
    };
    confess $@ if $@;

    return $proxmox_name;
}

sub free_image {
    my ( $class, $storeid, $scfg, $volname, $isBase ) = @_;

    my $lsc = linstor($scfg);
    my $in_use = 1;

    my $linstor_name = pm_name_to_linstor_name($volname);

    foreach (0..9) {
      my $resources = $lsc->update_resources();
      $in_use = $resources->{$linstor_name}->{in_use};
      last if (! $in_use);
      sleep(1);
    }

    warn "Resource $linstor_name still in use after giving it some time" if ($in_use);

    # yolo, what else should we do...
    eval { $lsc->delete_resource($linstor_name); };
    confess $@ if $@;

    return undef;
}

sub list_images {
    my ( $class, $storeid, $scfg, $vmid, $vollist, $cache ) = @_;

    my $nodename = PVE::INotify::nodename();

    # unlike 'status' this is not called in loops like crazy, we can get a current view via update_...():
    $cache->{"linstor:resources"} = linstor($scfg)->update_resources()
      unless $cache->{"linstor:resources"};
    $cache->{"linstor:resource_definitions"} =
      linstor($scfg)->update_resource_definitions()
      unless $cache->{"linstor:resource_definitions"};

    # TODO:
    # Currently we have/expect one resource per volume per proxmox disk image,
    # we do not (yet) use or expect multi-volume resources, even thought it may
    # be useful to have all vm images in one "consistency group".
    # On the other hand this would make moving disks hard(er).

    my $resources            = $cache->{"linstor:resources"};
    my $resource_definitions = $cache->{"linstor:resource_definitions"};
    my $res_grp              = get_resource_group($scfg);

    return get_images( $storeid, $vmid, $vollist,
        $resources, $nodename, $res_grp, $resource_definitions );
}

sub status {
    my ( $class, $storeid, $scfg, $cache ) = @_;
    my $nodename = PVE::INotify::nodename();
    my $res_grp = get_resource_group($scfg);

    my $cache_key = 'linstor:sizeinfos';
    my $info_cache = '/var/cache/linstor-proxmox/sizeinfos';
    unless($cache->{$cache_key}) {
        my $max_age = get_status_cache($scfg);

        if ($max_age and not cache_needs_update($info_cache, $max_age)) {
            $cache->{$cache_key} = lock_retrieve($info_cache);
        } else {
            # plugin uses 0 for disabled cache, LINSTOR -1, but no actual difference, so...
            my $infos = linstor($scfg)->query_all_size_info($max_age);
            $cache->{$cache_key} = $infos;
            lock_store($infos, $info_cache) if $max_age;
        }
    }

    my $total = $cache->{$cache_key}->{$res_grp}->{space_info}->{capacity_in_kib};
    my $avail = $cache->{$cache_key}->{$res_grp}->{space_info}->{available_size_in_kib};
    return undef unless defined($total); # key/RG does not even exist, mark undef == "inactive"
    if ($total == 0) { # might have been called very early on system boot/LINSTOR startup, invalidate caches but continue
        my $infos = linstor($scfg)->query_all_size_info(-1);
        $cache->{$cache_key} = $infos;
        lock_store($infos, $info_cache);
    }

    # they want it in bytes
    $total *= 1024;
    $avail *= 1024;
    my $used = $total - $avail;
    $used = 0 if $used < 0; # there was a linstor bug in calculating thin storage, at least don't generate negative value
    return ( $total, $avail, $used, 1 );
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
    my ( $class, $storeid, $scfg, $volname, $snap, $cache ) = @_;

    my $linstor_name = pm_name_to_linstor_name($volname);
    my $lsc = linstor($scfg);

    if ($snap) {    # need to create this resource from snapshot
        my $snapname = volname_and_snap_to_snapname( $linstor_name, $snap );
        my $new_volname = $snapname;
        if ( !$lsc->resource_exists($new_volname) ) {
            eval {
                $lsc->restore_snapshot( $linstor_name, $snapname, $new_volname );
            };
            confess $@ if $@;
        }
        $linstor_name = $new_volname; # for the rest of this function switch the name
    }

    # try to unset exact size if no longer present in scfg
    my $exact_size = get_exact_size($scfg);
    if ( !$exact_size ) {
        eval {
            $lsc->set_rd_prop( $linstor_name, 'DrbdOptions/ExactSize',
                LINBIT::Linstor::bool2linstor($exact_size) );
        };
    }

    my $nodename = PVE::INotify::nodename();

    eval { $lsc->activate_resource( $linstor_name, $nodename ); };
    confess $@ if $@;

    system ('blockdev --setrw ' . get_dev_path($volname));

    return undef;
}

sub deactivate_volume {
    my ( $class, $storeid, $scfg, $volname, $snapname, $cache ) = @_;

    die "deactivate_volume: snapshot not implemented ($snapname)\n" if $snapname;

    my $nodename     = PVE::INotify::nodename();
    my $linstor_name = pm_name_to_linstor_name($volname);

# deactivate_resource only removes the assignment if diskless, so this could be a single call.
# We do all this unnecessary dance to print the NOTICE.
    my $lsc = linstor($scfg);
    my $was_diskless_client =
      $lsc->resource_exists_intentionally_diskless( $linstor_name, $nodename );

    if ($was_diskless_client) {
        print "\nNOTICE\n"
          . "  Intentionally removing diskless assignment ($linstor_name) on ($nodename).\n"
          . "  It will be re-created when the resource is actually used on this node.\n";

        eval { $lsc->deactivate_resource( $linstor_name, $nodename ); };
        confess $@ if $@;
    }

    return undef;
}

sub volume_resize {
    my ( $class, $scfg, $storeid, $volname, $size, $running ) = @_;

    my $size_kib     = ( $size / 1024 );
    my $linstor_name = pm_name_to_linstor_name($volname);

    eval { linstor($scfg)->resize_resource( $linstor_name, $size_kib ); };
    confess $@ if $@;

    return 1;
}

sub volume_snapshot {
    my ( $class, $scfg, $storeid, $volname, $snap ) = @_;

    my $linstor_name = pm_name_to_linstor_name($volname);
    my $snapname     = volname_and_snap_to_snapname( $linstor_name, $snap );

    eval { linstor($scfg)->create_snapshot( $linstor_name, $snapname ); };
    confess $@ if $@;

    return 1;
}

sub volume_snapshot_rollback {
    my ( $class, $scfg, $storeid, $volname, $snap ) = @_;

    my $linstor_name = pm_name_to_linstor_name($volname);
    my $snapname     = volname_and_snap_to_snapname( $linstor_name, $snap );

    eval { linstor($scfg)->rollback_snapshot( $linstor_name, $snapname ); };
    confess $@ if $@;

    return 1;
}

sub volume_snapshot_delete {
    my ( $class, $scfg, $storeid, $volname, $snap ) = @_;

    my $linstor_name = pm_name_to_linstor_name($volname);
    my $snapname     = volname_and_snap_to_snapname( $linstor_name, $snap );

    my $lsc = linstor($scfg);

    # on backup we created a resource from the given snapshot
    # on cleanup we as plugin only get a volume_snapshot_delete
    # so we have to do some "heuristic" to also clean up the resource we created
    # backup would be: if ( $snap eq 'vzdump' )
    # but we also want to delete "tempoarary snapshot resources" when they got activated via a clone
    # this is a nop if the resource does not exist
    eval { $lsc->delete_resource( $snapname ); };
    confess $@ if $@;

    eval { $lsc->delete_snapshot( $linstor_name, $snapname ); };
    confess $@ if $@;

    return 1;
}

sub rename_volume {
    my ( $class, $scfg, $storeid, $source_volname, $target_vmid,
        $target_volname )
      = @_;

    die "rename_volume only possible for uuid disk names"
      if !valid_uuid_name($source_volname);

    die "rename_volume only possible if target name is unset"
      if defined($target_volname);

    # we keep the UUID name, we just switch to a new VM
    my $linstor_name = pm_name_to_linstor_name($source_volname);
    $target_volname = $linstor_name . "_" . $target_vmid;

    eval { linstor($scfg)->set_vmid( $linstor_name, $target_vmid ); };
    confess $@ if $@;

    return "${storeid}:${target_volname}";
}

sub volume_has_feature {
    my ( $class, $scfg, $feature, $storeid, $volname, $snapname, $running ) =
      @_;

    my $features = {
        copy     => { base    => 1, current => 1 },
        snapshot => { current => 1 },
    };

    # pretend we can rename stuff, but fail for legacy names.
    $features->{rename} = {current => 1};

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
