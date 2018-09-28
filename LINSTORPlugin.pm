package PVE::Storage::Custom::LINSTORPlugin;

use strict;
use warnings;
use IO::File;
use JSON::XS qw( decode_json );
use Data::Dumper;

use PVE::Tools qw(run_command trim);
use PVE::INotify;
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# Configuration

my $default_redundancy = 2;
my $default_controller = "localhost";
my $default_controller_vm = "";
my $APIVER = 1;

my $LINSTOR = '/usr/bin/linstor';

sub api {
    return $APIVER;
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
    };
## Please see file perltidy.ERR
}

sub options {
    return {
        redundancy   => { optional => 1 },
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

sub decode_json_from_pipe {
	my $kid = open(my $pipe, '-|');
	die "fork failed:  $!" unless defined $kid;
	if ($kid == 0) {
		exec { $_[0] } @_;
		exit 1;
	}
	local $/ = undef; # slurp mode
	my $output = readline $pipe;
	return decode_json($output);
}

sub drbd_list_volumes {
    my ($scfg) = @_;
    my $controller = get_controller($scfg);

    my $volumes = {};

    my $json = decode_json_from_pipe(
	    $LINSTOR, "--controllers=$controller", "-m",
	    "volume-definition", "list");

    for my $entry (@$json) {
        for my $rsc_dfn ( $entry->{rsc_dfns} ) {
            for my $rsc (@$rsc_dfn) {
                my $volname = $rsc->{rsc_name};
                next if $volname !~ m/^vm-(\d+)-/;
                my $vmid = $1;

                my $size_kib;
                if ( exists $rsc->{vlm_dfns}
                    and scalar @{ $rsc->{vlm_dfns} } == 1 )
                {
                    my $size_kib = $rsc->{vlm_dfns}[0]->{vlm_size};
                    my $size     = $size_kib * 1024;

                    $volumes->{$volname} =
                      { format => 'raw', size => $size, vmid => $vmid };
                }
            }
        }
    }

    return $volumes;
}

sub drbd_exists_locally {
    my ( $scfg, $resname, $nodename, $disklessonly ) = @_;

    my $controller = get_controller($scfg);
    my $json = decode_json_from_pipe(
	    $LINSTOR, "--controllers=$controller", "-m",
	    "resource", "list", "-r", $resname, "-n", $nodename);

    return undef unless exists $json->[0]->{resource_states};

    # We told linstor above to filter for resname,nodename already,
    # so this "loop" should "iterate" over exactly one element now.
    for my $res ( @{ $json->[0]->{resource_states} } ) {
        if ( $res->{rsc_name} eq $resname and $res->{node_name} eq $nodename ) {
            return 1 if not $disklessonly;
            if ($disklessonly) {
                return 1 if $res->{vlm_states}[0]->{disk_state} eq "Diskless";
		# FIXME what if more than one volume per resource?
            }
        }
    }

    return undef;
}

sub volname_and_snap_to_snapname {
    my ( $volname, $snap ) = @_;
    return "snap_${volname}_${snap}";
}

sub linstor_cmd {
    my ( $scfg, $cmd, $errormsg ) = @_;
    my $controller = get_controller($scfg);
    unshift @$cmd, $LINSTOR, "--no-color", "--no-utf8", "--controllers=$controller";
    run_command( $cmd, errmsg => $errormsg );
}

# Storage implementation
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

    my $path = "/dev/drbd/by-res/$volname/0";

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
    if (!defined($name)) {
        $retname = "vm-$vmid-disk-1";
    }
    return $retname if ignore_volume( $scfg, $retname );

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    die "illegal name '$name' - should be 'vm-$vmid-*'\n"
      if defined($name) && $name !~ m/^vm-$vmid-/;

    my $volumes = drbd_list_volumes($scfg);

    die "volume '$name' already exists\n"
      if defined($name) && $volumes->{$name};

    if ( !defined($name) ) {
        for ( my $i = 1 ; $i < 100 ; $i++ ) {
            my $tn = "vm-$vmid-disk-$i";
            if ( !defined( $volumes->{$tn} ) ) {
                $name = $tn;
                last;
            }
        }
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
      if !defined($name);

    $size = $size . 'kiB';
    linstor_cmd(
        $scfg,
        [ 'resource-definition', 'create', $name ],
        "Could not create resource definition $name"
    );
    linstor_cmd(
        $scfg,
        [
            'resource-definition', 'drbd-options',
            '--allow-two-primaries=yes', $name
        ],
        "Could not set 'allow-two-primaries'"
    );
    linstor_cmd(
        $scfg,
        [ 'volume-definition', 'create', $name, $size ],
        "Could not create-volume-definition in $name resource"
    );

    my $redundancy = get_redundancy($scfg);
    linstor_cmd(
        $scfg,
        [ 'resource', 'create', $name, '--auto-place', $redundancy ],
        "Could not place $name"
    );

    return $name;
}

sub free_image {
    my ( $class, $storeid, $scfg, $volname, $isBase ) = @_;

    # die() does not really help in that case, the VM definition is still removed
    # so we could just return undef, still this looks a bit cleaner
    die "Not freeing contoller VM" if ignore_volume($scfg, $volname);

    linstor_cmd(
        $scfg,
        [ 'resource-definition', 'delete', $volname ],
        "Could not remove $volname"
    );

    return undef;
}

sub list_images {
    my ( $class, $storeid, $scfg, $vmid, $vollist, $cache ) = @_;

    my $vgname = $scfg->{vgname};

    $cache->{drbd_volumes} = drbd_list_volumes($scfg)
      if !$cache->{drbd_volumes};

    my $res = [];
    my $dat = $cache->{drbd_volumes};

    foreach my $volname ( keys %$dat ) {
        my $owner = $dat->{$volname}->{vmid};
        my $volid = "$storeid:$volname";

        if ($vollist) {
            my $found = grep { $_ eq $volid } @$vollist;
            next if !$found;
        }
        else {
            next if defined($vmid) && ( $owner ne $vmid );
        }

        my $info = $dat->{$volname};
        $info->{volid} = $volid;

        push @$res, $info;
    }

    return $res;
}

sub status {
    my ( $class, $storeid, $scfg, $cache ) = @_;
    my $controller = get_controller($scfg);
    my $nodename   = PVE::INotify::nodename();

    my ( $total, $avail, $used );

    my $json = decode_json_from_pipe(
	    $LINSTOR, "--controllers=$controller", "-m",
	    "storage-pool", "list", "-n", $nodename);

    # we assume that in proxmox setups there is exactly one pool used for storage
    # therefore we use the one at [0]
    my $free_space = $json->[0]->{stor_pools}->[0]->{free_space};
    return undef unless $free_space;

    # they want it in bytes
    $avail = $free_space->{free_capacity} * 1024;
    $total = $free_space->{total_capacity} * 1024;
    $used  = $total - $avail;

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
    my ( $class, $storeid, $scfg, $volname, $snapname, $cache ) = @_;

    die "Snapshot not implemented on DRBD\n" if $snapname;

    return undef if ignore_volume($scfg, $volname);

    my $path = $class->path( $scfg, $volname );

    my $nodename = PVE::INotify::nodename();

    # my $redundancy = get_redundancy($scfg);;

    # create diskless assignment if required
    if ( drbd_exists_locally( $scfg, $volname, $nodename, 0 ) ) {
        return undef;
    }

    linstor_cmd(
        $scfg,
        [ 'resource', 'create', '--diskless', $nodename, $volname ],
        "Could not create diskless resource ($volname) on $nodename)"
    );

    # wait until device is accessible
    my $print_warning = 1;
    my $max_wait_time = 20;
    for ( my $i = 0 ; ; $i++ ) {
        last
          if system("dd if=$path of=/dev/null bs=512 count=1 >/dev/null 2>&1")
          == 0;
        die "aborting wait - device '$path' still not readable\n"
          if $i > $max_wait_time;
        print "waiting for device '$path' to become ready...\n"
          if $print_warning;
        $print_warning = 0;
        sleep(1);
    }

    return undef;
}

sub deactivate_volume {
    my ( $class, $storeid, $scfg, $volname, $snapname, $cache ) = @_;

    die "Snapshot not implemented on DRBD\n" if $snapname;

    return undef if ignore_volume($scfg, $volname);

    my $nodename = PVE::INotify::nodename();
    if ( drbd_exists_locally( $scfg, $volname, $nodename, 1 ) ) {
        linstor_cmd(
            $scfg,
            [ 'resource', 'delete', $nodename, $volname ],
            "Could not delete  resource ($volname) on $nodename)"
        );
    }

    return undef;
}

sub volume_resize {
    my ( $class, $scfg, $storeid, $volname, $size, $running ) = @_;

    $size = ( $size / 1024 ) . 'kiB';
    linstor_cmd(
        $scfg,
        [ 'volume-definition', 'set-size', $volname, 0, $size ],
        "Could not resize $volname"
    );

    return 1;
}

sub volume_snapshot {
    my ( $class, $scfg, $storeid, $volname, $snap ) = @_;

    my $snapname = volname_and_snap_to_snapname( $volname, $snap );
    my $nodename = PVE::INotify::nodename();
    linstor_cmd(
        $scfg,
        [ 'snapshot', 'create', $nodename, $volname, $snapname ],
        "Could not create snapshot for $volname on $nodename"
    );

    return 1;
}

sub volume_snapshot_rollback {
    my ( $class, $scfg, $storeid, $volname, $snap ) = @_;

    die "DRBD snapshot rollback is not implemented, please use 'linstor' to recover your data, use 'qm unlock' to unlock your VM";
}

sub volume_snapshot_delete {
    my ( $class, $scfg, $storeid, $volname, $snap ) = @_;
    my $snapname = volname_and_snap_to_snapname( $volname, $snap );

    linstor_cmd(
        $scfg,
        [ 'snapshot', 'delete', $volname, $snapname ],
        "Could not remove snapshot $snapname for resource $volname"
    );

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
