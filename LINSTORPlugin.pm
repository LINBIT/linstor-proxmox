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
        storagepool => {
             description => "The name of the LINSTOR storage pool to be used. Leave off if you want to use LINSTOR defaults.",
             type        => 'string',
             default     => undef,
        },
    };
## Please see file perltidy.ERR
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

# returns "-s", "$poolname" if defined,
# empty list otherwise.
sub dash_s_poolname_if_defined {
    my ($scfg) = @_;
    defined $scfg->{storagepool} ?
    ( "-s", $scfg->{storagepool} ) : ()
}

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

sub drbd_exists_locally {
    my ( $scfg, $resname, $nodename, $disklessonly ) = @_;

    my $controller = get_controller($scfg);
    my $r_list = decode_json_from_pipe(
	    $LINSTOR, "--controllers=$controller", "-m",
            "resource", "list",
            "--resources", $resname,
            "--nodes", $nodename);

    my (%resource, %resource_state);
    eval {
	%resource_state = map {
	    $_->{rsc_name} => {
		is_primary => !!$_->{in_use},
		Diskless => scalar grep { $_->{disk_state} eq "Diskless" }
					    @{$_->{vlm_states}},
	    }
	} @{$r_list->[0]->{resource_states}};
	%resource = map {
	    $_->{name} => {
                backend => join(", ", map { $_->{backing_disk} } @{$_->{vlms}}),
                DISKLESS => (scalar grep { $_ eq "DISKLESS" }
                    @{$_->{rsc_flags} ||= []}),
	    }
	} @{$r_list->[0]->{resources}};
    };
    warn $@ if $@;

    return undef unless exists $resource{$resname};

    # please clean up that mess manually yourself
    die ("DRBD resource ($resname) defined but unconfigured (down) on node ($nodename)!?\n" .
	 "'drbdadm adjust $resname' on $nodename may help.\n")
    	unless exists $resource_state{$resname};

    warn("WARNING:\n" .
	 "  DRBD resource ($resname) expected to have local storage on node ($nodename), but is currently detached,\n" .
	 "  possibly due to earlier IO problems on the backend ($resource{$resname}{backend}).\n" .
	 "  'drbdadm adjust $resname' on $nodename may help.\n")
 	if $resource_state{$resname}{Diskless} and not $resource{$resname}{DISKLESS};

    return 1 unless $disklessonly;
    return $resource{$resname}{DISKLESS};
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

sub wait_connect_resource {
    my ($resource) = @_;

    run_command(
        [ 'drbdsetup', 'wait-connect-resource', $resource ],
        errmsg => "Could not wait until replication established for ($resource)",
        timeout => 60 # could use --wfc-timeout, but hey when we already do it proxmoxy...
    );
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

    my $controller = get_controller($scfg);
    my $rd_list = decode_json_from_pipe(
        $LINSTOR, "--controllers=$controller", "-m",
        "resource-definition", "list");
    # [ { "rsc_dfns": [
    #       { "rsc_name": "XYZ", ...,
    #         "vlm_dfns": [ { "vlm_size": size-in-kiB, "vlm_nr": Nr, ... },
    #                       { ... } ] },
    #       { ... }, { ... }
    # ] } ]
    #
    # TODO:
    # Currently we have/expect one resource per volume per proxmox disk image,
    # we do not (yet) use or expect multi-volume resources, even thought it may
    # be useful to have all vm images in one "consistency group".

    # this is used to check for existence of rsc_name only:
    my %resource = map { $_->{rsc_name} => 1 } @{$rd_list->[0]->{rsc_dfns}};

    die "volume '$name' already exists\n"
      if defined($name) && exists $resource{$name};

    if ( !defined($name) ) {
        for ( my $i = 1 ; $i < 100 ; $i++ ) {
            my $tn = "vm-$vmid-disk-$i";
            if ( !exists( $resource{$tn} ) ) {
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
        [ 'resource', 'create', $name,
          dash_s_poolname_if_defined($scfg),
                '--auto-place', $redundancy ],
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

    my $res = [];
    my $nodename   = PVE::INotify::nodename();
    my $controller = get_controller($scfg);

    $cache->{"linstor:rd_list"} = decode_json_from_pipe(
            $LINSTOR, "--controllers=$controller", "-m",
            "resource-definition", "list")
        unless $cache->{"linstor:rd_list"};
    # [ { "rsc_dfns": [
    #       { "rsc_name": "XYZ", ...,
    #         "vlm_dfns": [ { "vlm_size": size-in-kiB, "vlm_nr": Nr, ... },
    #                       { ... } ] },
    #       { ... }, { ... }
    # ] } ]

    $cache->{"linstor:r_list"} = decode_json_from_pipe(
            $LINSTOR, "--controllers=$controller", "-m",
            "resource", "list",
            "--nodes", $nodename)
        unless $cache->{"linstor:r_list"};
    # [ { "resource_states": [ ... ],
    #     "resources": [
    #       { "vlms": [ { "stor_pool_name": storagepool, ... }, { ... } ],
    #           "name": "XYZ", ...  }, { ... } ]
    # } ]

    # TODO:
    # Currently we have/expect one resource per volume per proxmox disk image,
    # we do not (yet) use or expect multi-volume resources, even thought it may
    # be useful to have all vm images in one "consistency group".

    # Also, I'd like to have the actual current size reported
    # in the "resource list", so I won't have to query both
    # resource and resource-definition...

    my ($rd_list, $r_list) = @$cache{qw(linstor:rd_list linstor:r_list)};

    my %pool_of_res = map {
        $_->{name} => $_->{vlms}->[0]->{stor_pool_name} // ""
    } grep {
        $_->{name} =~ /^vm-\d+-/ and
        exists $_->{vlms} and scalar @{$_->{vlms}} == 1
    } @{$r_list->[0]->{resources}};

    # could also be written as map {} grep {} ...
    # but a for loop is probably easier to maintain
    for my $rsc (@{$rd_list->[0]->{rsc_dfns}}) {
        my $name = $rsc->{rsc_name};

        # skip if not on this node
        next unless exists $pool_of_res{$name};

        next unless $name =~ /^vm-(\d+)-/;
        my $owner = $1; # aka "vmid"

        # expect exactly one volume
        # XXX warn for 0 or >= 2 volume resources?
        next unless exists $rsc->{vlm_dfns} and scalar @{$rsc->{vlm_dfns}} == 1;

        my $size_kib = $rsc->{vlm_dfns}[0]{vlm_size};
        my $pool = $pool_of_res{$name};

        # filter by storagepool property, if set
        next if $scfg->{storagepool} and $scfg->{storagepool} ne $pool;

        push @$res,
            {
                format => 'raw',
                volid => "$storeid:$name",
                size => $size_kib * 1024,
                vmid => $owner,
            };
    }

    return $res;
}

sub status {
    my ( $class, $storeid, $scfg, $cache ) = @_;
    my $controller = get_controller($scfg);
    my $nodename   = PVE::INotify::nodename();

    my ( $total, $avail );

    $cache->{"linstor:sp_list"} = decode_json_from_pipe(
	    $LINSTOR, "--controllers=$controller", "-m",
            "storage-pool", "list", "--nodes", $nodename)
        unless $cache->{"linstor:sp_list"};
    my $sp_list = $cache->{"linstor:sp_list"};

    # To use the $cache, we do NOT filter for poolname above.
    # Iterate over "all of them",
    # aggregate in case it was undefined,
    # because that's effectively what we will get if we create a
    # new volume with auto-place without specifying the pool name.
    # Or filter here if it was defined.
    for my $pool (@{$sp_list->[0]->{stor_pools}}) {
        next if $scfg->{storagepool} and $scfg->{storagepool} ne $pool->{stor_pool_name};
	$avail += $pool->{free_space}->{free_capacity};
	$total += $pool->{free_space}->{total_capacity};
    }

    return undef unless $total;

    # they want it in bytes
    $total *= 1024;
    $avail *= 1024;
    return ($total, $avail, $total - $avail, 1);
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

    # create diskless assignment if required
    linstor_cmd(
        $scfg,
        [ 'resource', 'create', '--diskless', $nodename, $volname ],
        "Could not create diskless resource ($volname) on ($nodename)"
    ) unless drbd_exists_locally( $scfg, $volname, $nodename, 0 );

    wait_connect_resource($volname);

    return undef;
}

sub deactivate_volume {
    my ( $class, $storeid, $scfg, $volname, $snapname, $cache ) = @_;

    die "Snapshot not implemented on DRBD\n" if $snapname;

    return undef if ignore_volume($scfg, $volname);

    my $nodename = PVE::INotify::nodename();
    my $was_diskless_client = 0;

    eval { $was_diskless_client = drbd_exists_locally($scfg, $volname, $nodename, 1); };
    warn $@ if $@;
    
    if ($was_diskless_client) {
	print	"\nNOTICE\n" .
        	"  Intentionally removing diskless assignment ($volname) on ($nodename).\n" .
        	"  It will be re-created when the resource is actually used on this node.\n";
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
    # TODO: remove, temporary fix for non-synchronous LINSTOR resize
    sleep(10);

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
# vim: set et sw=4 :
