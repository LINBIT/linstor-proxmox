package PVE::Storage::Custom::DRBDPlugin;
# vim: set et tw=8 sw=4 :

use strict;
use warnings;
use IO::File;
use Net::DBus;
use Data::Dumper;

use PVE::Tools qw(run_command trim);
use PVE::INotify;
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# Configuration

my $default_redundancy = 2;
my $APIVER = 1;

sub api {
    return $APIVER;
}

sub type {
    return 'drbd';
}

sub plugindata {
    return {
        content => [ {images => 1, rootdir => 1}, { images => 1 }],
    };
}

sub properties {
    return {
        redundancy => {
            description => "The redundancy count specifies the number of nodes to which the resource should be deployed. It must be at least 1 and at most the number of nodes in the cluster.",
            type => 'integer',
            minimum => 1,
            maximum => 16,
            default => $default_redundancy,
        },
    };
}

sub options {
    return {
        redundancy => { optional => 1 },
        content => { optional => 1 },
        nodes => { optional => 1 },
        disable => { optional => 1 },
    };
}

# helper

sub get_redundancy {
    my ($scfg) = @_;

    return $scfg->{redundancy} || $default_redundancy;
}

sub drbd_list_volumes {
    my $volumes = {};

    # call drbdmange lv -m
    my @lv = qx{/usr/bin/drbdmanage list-volumes -m};

    foreach my $line (@lv) {
        my @f = split /,/, $line;
        my ($volname, $size_kib) = ($f[0], $f[3]);
        next if $volname !~ m/^vm-(\d+)-/;
        my $vmid = $1;

        my $size = $size_kib * 1024;

        $volumes->{$volname} = { format => 'raw', size => $size,
            vmid => $vmid };
    }

    return $volumes;
}

sub drbdmanage_cmd {
    # be extra pedantic.
    run_command(['/usr/bin/drbdmanage', 'wait-for-startup'], errmsg => 'drbdmanage not ready');

    my ($cmd, $errormsg) = @_;
    run_command($cmd, errmsg => $errormsg);
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m/^(vm-(\d+)-[a-z][a-z0-9\-\_\.]*[a-z0-9]+)$/) {
        return ('images', $1, $2, undef, undef, undef, 'raw');
    }

    die "unable to parse lvm volume name '$volname'\n";
}

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;

    die "drbd snapshot is not implemented\n" if defined($snapname);

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    # fixme: always use volid 0?
    my $path = "/dev/drbd/by-res/$volname/0";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "can't create base images in drbd storage\n";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    die "can't clone images in drbd storage\n";
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    die "illegal name '$name' - should be 'vm-$vmid-*'\n"
    if defined($name) && $name !~ m/^vm-$vmid-/;

    my $volumes = drbd_list_volumes();

    die "volume '$name' already exists\n" if defined($name) && $volumes->{$name};

    if (!defined($name)) {
        for (my $i = 1; $i < 100; $i++) {
            my $tn = "vm-$vmid-disk-$i";
            if (!defined ($volumes->{$tn})) {
                $name = $tn;
                last;
            }
        }
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
    if !defined($name);

    $size = ($size/1024/1024);
    drbdmanage_cmd(['/usr/bin/drbdmanage', 'new-resource', $name], "Could not create resource $name");
    drbdmanage_cmd(['/usr/bin/drbdmanage', 'new-volume', $name, $size], "Could not create-volume in $name resource");
    drbdmanage_cmd(['/usr/bin/drbdmanage', 'net-options', '--resource', $name, '--allow-two-primaries=yes'], "Could not set 'allow-two-primaries'");

    my $redundancy = get_redundancy($scfg);;
    drbdmanage_cmd(['/usr/bin/drbdmanage', 'deploy',  $name, $redundancy], "Could not deploy $name");

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    drbdmanage_cmd(['/usr/bin/drbdmanage', 'remove-resource', '-q', $volname], "Could not remove $volname");

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $vgname = $scfg->{vgname};

    $cache->{drbd_volumes} = drbd_list_volumes() if !$cache->{drbd_volumes};

    my $res = [];

    my $dat =  $cache->{drbd_volumes};

    foreach my $volname (keys %$dat) {

        my $owner = $dat->{$volname}->{vmid};

        my $volid = "$storeid:$volname";

        if ($vollist) {
            my $found = grep { $_ eq $volid } @$vollist;
            next if !$found;
        } else {
            next if defined ($vmid) && ($owner ne $vmid);
        }

        my $info = $dat->{$volname};
        $info->{volid} = $volid;

        push @$res, $info;
    }

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my ($total, $avail, $used);

    eval {
        my $redundancy = get_redundancy($scfg);
        my @fs = qx{/usr/bin/drbdmanage free-space $redundancy -m};

        my @f = split /,/, $fs[0];
        my ($free_space, $total_space) = ($f[0], $f[1]);

        $avail = $free_space*1024;
        $total = $total_space*1024;
        $used = $total - $avail;

    };
    if (my $err = $@) {
        # ignore error,
        # assume storage if offline

        return undef;
    }

    return ($total, $avail, $used, 1);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    return undef;
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    return undef;
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    die "Snapshot not implemented on DRBD\n" if $snapname;

    my $path = $class->path($scfg, $volname);

    my $nodename = PVE::INotify::nodename();

    my @res = qx{/usr/bin/drbdmanage list-assignments -m -N $nodename -R $volname};

# assignment already exists?
    return undef if @res;

    # create diskless assignment
    drbdmanage_cmd(['/usr/bin/drbdmanage', 'assign-resource', '--client', $volname, $nodename], "Could not create diskless assignment ($volname -> $nodename)");

    # wait until device is accessible
    my $print_warning = 1;
    my $max_wait_time = 20;
    for (my $i = 0;; $i++) {
        if (1) {
            # clumsy, but works
            last if system("dd if=$path of=/dev/null bs=512 count=1 >/dev/null 2>&1") == 0;
        } else {
            # correct, but does not work?
            # my ($rc, $res) = $hdl->list_assignments([$nodename], [$volname], 0, { "cstate:deploy" => "true" }, []);
            # check_drbd_res($rc);
            # my $len = scalar(@$res);
            # last if $len > 0;
        }
        die "aborting wait - device '$path' still not readable\n" if $i > $max_wait_time;
        print "waiting for device '$path' to become ready...\n" if $print_warning;
        $print_warning = 0;
        sleep(1);
    }

    return undef;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    die "Snapshot not implemented on DRBD\n" if $snapname;

    return undef; # fixme: should we unassign ?

    # remove above return to enable this code
    my $nodename = PVE::INotify::nodename();

    my @as = qx{/usr/bin/drbdmanage list-assignments -N $nodename -R $volname -m};
    my @f = split /,/, $as[0];
    my $cstate = $f[3];

    if ($cstate =~ /diskless/) {
        drbdmanage_cmd(['/usr/bin/drbdmanage', 'unassign-resource', $volname, $nodename], "Could not unassign resource $volname from node $nodename");
    }

    return undef;
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    $size = ($size/1024/1024/1024);
    drbdmanage_cmd(['/usr/bin/drbdmanage', 'resize', $volname, 0, $size], "Could not resize $volname");

    return 1;
}

sub volname_and_snap_to_snapname {
    my ($volname, $snap) = @_;
    return "snap_${volname}_${snap}";
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my $snapname = volname_and_snap_to_snapname($volname, $snap);
    my $nodename = PVE::INotify::nodename();
    drbdmanage_cmd(['/usr/bin/drbdmanage', 'create-snapshot', $snapname, $volname, $nodename], "Could not create snapshot for $volname on $nodename");

    return 1
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "drbd snapshot rollback is not implemented, please use 'drbdmanage' to recover your data, use 'qm unlock' to unlock your VM";
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    my $snapname = volname_and_snap_to_snapname($volname, $snap);

    drbdmanage_cmd(['/usr/bin/drbdmanage', 'remove-snapshot', $volname, $snapname], "Could not remove snapshot $snapname for resource $volname");

    return 1
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
        copy => { base => 1, current => 1},
        snapshot => { current => 1 },
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
    $class->parse_volname($volname);

    my $key = undef;
    if($snapname){
        $key = 'snap';
    }else{
        $key =  $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};

    return undef;
}


1;
