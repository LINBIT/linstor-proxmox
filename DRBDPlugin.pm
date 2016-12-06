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

sub connect_drbdmanage_service {

    my $bus = Net::DBus->system;

    my $service = $bus->get_service("org.drbd.drbdmanaged");

    my $hdl = $service->get_object("/interface", "org.drbd.drbdmanaged");

    return $hdl;
}

sub check_drbd_res {
    my ($rc) = @_;

    die "got undefined drbd result\n" if !$rc;

    # Messages for return codes 1 to 99 are not considered an error.
    foreach my $res (@$rc) {
        my ($code, $format, $details) = @$res;

        next if $code < 100;

        my $msg;
        if (defined($format)) {
            my @args = ();
            push @args, $details->{$1} // "" 
            while $format =~ s,\%\((\w+)\),%,;

            $msg = sprintf($format, @args);

        } else {    
            $msg = "drbd error: got error code $code";
        }

        chomp $msg;
        die "drbd error: $msg\n";
    }

    return undef;
}

sub drbd_list_volumes {
    my ($hdl) = @_;

    $hdl = connect_drbdmanage_service() if !$hdl;

    my ($rc, $res) = $hdl->list_volumes([], 0, {}, []);
    check_drbd_res($rc);

    my $volumes = {};

    foreach my $entry (@$res) {
        my ($volname, $properties, $vol_list) = @$entry;

        next if $volname !~ m/^vm-(\d+)-/;
        my $vmid = $1;

        # fixme: we always use volid 0 ?
        my $size = 0;
        foreach my $volentry (@$vol_list) {
            my ($vol_id, $vol_properties) = @$volentry;
            next if $vol_id != 0;
            my $vol_size = $vol_properties->{vol_size} * 1024;
            $size = $vol_size if $vol_size > $size;
        }

        $volumes->{$volname} = { format => 'raw', size => $size, 
            vmid => $vmid };
    }

    return $volumes; 
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

    my $hdl = connect_drbdmanage_service();
    my $volumes = drbd_list_volumes($hdl);

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

    my ($rc, $res) = $hdl->create_resource($name, {});
    check_drbd_res($rc);

    ($rc, $res) = $hdl->create_volume($name, $size, {});
    check_drbd_res($rc);

    ($rc, $res) = $hdl->set_drbdsetup_props(
        {
            target => "resource",
            resource => $name,
            type => 'neto',
            'allow-two-primaries' => 'yes',
        });
    check_drbd_res($rc);

    my $redundancy = get_redundancy($scfg);;

    ($rc, $res) = $hdl->auto_deploy($name, $redundancy, 0, 0);
    check_drbd_res($rc);

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my $hdl = connect_drbdmanage_service();
    my ($rc, $res) = $hdl->remove_resource($volname, 0);
    check_drbd_res($rc);

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
        my $hdl = connect_drbdmanage_service();
        my $redundancy = get_redundancy($scfg);;
        my ($rc, $free_space, $total_space) = $hdl->cluster_free_query($redundancy);
        check_drbd_res($rc);

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

    my $hdl = connect_drbdmanage_service();
    my $nodename = PVE::INotify::nodename();
    my ($rc, $res) = $hdl->list_assignments([$nodename], [$volname], 0, {}, []);
    check_drbd_res($rc);

# assignment already exists?
    return undef if @$res;

    # create diskless assignment
    ($rc, $res) = $hdl->assign($nodename, $volname, { diskless => 'true' });
    check_drbd_res($rc);

    # wait until device is accessible
    my $print_warning = 1;
    my $max_wait_time = 20;
    for (my $i = 0;; $i++) {
        if (1) {
            # clumsy, but works
            last if system("dd if=$path of=/dev/null bs=512 count=1 >/dev/null 2>&1") == 0;
        } else {
            # correct, but does not work?
            ($rc, $res) = $hdl->list_assignments([$nodename], [$volname], 0, { "cstate:deploy" => "true" }, []);
            check_drbd_res($rc);
            my $len = scalar(@$res);
            last if $len > 0;
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
    my $hdl = connect_drbdmanage_service();
    my $nodename = PVE::INotify::nodename();
    my ($rc, $res) = $hdl->list_assignments([$nodename], [$volname], 0, 
        { "cstate:diskless" => "true" }, []);
    check_drbd_res($rc);
    if (scalar(@$res)) {
        my ($rc, $res) = $hdl->unassign($nodename, $volname,0);
        check_drbd_res($rc);
    }

    return undef;    
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    $size = ($size/1024/1024) . "M";

    my $path = $class->path($scfg, $volname);

    # fixme: howto implement this
    die "drbd volume_resize is not implemented";

    #my $cmd = ['/sbin/lvextend', '-L', $size, $path];
    #run_command($cmd, errmsg => "error resizing volume '$path'");

    return 1;
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    die "drbd snapshot is not implemented";
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "drbd snapshot rollback is not implemented";
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "drbd snapshot delete is not implemented";
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
        copy => { base => 1, current => 1},
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
