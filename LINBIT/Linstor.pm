package LINBIT::Linstor;

use strict;
use warnings;

use REST::Client;
use JSON::XS qw( decode_json );
use JSON::XS qw( encode_json );
use Types::Serialiser;
use Carp qw( confess );

## helpers

sub dieContent {
	my ($msg, $restErr) = @_;
	# TODO: format it nicely
	die "API Return-Code: " . $restErr->responseCode() . ". Message: " . $msg . ", because:\n". $restErr->responseContent() . "\n";
}

# This plugin is thingle threaded, a real library would need a mutex around self->{xyz}

sub new{
	my ($class,$args) = @_;
	my $self = bless { cli => $args->{cli} }, $class;
}

# update the internal state and return it
sub update_resources {
    my $self = shift;

    my $res_info = {};
    my $ret      = $self->{cli}->GET('/v1/view/resources');
    dieContent "Could not get resource information", $ret
      unless $ret->responseCode() eq '200';

    my $resources;
    eval { $resources = decode_json( $ret->responseContent() ); };
    confess $@ if $@;

    foreach my $lr (@$resources) {
        my $res_name  = $lr->{name};
        my $node_name = $lr->{node_name};

        if ( !exists( $res_info->{$res_name} ) ) {
            $res_info->{$res_name} = {};
        }

        if ( exists( $res_info->{$res_name}->{$node_name} ) ) {
            next;
        }

        my $conf_as_diskless = $lr->{volumes}[0]{provider_kind} || '';
        $conf_as_diskless = lc $conf_as_diskless eq lc 'DISKLESS';

        my $current_state = $lr->{volumes}[0]{state}{disk_state} || '';
        $current_state = lc $current_state;

        my $storage_pool_name = $lr->{volumes}[0]{storage_pool_name} || '';

        my $in_use = $lr->{state}{in_use} || 0;

        my $usable_size_kib =
          $lr->{layer_object}{drbd}{drbd_volumes}[0]{usable_size_kib} || 0;

        my $nr_vols = @{ $lr->{volumes} } || 0;

        $res_info->{$res_name}->{in_use} = $in_use;
        $res_info->{$res_name}->{$node_name} = {
            "cur_state"         => $current_state,
            "conf_as_diskless"  => $conf_as_diskless,
            "usable_size_kib"   => $usable_size_kib,
            "nr_vols"           => $nr_vols,
            "storage_pool_name" => $storage_pool_name,
        };
    }

    $self->{res_info} = $res_info;
    return $res_info;
}

# update the internal state and return it
sub update_storagepools {
    my $self = shift;

    my $sp_info = {};
    my $ret = $self->{cli}->GET('/v1/view/storage-pools');
    dieContent "Could not get storage-pool information", $ret
      unless $ret->responseCode() eq '200';

    my $storagepools;
    eval { $storagepools = decode_json( $ret->responseContent() ); };
    confess $@ if $@;

    foreach my $sp (@$storagepools) {
        my $sp_name   = $sp->{storage_pool_name};
        my $node_name = $sp->{node_name};

        if ( !exists( $sp_info->{$sp_name} ) ) {
            $sp_info->{$sp_name} = {};
        }

        if ( exists( $sp_info->{$sp_name}->{$node_name} ) ) {
            next;
        }

        my $conf_as_diskless   = $sp->{provider_kind} || '';
        $conf_as_diskless   = lc $conf_as_diskless eq lc 'DISKLESS';

        my $free_capacity_kib  = $sp->{free_capacity} || 0;
        my $total_capacity_kib = $sp->{total_capacity} || 0;

        $sp_info->{$sp_name}->{$node_name} = {
            "conf_as_diskless"   => $conf_as_diskless,
            "free_capacity_kib"  => $free_capacity_kib,
            "total_capacity_kib" => $total_capacity_kib,
        };
    }

    $self->{sp_info} = $sp_info;
    return $sp_info;
}

# always return the existing one, if you want an updated one, call update_resources()
# just a getter that does initial update, information could be stale
sub get_resources {
	my $self = shift;

	return $self->{res_info}
		if exists($self->{res_info});

	return $self->update_resources();
}

# always return the existing one, if you want an updated one, call update_storagepools()
# just a getter that does initial update, information could be stale
sub get_storagepools {
	my $self = shift;

	return $self->{sp_info}
		if exists($self->{sp_info});

	return $self->update_storagepools();
}


sub resource_exists {
	my ($self, $name, $node_name) = @_;

	$self->update_resources();

	if (defined($node_name)) {
		return exists($self->get_resources()->{$name}->{$node_name});
	} else {
		return exists($self->get_resources()->{$name});
	}

	return 0;
}

sub resource_exists_intentionally_diskless {
    my ( $self, $name, $node_name ) = @_;

    # implicit state update via resource_exists()
    return 1
      if (  $self->resource_exists( $name, $node_name )
        and $self->get_resources()->{$name}->{$node_name}->{conf_as_diskless} );

    return 0;
}

sub create_resource_definition {
    my ( $self, $name ) = @_;

    my $ret = $self->{cli}->POST( '/v1/resource-definitions',
        encode_json( { resource_definition => { name => $name } } ) );
    dieContent "Could not create resource definition $name", $ret
      unless $ret->responseCode() eq '201';

	return 1;
}


sub create_resource_manual {
    my ( $self, $name, $size_kib, $storage_pool, $place_count ) = @_;

    $self->create_resource_definition($name);

    my $ret = $self->{cli}->POST(
        "/v1/resource-definitions/$name/volume-definitions",
        encode_json( { volume_definition => { size_kib => $size_kib } } )
    );
    dieContent "Could not create volume definition for resource $name", $ret
      unless $ret->responseCode() eq '201';

    $ret = $self->{cli}->POST(
        "/v1/resource-definitions/$name/autoplace",
        encode_json(
            {
                select_filter => {
                    place_count  => $place_count,
                    storage_pool => $storage_pool
                }
            }
        )
    );
    dieContent "Could not autoplace resource $name", $ret
      unless $ret->responseCode() eq '201';

    return 1;
}

sub create_resource_res_group {
    my ( $self, $name, $size_kib, $resgroup_name, $local_node_name ) = @_;

    my $definitions_only = Types::Serialiser::false;
    if ( defined($local_node_name) ) {
        $definitions_only = Types::Serialiser::true;
    }

    my $ret = $self->{cli}->POST(
        "/v1/resource-groups/$resgroup_name/spawn",
        encode_json(
            {
                resource_definition_name => $name,
                definitions_only         => $definitions_only,
                volume_sizes             => [$size_kib]
            }
        )
    );

    dieContent "Could not create resource definition $name from resource group $resgroup_name", $ret
      unless $ret->responseCode() eq '201';

    if ($definitions_only) {
        # maybe it can not even get local storage. just ignore the return value, the autoplace fixes it
        # alternatively we could first check if the node even has the SP.
        $ret = $self->{cli}->POST(
          "/v1/resource-definitions/$name/resources/$local_node_name/make-available",
          encode_json(
              {
                  diskful => Types::Serialiser::true
              }
          )
        );
        print "  Diskfull assignment failed, let's autoplace it.\n"
          unless $ret->responseCode() eq '200';

        $ret = $self->{cli}->POST( "/v1/resource-definitions/$name/autoplace",
            encode_json( {} ) );
        dieContent "Could not autoplace resource $name", $ret
          unless $ret->responseCode() eq '201';
    }

    return 1;
}

sub create_resource {
    my ( $self, $name ) = @_;

    create_resource_res_group(@_);

    my $ret = $self->{cli}->PUT(
        "/v1/resource-definitions/$name",
        encode_json(
            {
                override_props =>
                  { 'DrbdOptions/Net/allow-two-primaries' => 'yes' }
            }
        )
    );
    dieContent "Could not set allow-two-primaries on resource definition $name",
      $ret
      unless $ret->responseCode() eq '200';

    return 1;
}


sub activate_resource {
    my ( $self, $name, $node_name, $diskless_storage_pool ) = @_;

    $diskless_storage_pool = "DfltDisklessStorPool"
      unless defined($diskless_storage_pool);

    # implicit state update via resource_exists
    return undef
      if $self->resource_exists( $name, $node_name );

    my $ret = $self->{cli}->POST(
        "/v1/resource-definitions/$name/resources",
        encode_json(
            [
                {
                    resource => {
                        node_name => $node_name,
                        props     => {
                            StorPoolName => $diskless_storage_pool,
                        },
                        flags => ["DISKLESS"]
                    }
                }
            ]
        )
    );

    dieContent "Could not create diskless resource $name on $node_name", $ret
      unless $ret->responseCode() eq '201';

    return undef;
}

sub deactivate_resource {
    my ( $self, $name, $node_name ) = @_;

    # implicit state update via resource_exists()
    return undef
      unless $self->resource_exists_intentionally_diskless( $name, $node_name );

    # in case this was the auto tie-breaker:
    # on activation, when the resource became diskless *Primary, the TB flag got removed, it became a regular diskless.
    # on deactivation, LINSTOR sees the delete below, but does not delete the diskless, but converts it to a TB again.
    my $ret = $self->{cli}
      ->DELETE("/v1/resource-definitions/$name/resources/$node_name");

    dieContent "Could not delete diskless resource $name on $node_name", $ret
      unless $ret->responseCode() eq '200';

    # does not update objects state
    return undef;
}

sub resize_resource {
    my ( $self, $name, $size_kib ) = @_;

    my $ret =
      $self->{cli}->PUT( "/v1/resource-definitions/$name/volume-definitions/0",
        encode_json( { size_kib => $size_kib } ) );
    dieContent
      "Could not set size ($size_kib KiB) on resource definition $name", $ret
      unless $ret->responseCode() eq '200';

    return undef;
}

sub delete_resource {
    my ( $self, $name, $node_name ) = @_;

    # implicit state update via resource_exists()
    return undef unless $self->resource_exists($name);

    my $url;
    if ( defined($node_name) ) {
        $url = "/v1/resource-definitions/$name/resources/$node_name";
    } else {
        $url = "/v1/resource-definitions/$name";
    }

    my $ret = $self->{cli}->DELETE($url);

    dieContent "Could not delete resource $name", $ret
      unless $ret->responseCode() eq '200';

    # does not update objects state
    return undef;
}

sub create_snapshot {
    my ( $self, $res_name, $snap_name ) = @_;

    my $ret = $self->{cli}->POST(
        "/v1/resource-definitions/$res_name/snapshots",
        encode_json( { name => $snap_name } )
    );
    dieContent "Could not create cluster wide snapshot $snap_name of $res_name",
      $ret
      unless $ret->responseCode() eq '201';

    return undef;
}

sub delete_snapshot {
    my ( $self, $res_name, $snap_name ) = @_;

    my $ret = $self->{cli}
      ->DELETE("/v1/resource-definitions/$res_name/snapshots/$snap_name");
    dieContent "Could not delete cluster wide snapshot $snap_name of $res_name", $ret
      unless $ret->responseCode() eq '200';

    return undef;
}

sub rollback_snapshot {
    my ( $self, $res_name, $snap_name ) = @_;

    my $ret = $self->{cli}
      ->POST("/v1/resource-definitions/$res_name/snapshot-rollback/$snap_name");
    dieContent "Could not rollback cluster wide snapshot $snap_name of $res_name", $ret
      unless $ret->responseCode() eq '200';

    return undef;
}

sub restore_snapshot {
    my ( $self, $res_name, $snap_name, $new_res_name ) = @_;

    $self->create_resource_definition($new_res_name);

    my $ret = $self->{cli}->POST("/v1/resource-definitions/$res_name/snapshot-restore-volume-definition/$snap_name",
        encode_json( { to_resource => $new_res_name } )
    );
    dieContent "Could not restore snapshot volume definition $snap_name of $res_name to new $new_res_name", $ret
      unless $ret->responseCode() eq '200';

    $ret = $self->{cli}->POST("/v1/resource-definitions/$res_name/snapshot-restore-resource/$snap_name",
        encode_json( { to_resource => $new_res_name } )
    );
    dieContent "Could not restore snapshot $snap_name of $res_name to new $new_res_name", $ret
      unless $ret->responseCode() eq '200';

    return undef;
}

sub get_storagepool_for_resource_group {
    my ( $self, $resgroup_name ) = @_;

    my $ret = $self->{cli}->GET("/v1/resource-groups/$resgroup_name");
    dieContent "Could not get resource information", $ret
      unless $ret->responseCode() eq '200';

    my $resgroups;
    eval { $resgroups = decode_json( $ret->responseContent() ); };
    die $@ if $@;

    return $resgroups->{select_filter}->{storage_pool_list};
}

1;
