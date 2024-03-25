#!/usr/bin/env bash
#
LC_ALL=C
LANG=C
LANGUAGE=C
TZ=UTC

RUNNER_NODE="pvg"
DESTINATION_NODE="pve"
LINSTOR_CONTROLLER_NODE="pve"
SNAP_NAME="aGheihie4e"
CLONE_VMID="123"
TESTS="uuid"

LEGACY_STORAGE="ontwo"
LEGACY_VM_FROM="100"
LEGACY_TEST_NAME="disk-42"

UUID_STORAGE="ontwodistinct"
UUID_VM_FROM="101"
UUID_VM_TO="102"
UUID_TEST_NAME="pm-12345678"

help() {
cat <<EOF
$(basename "$0")
   -h | --help: Print help and exit
   -r | --runner: Node this script should be executed on (default: '$RUNNER_NODE')
   -d | --destination: Destination node for VM migrations (default: '$DESTINATION_NODE')
   -c | --controller: LINSTOR controller node (default: '$LINSTOR_CONTROLLER_NODE')
   -s | --snapname: Name used as snapshot name (default: '$SNAP_NAME')
      | --clone: VMID used for cloned VM (default: '$CLONE_VMID')
      | --legacy-storage: Name of the legacy storage (default: '$LEGACY_STORAGE')
      | --legacy-vm-from: Legacy VMID from (default: '$LEGACY_VM_FROM')
      | --legacy-test-name: Name of a volume created for testing (default: '$LEGACY_TEST_NAME')
      | --uuid-storage: Name of the UUID storage (default: '$UUID_STORAGE')
      | --uuid-vm-from: UUID VMID from (default: '$UUID_VM_FROM')
      | --uuid-vm-to: UUID VMID to (default: '$UUID_VM_TO')
      | --uuid-test-name: Name of a volume created for testing (default: '$UUID_TEST_NAME')
      | --tests: Execute legacy or UUID tests (default: '$TESTS')
EOF
	exit "$1"
}

getopts() {
	[ "$(id -u)" = "0" ] || die "Run this script as root"

	OPTS=$(getopt -o hr:d:c:s: --long help,runner:,destination:,controller:,snapname:,clone:,legacy-storage:,legacy-vm-from:,legacy-test-name:,uuid-storage:,uuid-vm-from:,uuid-vm-to:,uuid-test-name:,tests: -n 'parse-options' -- "$@")
	[ $? = 0 ] || die "Failed parsing options."

	eval set -- "$OPTS"

	while true; do
		case "$1" in
			-h | --help ) help "0";;
			-r | --runner ) RUNNER_NODE="$2"; shift; shift ;;
			-d | --destination ) DESTINATION_NODE="$2"; shift; shift ;;
			-c | --controller ) LINSTOR_CONTROLLER_NODE="$2"; shift; shift ;;
			-s | --snapname ) SNAP_NAME="$2"; shift; shift ;;
			--clone ) CLONE_VMID="$2"; shift; shift ;;
			--legacy-storage ) LEGACY_STORAGE="$2"; shift; shift ;;
			--legacy-vm-from ) LEGACY_VM_FROM="$2"; shift; shift ;;
			--legacy-test-name ) LEGACY_TEST_NAME="$2"; shift; shift ;;
			--uuid-storage ) UUID_STORAGE="$2"; shift; shift ;;
			--uuid-vm-from ) UUID_VM_FROM="$2"; shift; shift ;;
			--uuid-vm-to ) UUID_VM_TO="$2"; shift; shift ;;
			--uuid-test-name ) UUID_TEST_NAME="$2"; shift; shift ;;
			--tests ) TESTS="$2"; shift; shift ;;
			-- ) shift; break ;;
			* ) break ;;
		esac
	done

    if [[ $TESTS != legacy ]] && [[ $TESTS != uuid ]]; then
        help "1"
    fi
}

die() { >&2 printf "\nError: %s\n" "$*"; exit 1; }

volname() {
    local vmid=$1 name=$2

    if echo "$name" | grep -q '^pm-'; then
        echo "${name}_${vmid}"
    elif echo "$name" | grep -q '^disk-'; then
        echo "vm-${vmid}-${name}"
    else
        die "unknown name '$name'"
    fi
}

alloc_image() {
    local storage=$1 vmid=$2 name=$3 size=$4

    # zero name is used to allocate an image with an auto generated name, otherwise generate the proper format
    if [[ -n "$name" ]]; then
        name=$(volname "$vmid" "$name")
    fi

    pvesm alloc "$storage" "$vmid" "$name" "$size"
}

free_image() {
    local storage=$1 vmid=$2 name=$3

    name="${storage}:$(volname "$vmid" "$name")"

    pvesm free "$name"
}

check_image_exists() {
    local storage=$1 vmid=$2 name=$3

    name="${storage}:$(volname "$vmid" "$name")"

    pvesm list "$storage" --vmid "$vmid" | grep -q "^${name}"
}

image_has_to_exist() {
    local storage=$1 vmid=$2 name=$3

    check_image_exists "$1" "$2" "$3" || die "Could not find image '$name' with VMID '$vmid' on '$storage'"
}

image_has_to_not_exist() {
    local storage=$1 vmid=$2 name=$3

    check_image_exists "$1" "$2" "$3" && die "Found unexpected image '$name' with VMID '$vmid' on '$storage'"
}

vm_must_stop() {
    local vmid=$1

    qm stop "$vmid" || die "Could not stop '$vmid'"
}

vm_must_start() {
    local vmid=$1

    qm start "$vmid" || die "Could not start '$vmid'"
}

vm_must_migrate() {
    local vmid=$1 to_node=$2 on_node=$3

    # if the vm is stopped, then '--online true' is ignored, so always use it
    ssh "root@${on_node}" qm migrate "$vmid" "$to_node" "--online" "true" || die "Could not start '$vmid'"
}

vm_must_snapshot() {
    local vmid=$1 snapname=$2

    qm snapshot "$vmid" "$snapname" || die "Could not create snapshot '$snapname' for VM '$vmid'"
}

vm_must_delsnapshot() {
    local vmid=$1 snapname=$2

    qm delsnapshot "$vmid" "$snapname" || die "Could not delete '$snapname' for VM '$vmid'"
}

vm_must_rollback() {
    local vmid=$1 snapname=$2

    qm rollback "$vmid" "$snapname" || die "Could rollback '$snapname' for VM '$vmid'"
}

check_snapshot_exists() {
    local vmid=$1 snapname=$2

    curl -sX GET http://${LINSTOR_CONTROLLER_NODE}:3370/v1/view/snapshots | jq '.[].name' | grep -q "$snapname" || return 1
    qm listsnapshot "$vmid" | grep -q "$snapname" || return 1

    return 0
}

snapshot_must_exist() {
    local vmid=$1 snapname=$2

    check_snapshot_exists "$vmid" "$snapname" || die "Snapshot '$snapname' does not exist"
}

snapshot_must_not_exist() {
    local vmid=$1 snapname=$2

    check_snapshot_exists "$vmid" "$snapname" && die "Snapshot '$snapname' should not exist"
}

vm_must_mv_disk() {
    local from_vmid=$1 from_disk=$2 to_vmid=$3 to_disk=$4

    qm disk move "$from_vmid" "$from_disk" --target-vmid "$to_vmid" --target-disk "$to_disk" || die "Could not move '$from_disk' (VM '$from_vmid') to '$to_disk' (VM '$to_vmid')"
}

vm_must_set_disk() {
    local vmid=$1 name=$2 disk=$3

    name="${storage}:$(volname "$vmid" "$name")"

    qm set "$vmid" --"$disk" "$name" || die "Could not set disk '$disk', '$name' for VM '$vmid'"
}

vm_must_clone() {
    local from_vmid=$1 to_vmid=$2

    qm clone "$from_vmid" "$to_vmid" || die "Could not clone '$from_vmid' to '$to_vmid'"
}

vm_must_destroy() {
    local vmid=$1

    qm destroy "$vmid" --purge || die "Could not destroy VM '$vmid'"
}

vm_must_remove_disk() {
    local vmid=$1 disk=$2

    # force to really remove it, otherwise it would just rm it from the VM config and create a "unnamed" disk
    qm disk unlink "$vmid" --idlist "$disk" --force || die "Could not remove '$disk' from VM '$vmid'"
}

named_image_must_create() {
    local storage=$1 vmid=$2 name=$3

    image_has_to_not_exist "$storage" "$vmid" "$name"
    alloc_image "$storage" "$vmid" "$name" "1G"
    image_has_to_exist "$storage" "$vmid" "$name"
}

### tests
[[ $(uname -n) == "$RUNNER_NODE" ]] || die "Wrong runner node: '$RUNNER_NODE'"

test_named_image() {
    local storage=$1 vmid=$2 name=$3

    named_image_must_create "$storage" "$vmid" "$name"
    free_image "$storage" "$vmid" "$name"
    image_has_to_not_exist "$storage" "$vmid" "$name"
}

test_unnamed_image() {
    local storage=$1 vmid=$2

    pvesm list "$storage" --vmid "$vmid" | awk "/^${storage}/ {print \$1}" | sort > /tmp/before
    echo "was:"; cat /tmp/before
    alloc_image "$storage" "$vmid" "" "1G"
    pvesm list "$storage" --vmid "$vmid" | awk "/^${storage}/ {print \$1}" | sort > /tmp/after
    echo "is:"; cat /tmp/after
    local new
    new=$(comm -13 /tmp/before /tmp/after)
    new=${new#"${storage}:"}
    new=${new%"_${vmid}"}
    echo "new: '$new'"
    image_has_to_exist "$storage" "$vmid" "$new"
    free_image "$storage" "$vmid" "$new"
    image_has_to_not_exist "$storage" "$vmid" "$new"
}

test_mv_vm() {
    local vmid=$1 to_node=$2 online=$3 on_node=$4

    local status
    status="$(qm status "$vmid")"
    if [[ $online = 'yes' ]] && [[ "$status" == "status: stopped" ]]; then
        vm_must_start "$vmid"
    fi
    if [[ $online = 'no' ]] && [[ "$status" == "status: running" ]]; then
        vm_must_stop "$vmid"
    fi
    vm_must_migrate "$vmid" "$to_node" "$on_node"
}

test_rename_disk() {
    local storage=$1 name=$2 from_vmid=$3 from_disk=$4 to_vmid=$5 to_disk=$6

    named_image_must_create "$storage" "$from_vmid" "$name"
    vm_must_set_disk "$from_vmid" "$name" "$from_disk"
    pvesm list "$storage" | grep "$name"
    vm_must_mv_disk "$from_vmid" "$from_disk" "$to_vmid" "$to_disk"
    image_has_to_exist "$storage" "$to_vmid" "$name"
    pvesm list "$storage" | grep "$name"
    vm_must_remove_disk "$to_vmid" "$to_disk"
    image_has_to_not_exist "$storage" "$to_vmid" "$name"
}

test_snapshot() {
    local vmid=$1 snapname=$2

    snapshot_must_not_exist "$vmid" "$snapname"
    vm_must_snapshot "$vmid" "$snapname"
    snapshot_must_exist "$vmid" "$snapname"
    vm_must_rollback "$vmid" "$snapname"
    vm_must_delsnapshot "$vmid" "$snapname"
    snapshot_must_not_exist "$vmid" "$snapname"
}

test_clone() {
    local from_vmid=$1 to_vmid=$2

    vm_must_clone "$from_vmid" "$to_vmid"
    vm_must_destroy "$to_vmid"
}

# main
getopts "$@"

# assume UUID
STORAGE=$UUID_STORAGE
VM_FROM=$UUID_VM_FROM
TEST_NAME=$UUID_TEST_NAME
VM_TO=$UUID_VM_TO

if [[ $TESTS == "legacy" ]]; then
    STORAGE=$LEGACY_STORAGE
    VM_FROM=$LEGACY_VM_FROM
    TEST_NAME=$LEGACY_TEST_NAME
fi

echo "- named image $TESTS"
test_named_image "$STORAGE" "$VM_FROM" "$TEST_NAME"

echo "- unnamed image $TESTS"
test_unnamed_image "$STORAGE" "$VM_FROM"

echo "- mv VM online $TESTS"
test_mv_vm "$VM_FROM" "$DESTINATION_NODE" 'yes' "$RUNNER_NODE"
test_mv_vm "$VM_FROM" "$RUNNER_NODE" 'yes' "$DESTINATION_NODE"

echo "- mv VM offline $TESTS"
test_mv_vm "$VM_FROM" "$DESTINATION_NODE" 'no' "$RUNNER_NODE"
test_mv_vm "$VM_FROM" "$RUNNER_NODE" 'no' "$DESTINATION_NODE"

echo "- create/rollback/delete snapshot $TESTS"
test_snapshot "$VM_FROM" "$SNAP_NAME"

if [[ $TESTS == uuid ]]; then
    echo "- moving disk UUID"
    test_rename_disk "$STORAGE" "$TEST_NAME" "$VM_FROM" "scsi7" "$VM_TO" "scsi8"
fi

echo "- clone VM $TESTS"
test_clone "$VM_FROM" "$CLONE_VMID"
