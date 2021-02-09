%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015, ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module provides fslogic access control list definitions, such as
%%% bitmasks and names for common flags
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FSLOGIC_ACL_HRL).
-define(FSLOGIC_ACL_HRL, 1).

-define(ACL_XATTR_NAME, <<"cdmi_acl">>).

% Returns true if Bitmask contains all bits from Flag
-define(has_flag(Bitmask, Flag), ((Bitmask band Flag) =:= Flag)).

% ace types
-define(allow, <<"ALLOW">>).
-define(allow_mask, 16#00000000).
-define(deny, <<"DENY">>).
-define(deny_mask, 16#00000001).

% ace who identifiers
-define(owner, <<"OWNER@">>).
-define(group, <<"GROUP@">>).
-define(everyone, <<"EVERYONE@">>).

% ace flags
-define(no_flags, <<"NO_FLAGS">>).
-define(no_flags_mask, 16#00000000).
-define(identifier_group, <<"IDENTIFIER_GROUP">>).
-define(identifier_group_mask, 16#00000040).

% ace access masks
-define(read_object, <<"READ_OBJECT">>).
-define(read_object_mask, 16#00000001).
-define(list_container, <<"LIST_CONTAINER">>).
-define(list_container_mask, 16#00000001).
-define(write_object, <<"WRITE_OBJECT">>).
-define(write_object_mask, 16#00000002).
-define(add_object, <<"ADD_OBJECT">>).
-define(add_object_mask, 16#00000002).
-define(add_subcontainer, <<"ADD_SUBCONTAINER">>).
-define(add_subcontainer_mask, 16#00000004).
-define(read_metadata, <<"READ_METADATA">>).
-define(read_metadata_mask, 16#00000008).
-define(write_metadata, <<"WRITE_METADATA">>).
-define(write_metadata_mask, 16#00000010).
-define(traverse_container, <<"TRAVERSE_CONTAINER">>).
-define(traverse_container_mask, 16#00000020).
-define(delete_object, <<"DELETE_OBJECT">>).
-define(delete_subcontainer, <<"DELETE_SUBCONTAINER">>).
-define(delete_child_mask, 16#00000040).
-define(read_attributes, <<"READ_ATTRIBUTES">>).
-define(read_attributes_mask, 16#00000080).
-define(write_attributes, <<"WRITE_ATTRIBUTES">>).
-define(write_attributes_mask, 16#00000100).
-define(delete, <<"DELETE">>).
-define(delete_mask, 16#00010000).
-define(read_acl, <<"READ_ACL">>).
-define(read_acl_mask, 16#00020000).
-define(write_acl, <<"WRITE_ACL">>).
-define(write_acl_mask, 16#00040000).

% custom access flags
-define(read_all_object, <<"READ_ALL_OBJECT">>).
-define(read_all_object_mask, (
    ?read_object_mask bor
    ?read_metadata_mask bor
    ?read_attributes_mask bor
    ?read_acl_mask
)).
-define(read_all_container, <<"READ_ALL_CONTAINER">>).
-define(read_all_container_mask, (
    ?list_container_mask bor
    ?read_metadata_mask bor
    ?read_attributes_mask bor
    ?read_acl_mask
)).
-define(read, <<"READ_ALL">>).
-define(read_mask, (?read_all_object_mask bor ?read_all_container_mask)).

-define(write_all_object, <<"WRITE_ALL_OBJECT">>).
-define(write_all_object_mask, (
    ?write_object_mask bor
    ?write_metadata_mask bor
    ?write_attributes_mask bor
    ?delete_mask bor
    ?write_acl_mask
)).
-define(write_all_container, <<"WRITE_ALL_CONTAINER">>).
-define(write_all_container_mask, (
    ?add_object_mask bor
    ?add_subcontainer_mask bor
    ?write_metadata_mask bor
    ?delete_child_mask bor
    ?delete_mask bor
    ?write_attributes_mask bor
    ?write_acl_mask
)).
-define(write, <<"WRITE_ALL">>).
-define(write_mask, (?write_all_object_mask bor ?write_all_container_mask)).

-define(rw_all_object, <<"RW_ALL_OBJECT">>).
-define(rw_all_object_mask, (
    ?read_all_object_mask bor
    ?write_all_object_mask
)).
-define(rw_all_container, <<"RW_ALL_CONTAINER">>).
-define(rw_all_container_mask, (
    ?read_all_container_mask bor
    ?write_all_container_mask
)).
-define(rw, <<"RW_ALL">>).
-define(rw_mask, (?rw_all_object_mask bor ?rw_all_container_mask)).

-define(all_object_perms, <<"ALL_OBJECT_PERMS">>).
-define(all_object_perms_mask, ?rw_all_object_mask).
-define(all_container_perms, <<"ALL_CONTAINER_PERMS">>).
-define(all_container_perms_mask, (
    ?rw_all_container_mask bor
    ?traverse_container_mask
)).
-define(all_perms, <<"ALL_PERMS">>).
-define(all_perms_mask, (?all_object_perms_mask bor ?all_container_perms_mask)).

-type user_id() :: binary().
-type group_id() :: binary().

-record(access_control_entity, {
    acetype :: ?allow_mask | ?deny_mask,
    aceflags = ?no_flags_mask :: ?no_flags_mask | ?identifier_group_mask,
    identifier :: user_id() | group_id(),
    name :: undefined | binary(),
    acemask :: non_neg_integer()
}).

-endif.
