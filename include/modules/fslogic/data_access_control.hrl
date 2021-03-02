%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021, ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions of macros and records used for:
%%% - defining access requirements for file,
%%% - checking if access to file is allowed for given user.
%%% @end
%%%-------------------------------------------------------------------
-author("Bartosz Walkowicz").

-ifndef(FSLOGIC_DATA_ACCESS_CONTROL_HRL).
-define(FSLOGIC_DATA_ACCESS_CONTROL_HRL, 1).

-include("modules/fslogic/acl.hrl").


% Access Requirements
-define(OWNERSHIP, ownership).
-define(PUBLIC_ACCESS, public_access).
-define(TRAVERSE_ANCESTORS, traverse_ancestors).
-define(PERMISSIONS(Perms), {permissions, Perms}).
-define(PERMISSIONS(Perm1, Perm2), ?PERMISSIONS(Perm1 bor Perm2)).
-define(PERMISSIONS(Perm1, Perm2, Perm3), ?PERMISSIONS(Perm1 bor Perm2 bor Perm3)).
-define(PERMISSIONS(Perm1, Perm2, Perm3, Perm4),
    ?PERMISSIONS(Perm1 bor Perm2 bor Perm3 bor Perm4)
).
-define(OR(Requirement1, Requirement2), {Requirement1, 'or', Requirement2}).


% Permissions respected in readonly mode (operation requiring any other permission,
% even if granted by posix mode or ACL, will be denied)
-define(READONLY_MODE_RESPECTED_PERMS, (
    ?read_attributes_mask bor
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask bor
    ?read_acl_mask bor
    ?traverse_container_mask
)).


% Permissions denied by lack of ?SPACE_WRITE_DATA/?SPACE_READ_DATA space privilege
-define(SPACE_DENIED_WRITE_PERMS, (
    ?write_attributes_mask bor
    ?write_object_mask bor
    ?add_object_mask bor
    ?add_subcontainer_mask bor
    ?delete_mask bor
    ?delete_child_mask bor
    ?write_metadata_mask bor
    ?write_acl_mask
)).
-define(SPACE_DENIED_READ_PERMS, (
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask
)).


% Permissions granted by posix mode
-define(POSIX_ALWAYS_GRANTED_PERMS, (
    ?read_attributes_mask bor
    ?read_acl_mask
)).
-define(POSIX_READ_ONLY_PERMS, (
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask
)).
-define(POSIX_FILE_WRITE_ONLY_PERMS, (
    ?write_object_mask bor
    ?write_attributes_mask bor
    ?write_metadata_mask
)).
-define(POSIX_DIR_WRITE_ONLY_PERMS, (
    ?add_subcontainer_mask bor
    ?write_attributes_mask bor
    ?write_metadata_mask
)).
-define(POSIX_EXEC_ONLY_PERMS, (
    ?traverse_container_mask
)).
-define(POSIX_FILE_WRITE_EXEC_PERMS, (
    ?POSIX_FILE_WRITE_ONLY_PERMS bor
    ?POSIX_EXEC_ONLY_PERMS
)).
-define(POSIX_DIR_WRITE_EXEC_PERMS, (
    ?POSIX_DIR_WRITE_ONLY_PERMS bor
    ?POSIX_EXEC_ONLY_PERMS bor

    % Special permissions that are granted only when both 'write' and 'exec'
    % mode bits are set
    ?add_object_mask bor
    ?delete_child_mask
)).


% Record holding information about the permissions to the file granted and
% denied for the given user. It is build incrementally rather than at once as
% permissions check consists of number of steps and not all must be completed
% to tell whether requested permissions are granted or denied. That is why it
% contains pointer to where it stopped (e.g. space privileges check or concrete
% ACE in ACL) so that build can be resumed if needed.
-record(user_perms_matrix, {
    finished_step :: non_neg_integer(),
    granted :: ace:bitmask(),
    denied :: ace:bitmask()
}).

% Steps performed during access control checks
-define(SPACE_PRIVILEGES_CHECK, 0).
-define(POSIX_MODE_CHECK, 1).
-define(ACL_CHECK(AceNo), AceNo).


-endif.
