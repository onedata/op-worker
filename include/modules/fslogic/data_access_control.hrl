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


-define(has_all_flags(Bitmask, Flags), (((Bitmask) band (Flags)) =:= (Flags))).
-define(has_any_flags(Bitmask, Flags), (((Bitmask) band (Flags)) > 0)).
-define(set_flags(Bitmask, Flags), ((Bitmask) bor (Flags))).
-define(reset_flags(Bitmask, Flags), ((Bitmask) band (bnot (Flags)))).
-define(common_flags(Bitmask1, Bitmask2), ((Bitmask1) band (Bitmask2))).
-define(complement_flags(Bitmask), (bnot (Bitmask))).


% Access Requirements
-define(OWNERSHIP, ownership).
-define(PUBLIC_ACCESS, public_access).
-define(TRAVERSE_ANCESTORS, traverse_ancestors).
-define(OPERATIONS(Operations), {operations, Operations}).
-define(OPERATIONS(Operation1, Operation2), ?OPERATIONS(Operation1 bor Operation2)).
-define(OPERATIONS(Operation1, Operation2, Operation3),
    ?OPERATIONS(Operation1 bor Operation2 bor Operation3)
).
-define(OPERATIONS(Operation1, Operation2, Operation3, Operation4),
    ?OPERATIONS(Operation1 bor Operation2 bor Operation3 bor Operation4)
).
-define(OR(Requirement1, Requirement2), {Requirement1, 'or', Requirement2}).


% File protection flags
-define(DATA_PROTECTION_BIN, <<"data_protection">>).
-define(DATA_PROTECTION, 16#00000001).
-define(METADATA_PROTECTION_BIN, <<"metadata_protection">>).
-define(METADATA_PROTECTION, 16#00000002).


% Operations available in readonly mode
-define(READONLY_MODE_AVAILABLE_OPERATIONS, (
    ?read_attributes_mask bor
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask bor
    ?read_acl_mask bor
    ?traverse_container_mask
)).


% Operations forbidden by lack of ?SPACE_WRITE_DATA/?SPACE_READ_DATA space privilege
-define(SPACE_BLOCKED_WRITE_OPERATIONS, (
    ?write_attributes_mask bor
    ?write_object_mask bor
    ?add_object_mask bor
    ?add_subcontainer_mask bor
    ?delete_mask bor
    ?delete_child_mask bor
    ?write_metadata_mask bor
    ?write_acl_mask
)).
-define(SPACE_BLOCKED_READ_OPERATIONS, (
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask
)).

% Operations forbidden by file protection flags
-define(FILE_DATA_PROTECTION_BLOCKED_OPERATIONS, (
    ?write_object_mask bor
    ?add_object_mask bor
    ?add_subcontainer_mask bor
    ?delete_child_mask
)).
-define(FILE_METADATA_PROTECTION_BLOCKED_OPERATIONS, (
    ?write_attributes_mask bor
    ?write_metadata_mask bor
    ?write_acl_mask
)).

% Operations forbidden by dataset protection flags
-define(DATASET_DATA_PROTECTION_BLOCKED_OPERATIONS,
    % deletion should only be blocked in dataset subtree;
    % all file hardlinks outside of dataset are free to be deleted, as it does not change the file
    ?delete_mask
).


% Operations allowed by posix mode
-define(POSIX_ALWAYS_ALLOWED_OPS, (
    ?read_attributes_mask bor
    ?read_acl_mask
)).
-define(POSIX_READ_ONLY_OPS, (
    ?read_object_mask bor
    ?list_container_mask bor
    ?read_metadata_mask
)).
-define(POSIX_FILE_WRITE_ONLY_OPS, (
    ?write_object_mask bor
    ?write_attributes_mask bor
    ?write_metadata_mask
)).
-define(POSIX_DIR_WRITE_ONLY_OPS, (
    ?add_subcontainer_mask bor
    ?write_attributes_mask bor
    ?write_metadata_mask
)).
-define(POSIX_EXEC_ONLY_OPS, (
    ?traverse_container_mask
)).
-define(POSIX_FILE_WRITE_EXEC_OPS, (
    ?POSIX_FILE_WRITE_ONLY_OPS bor
    ?POSIX_EXEC_ONLY_OPS
)).
-define(POSIX_DIR_WRITE_EXEC_OPS, (
    ?POSIX_DIR_WRITE_ONLY_OPS bor
    ?POSIX_EXEC_ONLY_OPS bor

    % Special operations that are allowed only when both 'write' and 'exec'
    % mode bits are set
    ?add_object_mask bor
    ?delete_child_mask
)).


% Record holding information about the forbidden, allowed and denied operations
% specific user can perform on the file. It is build incrementally rather than
% at once as access control check consists of number of steps and not all must
% be completed to tell whether requested operations are allowed or denied.
% Those steps are:
% 1. operations availability (checking lack of space privileges and
%    file protection flags) check - step number `0`
% 2. depending on file active permissions type either:
%       a) posix mode check - step number `1`
%       b) acl check - each ACE in ACL has its own step number starting at `1`
-record(user_access_check_progress, {
    finished_step :: non_neg_integer(),
    % Operations forbidden for user by lack of space privileges or
    % data protection flags
    forbidden :: data_access_control:bitmask(),
    % Operations allowed/denied for user by posix mode/acl
    allowed :: data_access_control:bitmask(),
    denied :: data_access_control:bitmask()
}).

% Steps performed during access control checks
-define(OPERATIONS_AVAILABILITY_CHECK, 0).
-define(POSIX_MODE_CHECK, 1).
-define(ACL_CHECK(AceNo), AceNo).


-endif.
