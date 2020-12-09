%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common defines for fslogic.
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").

-ifndef(FSLOGIC_COMMON_HRL).
-define(FSLOGIC_COMMON_HRL, 1).

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/aai/aai.hrl").

%% Hidden file prefix
-define(HIDDEN_FILE_PREFIX, ".__onedata__").

%% Directory name for deleted opened files
-define(DELETED_OPENED_FILES_DIR, <<?HIDDEN_FILE_PREFIX, "deleted">>).
-define(DELETED_OPENED_FILES_DIR_STRING, binary_to_list(?DELETED_OPENED_FILES_DIR)).

% Global root - parent of all spaces
% note: spaces are also linked to virtual root directories of each user belonging to space
-define(GLOBAL_ROOT_DIR_UUID, <<"">>).
-define(GLOBAL_ROOT_DIR_NAME, <<"">>).
%% root user definitions
-define(ROOT_CREDENTIALS, root_credentials).
-define(ROOT_USER_ID, <<"0">>).
-define(ROOT_IDENTITY, ?SUB(root, ?ROOT_USER_ID)).
-define(ROOT_SESS_ID, <<"0">>).
%% posix compatible display credentials of root user
-define(ROOT_UID, 0).
-define(ROOT_GID, 0).

%% guest user definitions
-define(GUEST_CREDENTIALS, guest_credentials).
-define(GUEST_USER_ID, <<"nobody">>).
-define(GUEST_IDENTITY, ?SUB(nobody, ?GUEST_USER_ID)).
-define(GUEST_SESS_ID, <<"nobody">>).
-define(GUEST, #auth{subject = ?GUEST_IDENTITY, session_id = ?GUEST_SESS_ID}).

%% Virtual space owner
-define(SPACE_OWNER_PREFIX_STR, "VIRTUAL_SPACE_OWNER_").
-define(SPACE_OWNER_ID(SpaceId), <<?SPACE_OWNER_PREFIX_STR, SpaceId/binary>>).

%% Default permissions for regular files
-define(DEFAULT_FILE_PERMS, 8#664).
-define(DEFAULT_FILE_MODE, ?DEFAULT_FILE_PERMS bor 8#100000).

%% Default permissions for directories
-define(DEFAULT_DIR_PERMS, 8#775).
-define(DEFAULT_DIR_MODE, ?DEFAULT_DIR_PERMS bor 8#40000).

%% Mode of deleted opened files directory
-define(DELETED_OPENED_FILES_DIR_MODE, 8#700).

%% Allowed parameter keys
-define(PROXYIO_PARAMETER_HANDLE_ID, <<"handle_id">>).
-define(PROXYIO_PARAMETER_FILE_GUID, <<"file_uuid">>).

%% Macros for file lifecycle
-define(FILE_EXISTS, exists).
-define(FILE_DELETED, deleted).
-define(FILE_NEVER_EXISTED, never_existed).

% Maximal uid value in common linux distributions
-define(UID_MAX, 2147483647).

% Special values masking file real uid/gid in share mode as agreed with Oneclient
% developers (they can `case` on such values to perform some special actions).
-define(SHARE_UID, ?UID_MAX-1).
-define(SHARE_GID, ?SHARE_UID).

% Trash associated macros
-define(TRASH_DIR_NAME, <<".trash">>).
-define(TRASH_DIR_PERMS, 8#555).

-endif.