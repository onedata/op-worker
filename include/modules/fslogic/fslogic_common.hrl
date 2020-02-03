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
-include("modules/storage_file_manager/helpers/helpers.hrl").

%% helper macro for not implemented functions
-define(NOT_IMPLEMENTED, erlang:error(not_yet_implemented)).

%% Common names
-define(DIRECTORY_SEPARATOR, "/").
-define(DIRECTORY_SEPARATOR_BINARY, list_to_binary(?DIRECTORY_SEPARATOR)).

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
-define(ROOT_USER_ID, <<"0">>).
-define(ROOT_SESS_ID, <<"0">>).
-define(ROOT_AUTH, root_auth).
-define(GUEST_USER_ID, <<"nobody">>).
-define(GUEST_SESS_ID, <<"nobody">>).
-define(GUEST_AUTH, guest_auth).
-define(GUEST_IDENTITY, #user_identity{user_id = ?GUEST_USER_ID}).

-define(DEFAULT_SPACE_DIR_MODE, 8#775).

%% Mode for automatically created parent directory while creating file/directory.
-define(AUTO_CREATED_PARENT_DIR_MODE, 8#333).

%% Mode of deleted opened files directory
-define(DELETED_OPENED_FILES_DIR_MODE, 8#700).

% macros defining modes of file deletions
-define(LOCAL_DELETE, local_delete).
-define(REMOTE_DELETE, remote_delete).
-define(OPENED_FILE_DELETE, opened_file_delete).
-define(RELEASED_FILE_DELETE, released_file_delete).

%% Allowed parameter keys
-define(PROXYIO_PARAMETER_HANDLE_ID, <<"handle_id">>).
-define(PROXYIO_PARAMETER_FILE_GUID, <<"file_uuid">>).

-endif.