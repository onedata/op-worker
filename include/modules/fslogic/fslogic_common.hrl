%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Common defines for fslogic.
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").

-ifndef(FSLOGIC_COMMON_HRL).
-define(FSLOGIC_COMMON_HRL, 1).

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/helpers.hrl").

%% helper macro for not implemented functions
-define(NOT_IMPLEMENTED, erlang:error(not_yet_implemented)).

%% Common names
-define(DIRECTORY_SEPARATOR, "/").
-define(DIRECTORY_SEPARATOR_BINARY, list_to_binary(?DIRECTORY_SEPARATOR)).

%% Context definition
-record(fslogic_ctx, {
    session :: #session{},
    session_id :: session:id(),
    space_id :: file_meta:uuid()
}).

%% root user definitions
-define(ROOT_DIR_UUID, <<"">>).
-define(ROOT_DIR_NAME, <<"">>).
-define(ROOT_USER_ID, <<"0">>).
-define(ROOT_SESS_ID, <<"0">>).
-define(ROOT_SESS, #session{identity = #identity{user_id = ?ROOT_USER_ID}}).
-define(ROOT_POSIX_CTX, #posix_user_ctx{uid = 0, gid = 0}).

%% fslogic subscription id
-define(FSLOGIC_SUB_ID, binary:decode_unsigned(crypto:hash(md5, <<"fslogic">>)) rem 16#FFFFFFFFFFFF).

%% Deafult file modes

%% Mode for automatically created parent directory while creating file/directory.
-define(AUTO_CREATED_PARENT_DIR_MODE, 8#333).

%% Allowed parameter keys
-define(PROXYIO_PARAMETER_HANDLE_ID, <<"handle_id">>).
-define(PROXYIO_PARAMETER_FILE_UUID, <<"file_uuid">>).

-endif.