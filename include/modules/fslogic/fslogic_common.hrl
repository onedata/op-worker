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
-include_lib("annotations/include/annotations.hrl").

%% helper macro for not implemented functions
-define(NOT_IMPLEMENTED, erlang:error(not_yet_implemented)).

%% Common names
-define(SPACES_BASE_DIR_NAME, <<"spaces">>).
-define(DIRECTORY_SEPARATOR, "/").

%% Context definition
-record(fslogic_ctx, {
    session :: #session{},
    session_id :: session:id()
}).

%% Stub record
-record(space_info, {}).

%% root user definitions
-define(ROOT_USER_ID, <<"0">>).
-define(ROOT_SESS_ID, <<"0">>).

%% fslogic subscription id
-define(FSLOGIC_SUB_ID, binary:decode_unsigned(crypto:hash(md5, atom_to_binary(?MODULE, utf8))) rem 16#FFFFFFFFFFFF).

%% Deafult file modes

%% Mode for automatically created parent directory while creating file/directory.
-define(AUTO_CREATED_PARENT_DIR_MODE, 8#333).


-endif.