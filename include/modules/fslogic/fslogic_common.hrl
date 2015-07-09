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

-include("modules/datastore/datastore.hrl").
-include_lib("annotations/include/annotations.hrl").

%% helper macro for not implemented functions
-define(NOT_IMPLEMENTED, erlang:error(not_yet_implemented)).

%% Common names
-define(SPACES_BASE_DIR_NAME, <<"spaces">>).
-define(DIRECTORY_SEPARATOR, "/").

%% Context definition
-record(fslogic_ctx, {
    session :: #session{}
}).

-record(space_info, {}).

-define(REGULAR_FILE_TYPE, 'REG').
-define(DIRECTORY_TYPE, 'DIR').
-define(LINK_TYPE, 'LNK').

-define(ROOT_USER_ID, <<"0">>).
-define(ROOT_SESS_ID, <<"0">>).

-endif.