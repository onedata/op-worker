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

%% Remove after protocol impl.
-include("modules/fslogic/fslogic_mock.hrl").
-include("modules/datastore/datastore_models.hrl").

%% helper macro for not implemented functions
-define(NOT_IMPLEMENTED, erlang:error(not_yet_implemented)).

%% Common names
-define(SPACES_BASE_DIR_NAME, <<"spaces">>).
-define(DIRECTORY_SEPARATOR, <<"/">>).

%% Context definition
-record(fslogic_ctx, {
    session :: #session{}
}).

-endif.