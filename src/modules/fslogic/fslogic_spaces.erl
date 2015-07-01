%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_spaces).
-author("Krzysztof Trzepla").
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").

%% API
-export([get_default_space/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns default space document.
%% @end
%%--------------------------------------------------------------------
-spec get_default_space(Ctx :: fslogic_worker:ctx()) -> {ok, datastore:document()} | datastore:get_error().
get_default_space(Ctx) ->
    UserId = fslogic_context:get_user_id(Ctx),
    {ok, #document{value = #onedata_user{space_ids = [DefaultSpaceId | _]}}} =
        onedata_user:get(UserId),
    file_meta:get({uuid, DefaultSpaceId}).

%%%===================================================================
%%% Internal functions
%%%===================================================================