%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Onezone RPC api that should be sued by tests (the shouldn't directly
%%% call op as having everything in one place will help with refactoring).
%%% @end
%%%-------------------------------------------------------------------
-module(oz_test_rpc).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    cluster_time_seconds/1,

    get_user_protected_data/2,
    create_user_temporary_token/4,

    get_space_protected_data/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec cluster_time_seconds(node()) -> time_utils:seconds().
cluster_time_seconds(OzNode) ->
    rpc:call(OzNode, time_utils, cluster_time_seconds, []).


-spec get_user_protected_data(node(), od_user:id()) -> {ok, map()} | no_return().
get_user_protected_data(OzNode, UserId) ->
    ?assertMatch({ok, _}, rpc:call(OzNode, user_logic, get_protected_data, [
        ?ROOT, UserId
    ])).


-spec create_user_temporary_token(node(), aai:auth(), od_user:id(), map()) ->
    {ok, tokens:token()} | no_return().
create_user_temporary_token(OzNode, Auth, UserId, Data) ->
    ?assertMatch({ok, _}, rpc:call(OzNode, token_logic, create_user_temporary_token, [
        Auth, UserId, Data
    ])).


-spec get_space_protected_data(node(), od_space:id()) -> {ok, map()} | no_return().
get_space_protected_data(OzNode, SpaceId) ->
    ?assertMatch({ok, _}, rpc:call(OzNode, space_logic, get_protected_data, [
        ?ROOT, SpaceId
    ])).
