%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This module contains test utility functions useful in tests using onenv.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_onenv_test_utils).
-author("Michal Stanisz").

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    initialize/1,
    setup_sessions/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec initialize(test_config:config()) -> test_config:config().
initialize(Config) ->
    NewConfig = onenv_test_utils:prepare_base_test_config(Config),
    setup_sessions(NewConfig).


-spec setup_sessions(test_config:config()) -> test_config:config().
setup_sessions(Config) ->
    ProviderUsers = test_config:get_custom(Config, users),
    NodesPerProvider = test_config:get_custom(Config, provider_nodes),
    
    [OzNode | _ ] = test_config:get_all_oz_worker_nodes(Config),
    Sessions = maps:map(fun(ProviderId, Users) ->
        [Node | _] = maps:get(ProviderId, NodesPerProvider),
        lists:map(fun(UserId) ->
            {ok, SessId} = setup_user_session(UserId, OzNode, Node),
            {UserId, SessId}
        end, Users)
    end, ProviderUsers),
    
    test_config:set_many(Config, [
        [sess_id, Sessions]
    ]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec setup_user_session(UserId :: binary(), OzwNode :: node(), OpwNode :: node()) ->
    {ok, SessId :: binary()}.
setup_user_session(UserId, OzwNode, OpwNode) ->
    TimeCaveat = #cv_time{valid_until = rpc:call(OzwNode, time_utils, cluster_time_seconds, []) + 100000},
    {ok, AccessToken} =
        rpc:call(OzwNode, token_logic, create_user_temporary_token,
            [?ROOT, UserId, #{<<"caveats">> => [TimeCaveat]}]),
    {ok, SerializedAccessToken} = rpc:call(OzwNode, tokens, serialize, [AccessToken]),
    Nonce = base64:encode(crypto:strong_rand_bytes(8)),
    Identity = ?SUB(user, UserId),
    Credentials =
        rpc:call(OpwNode, auth_manager, build_token_credentials,
            [SerializedAccessToken, undefined, undefined, undefined, allow_data_access_caveats]),
    
    rpc:call(OpwNode, session_manager, reuse_or_create_fuse_session, [Nonce, Identity, Credentials]).
