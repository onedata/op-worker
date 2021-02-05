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
    setup_sessions/1,
    find_importing_provider/2,
    create_oz_temp_access_token/2
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec initialize(test_config:config()) -> test_config:config().
initialize(Config) ->
    NewConfig = oct_background:prepare_base_test_config(Config),
    setup_sessions(NewConfig).


-spec setup_sessions(test_config:config()) -> test_config:config().
setup_sessions(Config) ->
    ProviderUsers = lists:foldl(fun(ProviderId, Acc) ->
        Acc#{ProviderId => oct_background:get_provider_eff_users(ProviderId)}
    end, #{}, oct_background:get_provider_ids()),

    NodesPerProvider = lists:foldl(fun(ProviderId, Acc) ->
        Acc#{ProviderId => oct_background:get_provider_nodes(ProviderId)}
    end, #{}, oct_background:get_provider_ids()),

    [OzNode | _] = test_config:get_all_oz_worker_nodes(Config),
    Sessions = maps:map(fun(ProviderId, Users) ->
        [Node | _] = maps:get(ProviderId, NodesPerProvider),
        lists:map(fun(UserId) ->
            {ok, SessId} = setup_user_session(UserId, OzNode, Node),
            {UserId, SessId}
        end, Users)
    end, ProviderUsers),

    test_config:set_many(Config, [[sess_id, Sessions]]).


-spec find_importing_provider(test_config:config(), od_space:id()) -> od_provider:id() | undefined.
find_importing_provider(Config, SpaceId) ->
    Providers = [oct_background:get_provider_id(krakow), oct_background:get_provider_id(paris)],
    lists:foldl(fun
        (ProviderId, undefined) ->
            [OpNode | _] = oct_background:get_provider_nodes(ProviderId),
            {ok, StorageId} = rpc:call(OpNode, space_logic, get_local_supporting_storage, [SpaceId]),
            case rpc:call(OpNode, storage, is_imported, [StorageId]) of
                true -> ProviderId;
                false -> undefined
            end;
        (_ProviderId, ImportingProviderId) ->
            ImportingProviderId
    end, undefined, Providers).


-spec create_oz_temp_access_token(node(), UserId :: binary()) -> tokens:serialized().
create_oz_temp_access_token(OzwNode, UserId) ->
    TimeCaveat = #cv_time{
        valid_until = rpc:call(OzwNode, global_clock, timestamp_seconds, []) + 100000
    },

    {ok, AccessToken} = rpc:call(OzwNode, token_logic, create_user_temporary_token, [
        ?ROOT, UserId, #{<<"caveats">> => [TimeCaveat]}
    ]),
    {ok, SerializedAccessToken} = tokens:serialize(AccessToken),

    SerializedAccessToken.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec setup_user_session(UserId :: binary(), OzwNode :: node(), OpwNode :: node()) ->
    {ok, SessId :: binary()}.
setup_user_session(UserId, OzwNode, OpwNode) ->
    AccessToken = create_oz_temp_access_token(OzwNode, UserId),
    Nonce = base64:encode(crypto:strong_rand_bytes(8)),
    Identity = ?SUB(user, UserId),
    Credentials =
        rpc:call(OpwNode, auth_manager, build_token_credentials,
            [AccessToken, undefined, undefined, undefined, allow_data_access_caveats]),

    rpc:call(OpwNode, session_manager, reuse_or_create_fuse_session, [Nonce, Identity, Credentials]).
