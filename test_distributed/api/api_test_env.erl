%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles initialization of environment created using onenv and
%%% provides utility functions to get information about this environment.
%%%
%%%                         !!!ATTENTION !!!
%%% To inspect the internal structure of environment description (created by
%%% this module) it would be enough to add `ct:pal("CONFIG: ~p", [Config])`
%%% at the beginning of any api test and run it - description will be available
%%% under `api_test_env` key.
%%% @end
%%%-------------------------------------------------------------------
-module(api_test_env).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([init_per_suite/2]).
-export([
    to_entity_id/2,
    to_entity_placeholder/2,

    get_provider_id/2,
    get_provider_nodes/2,
    get_provider_eff_users/2,

    get_space_id/2,

    get_user_id/2,
    get_user_access_token/2,
    get_user_session_id/3
]).


-type entity_id() :: od_provider:id() | od_space:id() | od_user:id().
-type placeholder() :: atom().
-type onenv_test_config() :: #onenv_test_config{}.

-export_type([placeholder/0, entity_id/0]).


% Time caveat is required in temporary tokens, a default one is added if there isn't any
-define(DEFAULT_TEMP_CAVEAT_TTL, 360000).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_per_suite(api_test_runner:config(), onenv_test_config()) ->
    api_test_runner:config().
init_per_suite(Config, #onenv_test_config{
    onenv_scenario = Scenario,
    envs = Envs,
    posthook = CustomPostHook
}) ->
    AddEnvs = lists:map(fun({Service, Application, CustomEnv}) ->
        {add_envs, [Service, Application, CustomEnv]}
    end, Envs),

    test_config:set_many(Config, AddEnvs ++ [
        {set_onenv_scenario, [Scenario]},
        {set_posthook, fun(NewConfig) ->
            CustomPostHook(
                add_onenv_desc_to_config(
                    onenv_test_utils:prepare_base_test_config(NewConfig)
                )
            )
        end}
    ]).


-spec to_entity_id(entity_id() | placeholder(), api_test_runner:config()) ->
    entity_id().
to_entity_id(EntityPlaceholder, Config) when is_atom(EntityPlaceholder) ->
    kv_utils:get([api_test_env, EntityPlaceholder, id], Config);
to_entity_id(EntityId, _Config) when is_binary(EntityId) ->
    EntityId.


-spec to_entity_placeholder(entity_id() | placeholder(), api_test_runner:config()) ->
    placeholder().
to_entity_placeholder(Placeholder, _Config) when is_atom(Placeholder) ->
    Placeholder;
to_entity_placeholder(EntityId, Config) when is_binary(EntityId) ->
    kv_utils:get([api_test_env, entity_id_to_placeholder_mapping, EntityId], Config).


-spec get_provider_id(od_provider:id() | placeholder(), api_test_runner:config()) ->
    od_provider:id().
get_provider_id(ProviderIdOrPlaceholder, Config) ->
    ProviderPlaceholder = to_entity_placeholder(ProviderIdOrPlaceholder, Config),
    kv_utils:get([api_test_env, ProviderPlaceholder, id], Config).


-spec get_provider_nodes(od_provider:id() | placeholder(), api_test_runner:config()) ->
    [node()].
get_provider_nodes(ProviderIdOrPlaceholder, Config) ->
    ProviderPlaceholder = to_entity_placeholder(ProviderIdOrPlaceholder, Config),
    kv_utils:get([api_test_env, ProviderPlaceholder, nodes], Config).


-spec get_provider_eff_users(od_provider:id() | placeholder(), api_test_runner:config()) ->
    [od_user:id()].
get_provider_eff_users(ProviderIdOrPlaceholder, Config) ->
    ProviderPlaceholder = to_entity_placeholder(ProviderIdOrPlaceholder, Config),
    kv_utils:get([api_test_env, ProviderPlaceholder, users], Config).


-spec get_space_id(od_space:id() | placeholder(), api_test_runner:config()) ->
    od_space:id().
get_space_id(SpaceIdOrPlaceholder, Config) ->
    SpacePlaceholder = to_entity_placeholder(SpaceIdOrPlaceholder, Config),
    kv_utils:get([api_test_env, SpacePlaceholder, id], Config).


-spec get_user_id(od_user:id() | placeholder(), api_test_runner:config()) ->
    od_user:id().
get_user_id(UserIdOrPlaceholder, Config) ->
    UserPlaceholder = to_entity_placeholder(UserIdOrPlaceholder, Config),
    kv_utils:get([api_test_env, UserPlaceholder, id], Config).


-spec get_user_access_token(od_user:id() | placeholder(), api_test_runner:config()) ->
    auth_manager:access_token().
get_user_access_token(UserIdOrPlaceholder, Config) ->
    UserPlaceholder = to_entity_placeholder(UserIdOrPlaceholder, Config),
    kv_utils:get([api_test_env, UserPlaceholder, access_token], Config).


-spec get_user_session_id(
    UserIdOrPlaceholder :: od_user:id() | placeholder(),
    ProviderIdOrPlaceholder :: od_provider:id() | placeholder(),
    api_test_runner:config()
) ->
    session:id().
get_user_session_id(UserIdOrPlaceholder, ProviderIdOrPlaceholder, Config) ->
    UserPlaceholder = to_entity_placeholder(UserIdOrPlaceholder, Config),
    ProviderPlaceholder = to_entity_placeholder(ProviderIdOrPlaceholder, Config),
    kv_utils:get([api_test_env, UserPlaceholder, sessions, ProviderPlaceholder], Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec add_onenv_desc_to_config(api_test_runner:config()) -> api_test_runner:config().
add_onenv_desc_to_config(Config) ->
    {ProvidersDesc, ProviderIdToPlaceholder} = get_providers_desc(Config),
    {UsersDesc, UserIdToPlaceholder} = get_users_desc(ProvidersDesc, Config),
    {SpacesDesc, SpaceIdToPlaceholder} = get_spaces_desc(Config),

    EntityIdToPlaceholder = maps_utils:merge([
        ProviderIdToPlaceholder,
        UserIdToPlaceholder,
        SpaceIdToPlaceholder
    ]),

    [{api_test_env, maps_utils:merge([
        ProvidersDesc,
        UsersDesc,
        SpacesDesc,
        #{entity_id_to_placeholder_mapping => EntityIdToPlaceholder}
    ])} | Config].


%% @private
get_providers_desc(Config) ->
    [ProvA, ProvB] = test_config:get_custom(Config, [providers]),

    % Differentiate providers by number of supported spaces (see api_tests.yaml).
    {P1, P2} = case test_config:get_custom(Config, [provider_spaces, ProvA]) of
        [_] -> {ProvB, ProvA};
        [_, _] -> {ProvA, ProvB}
    end,

    ProviderIdToPlaceholder = #{P1 => p1, P2 => p2},

    ProvidersDesc = lists:foldl(fun({ProvPlaceholder, ProvId}, Acc) ->
        Acc#{ProvPlaceholder => #{
            id => ProvId,
            nodes => test_config:get_custom(Config, [provider_nodes, ProvId]),
            panel_nodes => test_config:get_custom(Config, [provider_panels, ProvId]),
            users => test_config:get_custom(Config, [users, ProvId])
        }}
    end, #{}, [{p1, P1}, {p2, P2}]),

    {ProvidersDesc, ProviderIdToPlaceholder}.


%% @private
get_users_desc(ProvidersDesc, Config) ->
    [OzNode] = test_config:get_custom(Config, [oz_worker_nodes]),

    lists:foldl(fun(UserId, {UserDescAcc, UserIdToPlaceholder}) ->
        {ok, UserInfo} = oz_test_rpc:get_user_protected_data(OzNode, UserId),
        Username = maps:get(<<"username">>, UserInfo),
        UserPlaceholder = binary_to_atom(Username, utf8),
        AccessToken = create_oz_temp_access_token(OzNode, UserId),

        Sessions = lists:foldl(fun({ProvPlaceholder, ProvDesc}, Acc) ->
            case lists:member(UserId, kv_utils:get([users], ProvDesc)) of
                true ->
                    [Node] = kv_utils:get([nodes], ProvDesc),
                    Acc#{ProvPlaceholder => create_session(Node, UserId, AccessToken)};
                false ->
                    Acc
            end
        end, #{}, maps:to_list(ProvidersDesc)),

        {
            UserDescAcc#{UserPlaceholder => #{
                id => UserId,
                access_token => AccessToken,
                sessions => Sessions,
                name => Username
            }},
            UserIdToPlaceholder#{UserId => UserPlaceholder}
        }
    end, {#{}, #{}}, lists:usort(lists:flatten(maps:values(
        test_config:get_custom(Config, [users])
    )))).


%% @private
get_spaces_desc(Config) ->
    [OzNode] = test_config:get_custom(Config, [oz_worker_nodes]),

    lists:foldl(fun(SpaceId, {SpaceDescAcc, SpaceIdToPlaceholder}) ->
        {ok, SpaceInfo} = oz_test_rpc:get_space_protected_data(OzNode, SpaceId),
        SpaceName = maps:get(<<"name">>, SpaceInfo),
        SpacePlaceholder = binary_to_atom(SpaceName, utf8),

        {
            SpaceDescAcc#{SpacePlaceholder => #{
                id => SpaceId,
                name => SpaceName
            }},
            SpaceIdToPlaceholder#{SpaceId => SpacePlaceholder}
        }
    end, {#{}, #{}}, lists:usort(lists:flatten(maps:values(
        test_config:get_custom(Config, [provider_spaces])
    )))).


%% @private
-spec create_oz_temp_access_token(node(), UserId :: binary()) -> tokens:serialized().
create_oz_temp_access_token(OzNode, UserId) ->
    Auth = ?USER(UserId),
    Now = oz_test_rpc:cluster_time_seconds(OzNode),

    {ok, Token} = oz_test_rpc:create_user_temporary_token(OzNode, Auth, UserId, #{
        <<"type">> => ?ACCESS_TOKEN,
        <<"caveats">> => [#cv_time{valid_until = Now + ?DEFAULT_TEMP_CAVEAT_TTL}]
    }),
    {ok, SerializedToken} = tokens:serialize(Token),
    SerializedToken.


%% @private
-spec create_session(node(), od_user:id(), tokens:serialized()) ->
    session:id().
create_session(Node, UserId, AccessToken) ->
    Nonce = crypto:strong_rand_bytes(10),
    Identity = ?SUB(user, UserId),
    TokenCredentials = auth_manager:build_token_credentials(
        AccessToken, undefined,
        local_ip_v4(), oneclient, allow_data_access_caveats
    ),
    {ok, SessionId} = op_test_rpc:create_fuse_session(Node, Nonce, Identity, TokenCredentials),
    SessionId.


%% @private
-spec local_ip_v4() -> inet:ip_address().
local_ip_v4() ->
    {ok, Addrs} = inet:getifaddrs(),
    hd([
        Addr || {_, Opts} <- Addrs, {addr, Addr} <- Opts,
        size(Addr) == 4, Addr =/= {127,0,0,1}
    ]).
