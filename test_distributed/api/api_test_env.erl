%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Functions to get onenv created env configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(api_test_env).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([init/1]).
-export([
    resolve_entity_id/2,
    resolve_entity_name/2,

    get_provider_nodes/2,
    get_provider_eff_users/2,

    get_user_id/2,
    get_user_access_token/2,
    get_user_session_id/3
]).


-type entity_id() :: od_provider:id() | od_space:id() | od_user:id().
-type entity_name() :: atom().

-export_type([entity_id/0, entity_name/0]).


% Time caveat is required in temporary tokens, a default one is added if there isn't any
-define(DEFAULT_TEMP_CAVEAT_TTL, 360000).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(api_test_runner:config()) -> api_test_runner:config().
init(Config) ->
    [OzNode] = test_config:get_custom(Config, [oz_worker_nodes]),
    [ProvA, ProvB] = test_config:get_custom(Config, [providers]),

    {P1, P2} = case test_config:get_custom(Config, [provider_spaces, ProvA]) of
        [_] -> {ProvB, ProvA};
        [_, _] -> {ProvA, ProvB}
    end,

    EntityIdToName0 = #{P1 => p1, P2 => p2},

    ProvidersDesc = lists:foldl(fun({ProvCodeName, ProvId}, Acc) ->
        Acc#{ProvCodeName => #{
            id => ProvId,
            nodes => test_config:get_custom(Config, [provider_nodes, ProvId]),
            panel_nodes => test_config:get_custom(Config, [provider_panels, ProvId]),
            users => test_config:get_custom(Config, [users, ProvId])
        }}
    end, #{}, [{p1, P1}, {p2, P2}]),

    {UsersDesc, EntityIdToName1} = lists:foldl(fun(UserId, {UserDescAcc, EntityIdToNameAcc}) ->
        {ok, UserInfo} = oz_test_rpc:get_user_protected_data(OzNode, UserId),
        Username = binary_to_atom(maps:get(<<"username">>, UserInfo), utf8),
        AccessToken = create_oz_temp_access_token(OzNode, UserId),

        Sessions = lists:foldl(fun(Prov, Acc) ->
            case lists:member(UserId, kv_utils:get([Prov, users], ProvidersDesc)) of
                true ->
                    [Node] = kv_utils:get([Prov, nodes], ProvidersDesc),
                    Acc#{Prov => create_session(Node, UserId, AccessToken)};
                false ->
                    Acc
            end
        end, #{}, [p1, p2]),

        {
            UserDescAcc#{Username => #{
                id => UserId,
                access_token => AccessToken,
                sessions => Sessions,
                name => Username
            }},
            EntityIdToNameAcc#{UserId => Username}
        }
    end, {#{}, EntityIdToName0}, lists:usort(lists:flatten(maps:values(
        test_config:get_custom(Config, [users])
    )))),

    {SpacesDesc, EntityIdToName2} = lists:foldl(fun(SpaceId, {SpaceDescAcc, EntityIdToNameAcc}) ->
        {ok, SpaceInfo} = oz_test_rpc:get_space_protected_data(OzNode, SpaceId),
        SpaceName = binary_to_atom(maps:get(<<"name">>, SpaceInfo), utf8),

        {
            SpaceDescAcc#{SpaceName => #{
                id => SpaceId,
                name => SpaceName
            }},
            EntityIdToNameAcc#{SpaceId => SpaceName}
        }
    end, {#{}, EntityIdToName1}, lists:usort(lists:flatten(maps:values(
        test_config:get_custom(Config, [provider_spaces])
    )))),

    [{api_test_env, maps_utils:merge([
        ProvidersDesc,
        UsersDesc,
        SpacesDesc,
        #{entity_id_to_name_mapping => EntityIdToName2}
    ])} | Config].


-spec resolve_entity_id(entity_id() | entity_name(), api_test_runner:config()) ->
    entity_id().
resolve_entity_id(EntityName, Config) when is_atom(EntityName) ->
    kv_utils:get([api_test_env, EntityName, id], Config);
resolve_entity_id(EntityId, _Config) when is_binary(EntityId) ->
    EntityId.


-spec resolve_entity_name(entity_id() | entity_name(), api_test_runner:config()) ->
    entity_name().
resolve_entity_name(EntityName, _Config) when is_atom(EntityName) ->
    EntityName;
resolve_entity_name(EntityId, Config) when is_binary(EntityId) ->
    kv_utils:get([api_test_env, entity_id_to_name_mapping, EntityId], Config).


-spec get_provider_nodes(entity_id() | entity_name(), api_test_runner:config()) ->
    [node()].
get_provider_nodes(ProviderIdOrName, Config) ->
    ProviderName = resolve_entity_name(ProviderIdOrName, Config),
    kv_utils:get([api_test_env, ProviderName, nodes], Config).


-spec get_provider_eff_users(entity_id() | entity_name(), api_test_runner:config()) ->
    [od_user:id()].
get_provider_eff_users(ProviderIdOrName, Config) ->
    ProviderName = resolve_entity_name(ProviderIdOrName, Config),
    kv_utils:get([api_test_env, ProviderName, users], Config).


-spec get_user_id(entity_id() | entity_name(), api_test_runner:config()) ->
    od_user:id().
get_user_id(UserIdOrName, Config) ->
    UserName = resolve_entity_name(UserIdOrName, Config),
    kv_utils:get([api_test_env, UserName, id], Config).


-spec get_user_access_token(entity_id() | entity_name(), api_test_runner:config()) ->
    auth_manager:access_token().
get_user_access_token(UserIdOrName, Config) ->
    UserName = resolve_entity_name(UserIdOrName, Config),
    kv_utils:get([api_test_env, UserName, access_token], Config).


-spec get_user_session_id(
    UserIdOrName :: entity_id() | entity_name(),
    ProviderIdOrName :: entity_id() | entity_name(),
    api_test_runner:config()
) ->
    session:id().
get_user_session_id(UserIdOrName, ProviderIdOrName, Config) ->
    UserName = resolve_entity_name(UserIdOrName, Config),
    ProviderName = resolve_entity_name(ProviderIdOrName, Config),
    kv_utils:get([api_test_env, UserName, sessions, ProviderName], Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
local_ip_v4() ->
    {ok, Addrs} = inet:getifaddrs(),
    hd([
        Addr || {_, Opts} <- Addrs, {addr, Addr} <- Opts,
        size(Addr) == 4, Addr =/= {127,0,0,1}
    ]).
