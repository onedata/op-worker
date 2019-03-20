%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains common function used across logic tests and gs_channel
%%% tests.
%%% @end
%%%--------------------------------------------------------------------
-module(logic_tests_common).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").
-include("http/gui_paths.hrl").

%% API
-export([
    init_per_testcase/1,
    get_user_session/2,
    mock_gs_client/1, unmock_gs_client/1,
    wait_for_mocked_connection/1,
    create_user_session/2,
    count_reqs/2,
    invalidate_cache/3,
    invalidate_all_test_records/1
]).


init_per_testcase(Config) ->
    wait_for_mocked_connection(Config),

    invalidate_all_test_records(Config),

    UserSessions = #{
        ?USER_1 => create_user_session(Config, ?USER_1),
        ?USER_2 => create_user_session(Config, ?USER_2),
        ?USER_3 => create_user_session(Config, ?USER_3)
    },

    [{sessions, UserSessions} | Config].


get_user_session(Config, UserId) ->
    maps:get(UserId, proplists:get_value(sessions, Config, #{})).


mock_gs_client(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Nodes, gs_client, []),
    ok = test_utils:mock_new(Nodes, provider_logic, [passthrough]),
    ok = test_utils:mock_new(Nodes, macaroon, [passthrough]),
    ok = test_utils:mock_new(Nodes, onedata_macaroons, [passthrough]),
    ok = test_utils:mock_new(Nodes, oneprovider, [passthrough]),
    ok = test_utils:mock_expect(Nodes, gs_client, start_link, fun mock_start_link/5),
    ok = test_utils:mock_expect(Nodes, gs_client, sync_request, fun mock_sync_request/2),

    GetProviderId = fun() ->
        ?DUMMY_PROVIDER_ID
    end,

    ok = test_utils:mock_expect(Nodes, oneprovider, get_id, GetProviderId),
    ok = test_utils:mock_expect(Nodes, oneprovider, get_id_or_undefined, GetProviderId),

    initializer:mock_provider_id(
        Nodes, ?PROVIDER_1, ?MOCK_PROVIDER_AUTH_MACAROON(?PROVIDER_1), ?MOCK_PROVIDER_IDENTITY_MACAROON(?PROVIDER_1)
    ),

    % gs_client requires successful setting of subdomain delegation IPs, but it cannot
    % be achieved in test environment
    ok = test_utils:mock_expect(Nodes, provider_logic, update_subdomain_delegation_ips, fun() ->
        ok
    end),
    ok = test_utils:mock_expect(Nodes, provider_logic, assert_zone_compatibility, fun() ->
        ok
    end),

    % Mock macaroons handling
    ok = test_utils:mock_expect(Nodes, onedata_macaroons, serialize, fun(M) ->
        {ok, M}
    end),
    ok = test_utils:mock_expect(Nodes, onedata_macaroons, deserialize, fun(M) ->
        {ok, M}
    end),
    ok = test_utils:mock_expect(Nodes, macaroon, prepare_for_request, fun(_, DM) ->
        DM
    end),

    % Fetch dummy provider so it is cached and does not generate Graph Sync requests.
    rpc:call(hd(Nodes), provider_logic, get, []).




unmock_gs_client(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, [gs_client, provider_logic, macaroon, onedata_macaroons, oneprovider]),
    initializer:unmock_provider_ids(Nodes),
    ok.


wait_for_mocked_connection(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    % Modify env variables to ensure frequent reconnect attempts
    [test_utils:set_env(N, ?APP_NAME, graph_sync_healthcheck_interval, 1000) || N <- Nodes],
    [test_utils:set_env(N, ?APP_NAME, graph_sync_reconnect_backoff_rate, 1) || N <- Nodes],
    [test_utils:set_env(N, ?APP_NAME, graph_sync_reconnect_max_backoff, 1000) || N <- Nodes],
    % Use graph sync path that allows correct mocked connection
    [test_utils:set_env(N, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_CORRECT_CONNECTION) || N <- Nodes],
    [Node | _] = ?config(op_worker_nodes, Config),

    CheckConnection = fun() ->
        case rpc:call(Node, global, whereis_name, [?GS_CLIENT_WORKER_GLOBAL_NAME]) of
            Pid when is_pid(Pid) -> ok;
            _ -> error
        end
    end,

    ?assertMatch(ok, CheckConnection(), 60).


create_user_session(Config, UserId) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    Auth = ?USER_INTERNAL_MACAROON_AUTH(UserId),
    {ok, #document{value = Identity}} = rpc:call(Node, user_identity, get_or_fetch, [Auth]),
    {ok, SessionId} = rpc:call(Node, session_manager, create_gui_session, [Identity, Auth]),
    % Make sure private user data is fetched (if user identity was cached, it might
    % not happen).
    rpc:call(Node, user_logic, get, [SessionId, ?SELF]),
    SessionId.


count_reqs(Config, Type) ->
    Nodes = ?config(op_worker_nodes, Config),
    RequestMatcher = case Type of
        rpc -> #gs_req{request = #gs_req_rpc{_ = '_'}, _ = '_'};
        graph -> #gs_req{request = #gs_req_graph{_ = '_'}, _ = '_'};
        unsub -> #gs_req{request = #gs_req_unsub{_ = '_'}, _ = '_'}
    end,
    lists:sum(
        [rpc:call(N, meck, num_calls, [gs_client, sync_request, ['_', RequestMatcher]]) || N <- Nodes]
    ).


invalidate_cache(Config, Type, Id) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    rpc:call(Node, gs_client_worker, invalidate_cache, [Type, Id]).


invalidate_all_test_records(Config) ->
    ToInvalidate = [
        {od_user, ?USER_1}, {od_user, ?USER_2},
        {od_group, ?GROUP_1}, {od_group, ?GROUP_2},
        {od_space, ?SPACE_1}, {od_space, ?SPACE_2},
        {od_share, ?SHARE_1}, {od_share, ?SHARE_2},
        {od_provider, ?PROVIDER_1}, {od_provider, ?PROVIDER_2},
        {od_handle_service, ?HANDLE_SERVICE_1}, {od_handle_service, ?HANDLE_SERVICE_2},
        {od_handle, ?HANDLE_1}, {od_handle, ?HANDLE_2},
        {od_harvester, ?HARVESTER_1}
    ],
    lists:foreach(
        fun({Type, Id}) ->
            logic_tests_common:invalidate_cache(Config, Type, Id)
        end, ToInvalidate).


%%%===================================================================
%%% gs_client module mocks
%%%===================================================================

% Path in address is used to control gs_client:start_link mock behaviour
mock_start_link(Address, _, _, _, _) ->
    case lists:suffix(?PATH_CAUSING_CONN_ERROR, Address) of
        true ->
            {error, nxdomain};
        false ->
            case lists:suffix(?PATH_CAUSING_NOBODY_IDENTITY, Address) of
                true ->
                    Pid = spawn(fun() ->
                        die_instantly % simulates gen_server getting {stop, normal}
                    end),
                    {ok, Pid, #gs_resp_handshake{identity = nobody}};
                false ->
                    true = lists:suffix(?PATH_CAUSING_CORRECT_CONNECTION, Address),
                    Pid = spawn_link(fun() ->
                        % Successful handshake - register under global name
                        % for test purposes
                        global:register_name(gs_client_mock, self()),
                        receive just_wait_infinitely -> ok end
                    end),
                    {ok, Pid, #gs_resp_handshake{identity = {provider, <<"mockProvId">>}}}
            end
    end.


mock_sync_request(ClientRef, GsRequest) ->
    case global:whereis_name(gs_client_mock) of
        ClientRef -> mock_sync_request(GsRequest);
        _ -> ?ERROR_NO_CONNECTION_TO_OZ
    end.


mock_sync_request(#gs_req{request = RPCReq = #gs_req_rpc{}}) ->
    case RPCReq of
        #gs_req_rpc{function = <<"authorizeUser">>, args = #{<<"identifier">> := ?MOCK_CAVEAT_ID}} ->
            {ok, #gs_resp_rpc{result = ?MOCK_DISCH_MACAROON}};
        _ ->
            ?ERROR_RPC_UNDEFINED
    end;
mock_sync_request(#gs_req{auth_override = Authorization, request = GraphReq = #gs_req_graph{}}) ->
    mock_graph_request(GraphReq, Authorization);
mock_sync_request(#gs_req{request = #gs_req_unsub{}}) ->
    ok.


mock_graph_request(GsGraph = #gs_req_graph{operation = create, data = Data}, Authorization) ->
    mock_graph_create(GsGraph#gs_req_graph.gri, Authorization, Data);
mock_graph_request(GsGraph = #gs_req_graph{operation = get, auth_hint = AuthHint}, Authorization) ->
    mock_graph_get(GsGraph#gs_req_graph.gri, Authorization, AuthHint);
mock_graph_request(GsGraph = #gs_req_graph{operation = update, data = Data}, Authorization) ->
    mock_graph_update(GsGraph#gs_req_graph.gri, Authorization, Data);
mock_graph_request(GsGraph = #gs_req_graph{operation = delete}, Authorization) ->
    mock_graph_delete(GsGraph#gs_req_graph.gri, Authorization).


mock_graph_create(#gri{type = od_user, id = UserId, aspect = {idp_access_token, IdP}}, ?USER_GS_MACAROON_AUTH(_UserId), _) ->
    case lists:member(UserId, [?USER_1, ?USER_2, ?USER_3]) andalso IdP == ?MOCK_IDP of
        true ->
            {ok, #gs_resp_graph{data_format = value, data = #{
                <<"token">> => ?MOCK_IDP_ACCESS_TOKEN, <<"ttl">> => 3600
            }}};
        _ ->
            ?ERROR_NOT_FOUND
    end;

mock_graph_create(#gri{type = od_share, id = undefined, aspect = instance}, ?USER_GS_MACAROON_AUTH(_UserId), Data) ->
    #{
        <<"shareId">> := ShareId,
        <<"name">> := _Name,
        <<"rootFileId">> := _RootFileId,
        <<"spaceId">> := SpaceId
    } = Data,
    case lists:member(SpaceId, [?SPACE_1, ?SPACE_2]) of
        true ->
            {ok, #gs_resp_graph{data_format = resource, data = ?SHARE_PRIVATE_DATA_VALUE(ShareId)}};
        _ ->
            ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"spaceId">>)
    end;

mock_graph_create(#gri{type = od_handle, id = undefined, aspect = instance}, ?USER_GS_MACAROON_AUTH(_UserId), Data) ->
    #{
        <<"handleServiceId">> := HandleServiceId,
        <<"resourceType">> := _ResourceType,
        <<"resourceId">> := _ResourceId,
        <<"metadata">> := _Metadata
    } = Data,
    case lists:member(HandleServiceId, [?HANDLE_SERVICE_1, ?HANDLE_SERVICE_2]) of
        true ->
            {ok, #gs_resp_graph{data_format = resource, data = ?HANDLE_PRIVATE_DATA_VALUE(?MOCK_CREATED_HANDLE_ID)}};
        _ ->
            ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"handleServiceId">>)
    end;

mock_graph_create(#gri{type = od_harvester, id = _, aspect = {entry, _}}, undefined, _Data) ->
    {ok, #gs_resp_graph{data_format = undefined}}.



mock_graph_update(#gri{type = od_share, id = _ShareId, aspect = instance}, ?USER_GS_MACAROON_AUTH(_UserId), Data) ->
    case Data of
        #{<<"name">> := Name} when is_binary(Name) ->
            {ok, #gs_resp_graph{}};
        _ ->
            ?ERROR_BAD_VALUE_BINARY(<<"name">>)
    end.


mock_graph_delete(#gri{type = od_share, id = ShareId, aspect = instance}, ?USER_GS_MACAROON_AUTH(_UserId)) ->
    case lists:member(ShareId, [?SHARE_1, ?SHARE_2]) of
        true ->
            {ok, #gs_resp_graph{}};
        false ->
            ?ERROR_NOT_FOUND
    end;
mock_graph_delete(#gri{type = od_harvester, id = _, aspect = {entry, _}}, undefined) ->
    {ok, #gs_resp_graph{}}.



mock_graph_get(GRI = #gri{type = od_user, id = Id, aspect = instance}, Authorization, AuthHint) ->
    UserId = case Id of
        ?SELF -> case Authorization of
            ?USER_GS_MACAROON_AUTH(ClientUserId) ->
                ClientUserId;
            _ ->
                ?ERROR_NOT_FOUND
        end;
        _ -> Id
    end,
    Authorized = case {Authorization, GRI#gri.scope, AuthHint} of
        {?USER_GS_MACAROON_AUTH(UserId), _, _} ->
            true;
        {?USER_GS_MACAROON_AUTH(ClientUser), shared, ?THROUGH_SPACE(_ThroughSpId)} ->
            lists:member(atom_to_binary(?SPACE_VIEW, utf8), maps:get(ClientUser, ?SPACE_EFF_USERS_VALUE(_ThroughSpId), [])) andalso
                maps:is_key(UserId, ?SPACE_EFF_USERS_VALUE(_ThroughSpId));
        {?USER_GS_MACAROON_AUTH(_OtherUser), _, _} ->
            false;
        % undefined Authorization means asking with provider's auth
        {undefined, private, _} ->
            false;
        {undefined, _, _} ->
            true
    end,
    case Authorized of
        true ->
            Data = case GRI#gri.scope of
                shared -> ?USER_SHARED_DATA_VALUE(UserId);
                protected -> ?USER_PROTECTED_DATA_VALUE(UserId);
                private -> ?USER_PRIVATE_DATA_VALUE(UserId)
            end,
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_group, id = GroupId, aspect = instance}, Authorization, AuthHint) ->
    Authorized = case {Authorization, GRI#gri.scope, AuthHint} of
        {?USER_GS_MACAROON_AUTH(UserId), shared, ?THROUGH_SPACE(_ThroughSpId)} ->
            lists:member(atom_to_binary(?SPACE_VIEW, utf8), maps:get(UserId, ?SPACE_EFF_USERS_VALUE(_ThroughSpId), [])) andalso
                maps:is_key(GroupId, ?SPACE_EFF_GROUPS_VALUE(_ThroughSpId));
        {?USER_GS_MACAROON_AUTH(UserId), shared, _} ->
            lists:member(GroupId, ?USER_EFF_GROUPS(UserId));
        {?USER_GS_MACAROON_AUTH(_UserId), _, _} ->
            false;
        % undefined Authorization means asking with provider's auth
        {undefined, shared, _} ->
            true
    end,
    case Authorized of
        true ->
            {ok, #gs_resp_graph{data_format = resource, data = ?GROUP_SHARED_DATA_VALUE(GroupId)}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_space, id = SpaceId, aspect = instance}, Authorization, _) ->
    Authorized = case {Authorization, GRI#gri.scope} of
        {?USER_GS_MACAROON_AUTH(UserId), private} ->
            lists:member(atom_to_binary(?SPACE_VIEW, utf8), maps:get(UserId, ?SPACE_EFF_USERS_VALUE(SpaceId), []));
        {?USER_GS_MACAROON_AUTH(UserId), protected} ->
            maps:is_key(UserId, ?SPACE_EFF_USERS_VALUE(SpaceId));
        % undefined Authorization means asking with provider's auth
        {undefined, _} ->
            true
    end,
    case Authorized of
        true ->
            Data = case GRI#gri.scope of
                protected -> ?SPACE_PROTECTED_DATA_VALUE(SpaceId);
                private -> ?SPACE_PRIVATE_DATA_VALUE(SpaceId)
            end,
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_share, id = ShareId, aspect = instance}, Authorization, _) ->
    Authorized = case {Authorization, GRI#gri.scope} of
        {?USER_GS_MACAROON_AUTH(UserId), private} ->
            maps:is_key(UserId, ?SPACE_EFF_USERS_VALUE(?SHARE_SPACE(ShareId)));
        % undefined Authorization means asking with provider's auth
        {undefined, private} ->
            % Provider is allowed to ask for shares of supported spaces
            true;
        {_, public} ->
            true
    end,
    case Authorized of
        true ->
            Data = case GRI#gri.scope of
                public -> ?SHARE_PUBLIC_DATA_VALUE(ShareId);
                private -> ?SHARE_PRIVATE_DATA_VALUE(ShareId)
            end,
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_provider, id = ProviderId, aspect = instance}, Authorization, _) ->
    Authorized = case {Authorization, GRI#gri.scope} of
        {?USER_GS_MACAROON_AUTH(_UserId), private} ->
            false;
        {?USER_GS_MACAROON_AUTH(UserId), protected} ->
            lists:member(UserId, ?PROVIDER_EFF_USERS(ProviderId));
        % undefined Authorization means asking with provider's auth
        {undefined, _} ->
            true
    end,
    case Authorized of
        true ->
            Data = case GRI#gri.scope of
                protected -> ?PROVIDER_PROTECTED_DATA_VALUE(ProviderId);
                private -> ?PROVIDER_PRIVATE_DATA_VALUE(ProviderId)
            end,
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(#gri{type = od_handle_service, id = HServiceId, aspect = instance, scope = private}, Authorization, _) ->
    Authorized = case Authorization of
        ?USER_GS_MACAROON_AUTH(UserId) ->
            lists:member(atom_to_binary(?HANDLE_SERVICE_VIEW, utf8), maps:get(UserId, ?HANDLE_SERVICE_EFF_USERS_VALUE(HServiceId), []));
        % undefined Authorization means asking with provider's auth
        undefined ->
            false
    end,
    case Authorized of
        true ->
            {ok, #gs_resp_graph{data_format = resource, data = ?HANDLE_SERVICE_PRIVATE_DATA_VALUE(HServiceId)}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_handle, id = HandleId, aspect = instance}, Authorization, _) ->
    Authorized = case {Authorization, GRI#gri.scope} of
        {?USER_GS_MACAROON_AUTH(UserId), private} ->
            lists:member(atom_to_binary(?HANDLE_VIEW, utf8), maps:get(UserId, ?HANDLE_EFF_USERS_VALUE(HandleId), []));
        % undefined Authorization means asking with provider's auth
        {undefined, private} ->
            false;
        {_, public} ->
            true
    end,
    case Authorized of
        true ->
            Data = case GRI#gri.scope of
                public -> ?HANDLE_PUBLIC_DATA_VALUE(HandleId);
                private -> ?HANDLE_PRIVATE_DATA_VALUE(HandleId)
            end,
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_harvester, id = SpaceId, aspect = instance}, Authorization, _) ->
    Authorized = case {Authorization, GRI#gri.scope} of
        {undefined, protected} ->
            true;
        {?USER_GS_MACAROON_AUTH(_), protected} ->
            false
    end,
    case Authorized of
        true ->
            Data = case GRI#gri.scope of
                protected -> ?HARVESTER_PROTECTED_DATA_VALUE(SpaceId)
            end,
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end.