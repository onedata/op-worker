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

-include("../include/logic_tests_common.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/aai/aai.hrl").

%% API
-export([
    init_per_testcase/1,
    get_user_session/2,
    mock_gs_client/1, unmock_gs_client/1,
    set_envs_for_correct_connection/1,
    wait_for_mocked_connection/1,
    simulate_push/2,
    create_user_session/2,
    count_reqs/3,
    invalidate_cache/3,
    invalidate_all_test_records/1,
    mock_request_processing_time/3,
    mock_harvest_request_processing_time/3,
    set_request_timeout/2,
    set_harvest_request_timeout/2
]).

-define(NODES(Config), ?config(op_worker_nodes, Config)).


init_per_testcase(Config) ->
    wait_for_mocked_connection(Config),

    mock_request_processing_time(Config, 1, 20),
    mock_harvest_request_processing_time(Config, 1, 100),
    set_request_timeout(Config, 30000),
    set_harvest_request_timeout(Config, 120000),

    UserSessions = #{
        ?USER_1 => create_user_session(Config, ?USER_1),
        ?USER_2 => create_user_session(Config, ?USER_2),
        ?USER_3 => create_user_session(Config, ?USER_3),
        ?USER_INCREASING_REV => create_user_session(Config, ?USER_INCREASING_REV)
    },
    invalidate_all_test_records(Config),
    [{sessions, UserSessions} | Config].


get_user_session(Config, UserId) ->
    maps:get(UserId, proplists:get_value(sessions, Config, #{})).


mock_gs_client(Config) ->
    Nodes = ?NODES(Config),
    ok = test_utils:mock_new(Nodes, gs_client, []),
    ok = test_utils:mock_new(Nodes, provider_logic, [passthrough]),
    ok = test_utils:mock_new(Nodes, space_logic, [passthrough]),
    ok = test_utils:mock_new(Nodes, oneprovider, [passthrough]),
    ok = test_utils:mock_new(Nodes, rtransfer_config, [passthrough]),
    ok = test_utils:mock_expect(Nodes, gs_client, start_link, fun mock_start_link/5),
    ok = test_utils:mock_expect(Nodes, gs_client, async_request, fun mock_async_request/2),

    GetProviderId = fun() ->
        ?DUMMY_PROVIDER_ID
    end,

    ok = test_utils:set_env(Nodes, op_worker, oz_domain, ?DUMMY_ONEZONE_DOMAIN),
    ok = test_utils:mock_expect(Nodes, oneprovider, get_id, GetProviderId),
    ok = test_utils:mock_expect(Nodes, oneprovider, get_id_or_undefined, GetProviderId),

    initializer:mock_provider_id(
        Nodes, ?PROVIDER_1, ?MOCK_PROVIDER_ACCESS_TOKEN(?PROVIDER_1), ?MOCK_PROVIDER_IDENTITY_TOKEN(?PROVIDER_1)
    ),

    % mock Onezone version and compatibility registry to be the same as provider's
    ok = test_utils:mock_expect(Nodes, provider_logic, get_service_configuration, fun(onezone) ->
        Resolver = compatibility:build_resolver([node()], []),
        {ok, #{
            <<"version">> => op_worker:get_release_version(),
            <<"compatibilityRegistryRevision">> => element(2, {ok, _} = compatibility:peek_current_registry_revision(Resolver))
        }}
    end),
    ok = test_utils:mock_expect(Nodes, provider_logic, has_eff_user, fun(UserId) ->
        lists:member(UserId, [?USER_1, ?USER_2, ?USER_3, ?USER_INCREASING_REV])
    end),
    ok = test_utils:mock_expect(Nodes, provider_logic, has_storage, fun(_) ->
        true
    end),

    % adding dummy storages to rtransfer would fail
    ok = test_utils:mock_expect(Nodes, rtransfer_config, add_storages, fun() -> ok end),

    % Fetch dummy provider so it is cached and does not generate Graph Sync requests.
    utils:rpc_multicall(Nodes, provider_logic, get, []).


unmock_gs_client(Config) ->
    Nodes = ?NODES(Config),
    test_utils:mock_unload(Nodes, [gs_client, provider_logic, space_logic, oneprovider]),
    initializer:unmock_provider_ids(Nodes),
    ok.


set_envs_for_correct_connection(Config) ->
    Nodes = ?NODES(Config),
    % Modify env variables to ensure frequent reconnect attempts
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_reconnect_base_interval, 1000),
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_reconnect_backoff_rate, 1),
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_reconnect_max_backoff, 1000),
    % Use graph sync path that allows correct mocked connection
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_CORRECT_CONNECTION).


wait_for_mocked_connection(Config) ->
    set_envs_for_correct_connection(Config),
    Nodes = ?NODES(Config),
    CheckConnection = fun() ->
        case rpc:call(hd(Nodes), gs_client_worker, get_connection_pid, []) of
            Pid when is_pid(Pid) -> ok;
            _ -> error
        end
    end,
    ?assertMatch(ok, CheckConnection(), 60).


simulate_push(Config, PushMessage) when is_list(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    simulate_push(Node, PushMessage);
simulate_push(Node, PushMessage) when is_atom(Node) ->
    Pid = rpc:call(Node, gs_client_worker, process_push_message, [PushMessage]),
    WaitForCompletion = fun F() ->
        case rpc:call(Node, erlang, is_process_alive, [Pid]) of
            true ->
                timer:sleep(100),
                F();
            false ->
                ok
        end
    end,
    WaitForCompletion().


create_user_session(Config, UserId) ->
    [Node | _] = ?NODES(Config),

    AccessToken = initializer:create_access_token(UserId),
    TokenCredentials = auth_manager:build_token_credentials(
        AccessToken, undefined,
        initializer:local_ip_v4(), graphsync, disallow_data_access_caveats
    ),
    {ok, ?USER(UserId), _} = rpc:call(Node, auth_manager, verify_credentials, [TokenCredentials]),
    {ok, SessionId} = rpc:call(Node, session_manager, reuse_or_create_gui_session, [
        ?SUB(user, UserId), TokenCredentials
    ]),
    % Make sure private user data is fetched (if user identity was cached, it might
    % not happen).
    rpc:call(Node, user_logic, get, [SessionId, ?SELF]),
    SessionId.


count_reqs(Config, Type, GriMatcher) ->
    Nodes = ?NODES(Config),
    RequestMatcher = case Type of
        graph -> #gs_req{request = #gs_req_graph{gri = GriMatcher, _ = '_'}, _ = '_'};
        unsub -> #gs_req{request = #gs_req_unsub{gri = GriMatcher}, _ = '_'}
    end,
    lists:sum(
        [rpc:call(N, meck, num_calls, [gs_client, async_request, ['_', RequestMatcher]]) || N <- Nodes]
    ).


invalidate_cache(Config, Type, Id) ->
    [Node | _] = ?NODES(Config),
    rpc:call(Node, gs_client_worker, invalidate_cache, [Type, Id]).


invalidate_all_test_records(Config) ->
    ToInvalidate = [
        {od_user, ?USER_1}, {od_user, ?USER_2}, {od_user, ?USER_3}, {od_user, ?USER_INCREASING_REV},
        {od_group, ?GROUP_1}, {od_group, ?GROUP_2},
        {od_space, ?SPACE_1}, {od_space, ?SPACE_2},
        {od_share, ?SHARE_1}, {od_share, ?SHARE_2},
        {od_provider, ?PROVIDER_1}, {od_provider, ?PROVIDER_2},
        {od_handle_service, ?HANDLE_SERVICE_1}, {od_handle_service, ?HANDLE_SERVICE_2},
        {od_handle, ?HANDLE_1}, {od_handle, ?HANDLE_2},
        {od_harvester, ?HARVESTER_1},
        {od_token, ?TOKEN_1}, {od_token, ?TOKEN_2},
        {temporary_token_secret, ?USER_1}, {temporary_token_secret, ?USER_2}, {temporary_token_secret, ?USER_3}
    ],
    lists:foreach(
        fun({Type, Id}) ->
            logic_tests_common:invalidate_cache(Config, Type, Id)
        end, ToInvalidate).


% Below functions are used to control Graph Sync request processing time

% Called from test code (on testmaster node)
mock_request_processing_time(Config, MinTime, MaxTime) ->
    test_utils:set_env(
        ?NODES(Config), op_worker, mock_req_proc_time, {MinTime, MaxTime}
    ).

% Called from test code (on testmaster node)
mock_harvest_request_processing_time(Config, MinTime, MaxTime) ->
    test_utils:set_env(
        ?NODES(Config), op_worker, mock_harvest_req_proc_time, {MinTime, MaxTime}
    ).

% Called from the mock code (on a cluster node)
random_request_processing_time(Req) ->
    {MinTime, MaxTime} = case Req of
        #gs_req{request = #gs_req_graph{gri = #gri{type = od_space, aspect = harvest_metadata}}} ->
            op_worker:get_env(mock_harvest_req_proc_time, {1, 20});
        _ ->
            op_worker:get_env(mock_req_proc_time, {1, 20})
    end,
    MinTime + rand:uniform(MaxTime - MinTime + 1) - 1.


set_request_timeout(Config, Timeout) ->
    test_utils:set_env(
        ?NODES(Config), op_worker, graph_sync_request_timeout, Timeout
    ).


set_harvest_request_timeout(Config, Timeout) ->
    test_utils:set_env(
        ?NODES(Config), op_worker, graph_sync_harvest_metadata_request_timeout, Timeout
    ).


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
                    {ok, Pid, #gs_resp_handshake{identity = ?SUB(nobody)}};
                false ->
                    true = lists:suffix(?PATH_CAUSING_CORRECT_CONNECTION, Address),
                    CallerPid = self(),

                    Pid = spawn_link(fun() ->
                        % Successful handshake - register under global name for test purposes
                        yes = global:register_name(gs_client_mock, self()),
                        CallerPid ! registered,
                        receive just_wait_infinitely -> ok end
                    end),

                    receive registered ->
                        {ok, Pid, #gs_resp_handshake{identity = ?SUB(?ONEPROVIDER, <<"mockProvId">>)}}
                    after 5000 ->
                        exit(Pid, kill),
                        {error, timeout}
                    end
            end
    end.


mock_async_request(ClientRef, GsRequest = #gs_req{id = undefined}) ->
    mock_async_request(ClientRef, GsRequest#gs_req{id = datastore_key:new()});
mock_async_request(ClientRef, GsRequest = #gs_req{id = ReqId}) ->
    case global:whereis_name(gs_client_mock) of
        ClientRef ->
            CallerPid = self(),

            spawn(fun() ->
                Result = mock_request(GsRequest),
                timer:sleep(random_request_processing_time(GsRequest)),
                CallerPid ! {response, ReqId, Result}
            end),

            ReqId;
        _ ->
            error(noproc)
    end.


mock_request(#gs_req{request = #gs_req_rpc{}}) ->
    ?ERROR_RPC_UNDEFINED;
mock_request(#gs_req{auth_override = AuthOverride, request = GraphReq = #gs_req_graph{}}) ->
    mock_graph_request(GraphReq, AuthOverride);
mock_request(#gs_req{request = #gs_req_unsub{}}) ->
    {ok, #gs_resp_unsub{}}.


mock_graph_request(GsGraph = #gs_req_graph{operation = create, data = Data}, AuthOverride) ->
    mock_graph_create(GsGraph#gs_req_graph.gri, AuthOverride, Data);
mock_graph_request(GsGraph = #gs_req_graph{operation = get, auth_hint = AuthHint}, AuthOverride) ->
    mock_graph_get(GsGraph#gs_req_graph.gri, AuthOverride, AuthHint);
mock_graph_request(GsGraph = #gs_req_graph{operation = update, data = Data}, AuthOverride) ->
    mock_graph_update(GsGraph#gs_req_graph.gri, AuthOverride, Data);
mock_graph_request(GsGraph = #gs_req_graph{operation = delete}, AuthOverride) ->
    mock_graph_delete(GsGraph#gs_req_graph.gri, AuthOverride).


mock_graph_create(#gri{type = od_token, id = undefined, aspect = verify_access_token, scope = public}, _, Data) ->
    #{<<"token">> := Token} = Data,
    {ok, #token{subject = ?SUB(user, UserId)}} = tokens:deserialize(Token),
    {ok, #gs_resp_graph{data_format = value, data = #{
        <<"subject">> => aai:serialize_subject(?SUB(user, UserId)), <<"caveats">> => []
    }}};

mock_graph_create(#gri{type = od_user, id = UserId, aspect = {idp_access_token, IdP}}, #auth_override{client_auth = {token, _}}, _) ->
    case lists:member(UserId, [?USER_1, ?USER_2, ?USER_3, ?USER_INCREASING_REV]) andalso IdP == ?MOCK_IDP of
        true ->
            {ok, #gs_resp_graph{data_format = value, data = #{
                <<"token">> => ?MOCK_IDP_ACCESS_TOKEN, <<"ttl">> => 3600
            }}};
        _ ->
            ?ERROR_NOT_FOUND
    end;

mock_graph_create(#gri{type = od_share, id = undefined, aspect = instance}, #auth_override{client_auth = {token, _}}, Data) ->
    #{
        <<"shareId">> := ShareId,
        <<"description">> := _Description,
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

mock_graph_create(#gri{type = od_handle, id = undefined, aspect = instance}, #auth_override{client_auth = {token, _}}, Data) ->
    #{
        <<"handleServiceId">> := HandleServiceId,
        <<"resourceType">> := _ResourceType,
        <<"resourceId">> := _ResourceId,
        <<"metadata">> := _Metadata
    } = Data,
    case lists:member(HandleServiceId, [?HANDLE_SERVICE_1, ?HANDLE_SERVICE_2]) of
        true ->
            {ok, #gs_resp_graph{data_format = resource, data = ?HANDLE_PUBLIC_DATA_VALUE(?MOCK_CREATED_HANDLE_ID)}};
        _ ->
            ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"handleServiceId">>)
    end;
mock_graph_create(#gri{type = od_space, id = _, aspect = harvest_metadata}, undefined, _Data) ->
    {ok, #gs_resp_graph{data_format = undefined}}.

mock_graph_update(#gri{type = od_space, id = _, aspect = {support_parameters, _}}, undefined, _Data) ->
    {ok, #gs_resp_graph{}};
mock_graph_update(#gri{type = od_share, id = _ShareId, aspect = instance}, #auth_override{client_auth = {token, _}}, Data) ->
    NameArgCheck = case maps:find(<<"name">>, Data) of
        error ->
            none;
        {ok, Name} ->
            case is_binary(Name) of
                true -> ok;
                false -> bad
            end
    end,
    DescriptionArgCheck = case maps:find(<<"description">>, Data) of
        error ->
            none;
        {ok, Description} ->
            case is_binary(Description) of
                true -> ok;
                false -> bad
            end
    end,
    case {NameArgCheck, DescriptionArgCheck} of
        {bad, _} -> ?ERROR_BAD_VALUE_BINARY(<<"name">>);
        {_, bad} -> ?ERROR_BAD_VALUE_BINARY(<<"description">>);
        {none, none} -> ?ERROR_MISSING_AT_LEAST_ONE_VALUE([<<"description">>, <<"name">>]);
        _ -> {ok, #gs_resp_graph{}}
    end;
mock_graph_update(#gri{type = od_cluster, id = _ShareId, aspect = instance}, undefined, Data) ->
    % undefined Authorization means asking with provider's auth
    try
        #{<<"workerVersion">> := #{<<"gui">> := GuiHash}} = Data,
        case is_binary(GuiHash) of
            true -> {ok, #gs_resp_graph{}};
            false -> ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"workerVersion.gui">>)
        end
    catch _:_ ->
        ?ERROR_INTERNAL_SERVER_ERROR
    end.


mock_graph_delete(#gri{type = od_share, id = ShareId, aspect = instance}, #auth_override{client_auth = {token, _}}) ->
    case lists:member(ShareId, [?SHARE_1, ?SHARE_2]) of
        true ->
            {ok, #gs_resp_graph{}};
        false ->
            ?ERROR_NOT_FOUND
    end.


mock_graph_get(GRI = #gri{type = od_user, id = Id, aspect = instance}, AuthOverride, AuthHint) ->
    ClientUserId = case AuthOverride of
        undefined ->
            undefined;
        #auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)} ->
            token_to_user_id(SerializedToken)
    end,

    UserId = case Id of
        ?SELF ->
            case AuthOverride of
                #auth_override{client_auth = {token, _}} ->
                    ClientUserId;
                _ ->
                    ?ERROR_NOT_FOUND
            end;
        _ ->
            Id
    end,

    Authorized = case {ClientUserId, GRI#gri.scope, AuthHint} of
        % undefined ClientUserId means asking with provider's auth
        {undefined, private, _} ->
            false;
        {undefined, _, _} ->
            true;
        {UserId, _, _} ->
            true;
        {_, shared, ?THROUGH_SPACE(_ThroughSpaceId)} ->
            SpaceEffUsers = ?SPACE_EFF_USERS_VALUE(_ThroughSpaceId),
            UserPrivileges = maps:get(ClientUserId, SpaceEffUsers, []),
            lists:member(atom_to_binary(?SPACE_VIEW, utf8), UserPrivileges) andalso
                maps:is_key(UserId, SpaceEffUsers);
        {_, _, _} ->
            false
    end,
    case Authorized of
        true ->
            Data = case GRI#gri.scope of
                shared -> ?USER_SHARED_DATA_VALUE(UserId);
                protected -> ?USER_PROTECTED_DATA_VALUE(UserId);
                private -> ?USER_PRIVATE_DATA_VALUE(UserId)
            end,
            case UserId of
                ?USER_INCREASING_REV ->
                    % This user's revision rises with every fetch
                    critical_section:run(?USER_INCREASING_REV, fun() ->
                        Rev = op_worker:get_env(mock_inc_rev, 1),
                        op_worker:set_env(mock_inc_rev, Rev + 1),
                        {ok, #gs_resp_graph{data_format = resource, data = Data#{
                            <<"revision">> => Rev
                        }}}
                    end);
                _ ->
                    {ok, #gs_resp_graph{data_format = resource, data = Data}}
            end;
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_group, id = GroupId, aspect = instance}, AuthOverride, AuthHint) ->
    Authorized = case {AuthOverride, GRI#gri.scope, AuthHint} of
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)}, shared, ?THROUGH_SPACE(_ThroughSpId)} ->
            UserId = token_to_user_id(SerializedToken),
            UserPrivileges = maps:get(UserId, ?SPACE_EFF_USERS_VALUE(_ThroughSpId), []),
            lists:member(atom_to_binary(?SPACE_VIEW, utf8), UserPrivileges) andalso
                maps:is_key(GroupId, ?SPACE_EFF_GROUPS_VALUE(_ThroughSpId));
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)}, shared, _} ->
            _UserId = token_to_user_id(SerializedToken),
            lists:member(GroupId, ?USER_EFF_GROUPS(_UserId));
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(_SerializedToken)}, _, _} ->
            false;
        % undefined AuthOverride means asking with provider's auth
        {undefined, shared, _} ->
            true
    end,
    case Authorized of
        true ->
            {ok, #gs_resp_graph{data_format = resource, data = ?GROUP_SHARED_DATA_VALUE(GroupId)}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_space, id = SpaceId, aspect = instance}, AuthOverride, _) ->
    Authorized = case {AuthOverride, GRI#gri.scope} of
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)}, private} ->
            UserId = token_to_user_id(SerializedToken),
            lists:member(atom_to_binary(?SPACE_VIEW, utf8), maps:get(UserId, ?SPACE_EFF_USERS_VALUE(SpaceId), []));
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)}, protected} ->
            UserId = token_to_user_id(SerializedToken),
            maps:is_key(UserId, ?SPACE_EFF_USERS_VALUE(SpaceId));
        % undefined AuthOverride means asking with provider's auth
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

mock_graph_get(GRI = #gri{type = od_share, id = ShareId, aspect = instance}, AuthOverride, _) ->
    Authorized = case {AuthOverride, GRI#gri.scope} of
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)}, private} ->
            UserId = token_to_user_id(SerializedToken),
            maps:is_key(UserId, ?SPACE_EFF_USERS_VALUE(?SHARE_SPACE(ShareId)));
        % undefined AuthOverride means asking with provider's auth
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

mock_graph_get(GRI = #gri{type = od_provider, id = ProviderId, aspect = instance}, AuthOverride, _) ->
    Authorized = case {AuthOverride, GRI#gri.scope} of
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(_SerializedToken)}, private} ->
            false;
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)}, protected} ->
            UserId = token_to_user_id(SerializedToken),
            lists:member(UserId, ?PROVIDER_EFF_USERS(ProviderId));
        % undefined AuthOverride means asking with provider's auth
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

mock_graph_get(#gri{type = od_handle_service, id = HServiceId, aspect = instance}, _, _) ->
    {ok, #gs_resp_graph{data_format = resource, data = ?HANDLE_SERVICE_PUBLIC_DATA_VALUE(HServiceId)}};

mock_graph_get(#gri{type = od_handle, id = HandleId, aspect = instance}, _, _) ->
    {ok, #gs_resp_graph{data_format = resource, data = ?HANDLE_PUBLIC_DATA_VALUE(HandleId)}};

mock_graph_get(GRI = #gri{type = od_harvester, id = HarvesterId, aspect = instance}, AuthOverride, _) ->
    Authorized = case {AuthOverride, GRI#gri.scope} of
        {undefined, private} ->
            true;
        {?USER_GS_TOKEN_AUTH(_), _} ->
            false
    end,
    case Authorized of
        true ->
            Data = case GRI#gri.scope of
                private -> ?HARVESTER_PRIVATE_DATA_VALUE(HarvesterId)
            end,
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_storage, id = StorageId, aspect = instance}, AuthOverride, AuthHint) ->
    Authorized = case {AuthOverride, GRI#gri.scope, AuthHint} of
        {undefined, private, _} ->
            true;
        {undefined, shared, ?THROUGH_SPACE(_SpaceId)} ->
            true;
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(_SerializedToken)}, _, _} ->
            false
    end,
    case Authorized of
        true ->
            Data = case GRI#gri.scope of
                private -> ?STORAGE_PRIVATE_DATA_VALUE(StorageId);
                shared -> ?STORAGE_SHARED_DATA_VALUE(StorageId)
            end,
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(#gri{type = od_token, id = TokenId, aspect = instance, scope = shared}, AuthOverride, _) ->
    Authorized = case AuthOverride of
        undefined ->
            true;
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)}, _} ->
            token_to_user_id(SerializedToken) == ?USER_1
    end,
    case Authorized of
        true ->
            case lists:member(TokenId, [?TOKEN_1, ?TOKEN_2]) of
                true ->
                    {ok, #gs_resp_graph{data_format = resource, data = ?TOKEN_SHARED_DATA_VALUE(TokenId)}};
                _ ->
                    ?ERROR_NOT_FOUND
            end;
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(#gri{type = temporary_token_secret, id = UserId, aspect = user, scope = shared}, AuthOverride, _) ->
    Authorized = case AuthOverride of
        undefined ->
            true;
        {#auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)}, _} ->
            token_to_user_id(SerializedToken) == UserId
    end,
    case Authorized of
        true ->
            {ok, #gs_resp_graph{
                data_format = resource,
                data = ?TEMPORARY_TOKENS_SECRET_SHARED_DATA_VALUE(UserId)}
            };
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(#gri{type = od_atm_inventory, id = AtmInventoryId, aspect = instance}, AuthOverride, _AuthHint) ->
    Authorized = case AuthOverride of
        undefined ->
            false;
        #auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)} ->
            UserId = token_to_user_id(SerializedToken),
            lists:member(AtmInventoryId, ?USER_EFF_ATM_INVENTORIES(UserId))
    end,
    case Authorized of
        true ->
            Data = ?ATM_INVENTORY_PRIVATE_DATA_VALUE(AtmInventoryId),
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(#gri{type = od_atm_lambda, id = AtmLambdaId, aspect = instance}, AuthOverride, _AuthHint) ->
    Authorized = case AuthOverride of
        undefined ->
            false;
        #auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)} ->
            UserId = token_to_user_id(SerializedToken),
            lists:any(fun(AtmInventoryId) ->
                lists:member(AtmInventoryId, ?USER_EFF_ATM_INVENTORIES(UserId))
            end, ?ATM_LAMBDA_INVENTORIES(AtmLambdaId))
    end,
    case Authorized of
        true ->
            Data = ?ATM_LAMBDA_PRIVATE_DATA_VALUE(AtmLambdaId),
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(#gri{type = od_atm_workflow_schema, id = AtmWorkflowSchemaId, aspect = instance}, AuthOverride, _AuthHint) ->
    Authorized = case AuthOverride of
        undefined ->
            false;
        #auth_override{client_auth = ?USER_GS_TOKEN_AUTH(SerializedToken)} ->
            UserId = token_to_user_id(SerializedToken),
            lists:member(?ATM_WORKFLOW_SCHEMA_INVENTORY(AtmWorkflowSchemaId), ?USER_EFF_ATM_INVENTORIES(UserId))
    end,
    case Authorized of
        true ->
            Data = ?ATM_WORKFLOW_SCHEMA_PRIVATE_DATA_VALUE(AtmWorkflowSchemaId),
            {ok, #gs_resp_graph{data_format = resource, data = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end.


token_to_user_id(SerializedToken) ->
    {ok, #token{subject = ?SUB(user, UserId)}} = tokens:deserialize(SerializedToken),
    UserId.
