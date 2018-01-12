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

%% API
-export([
    mock_gs_client/1, unmock_gs_client/1,
    wait_for_mocked_connection/1,
    create_user_session/2,
    count_reqs/2,
    invalidate_cache/3,
    invalidate_all_test_records/1
]).



mock_gs_client(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Nodes, gs_client, []),
    ok = test_utils:mock_expect(Nodes, gs_client, start_link, fun mock_start_link/5),
    ok = test_utils:mock_expect(Nodes, gs_client, sync_request, fun mock_sync_request/2),

    initializer:mock_provider_id(
        Nodes, ?PROVIDER_1, ?MOCK_PROVIDER_AUTH_MACAROON(?PROVIDER_1), ?MOCK_PROVIDER_IDENTITY_MACAROON(?PROVIDER_1)
    ),

    % gs_client requires successful setting of subdomain delegation IPs, but it cannot
    % be achieved in test environment
    ok = test_utils:mock_expect(Nodes, provider_logic, update_subdomain_delegation_ips, fun () -> ok end),

    ok.


unmock_gs_client(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, gs_client),
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
    Auth = ?USER_INTERNAL_TOKEN_AUTH(UserId),
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
        {od_handle, ?HANDLE_1}, {od_handle, ?HANDLE_2}
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


mock_graph_create(#gri{type = od_user, id = UserId, aspect = default_space}, ?USER_GS_TOKEN_AUTH(UserId), Data) ->
    #{<<"spaceId">> := SpaceId} = Data,
    case lists:member(SpaceId, [?SPACE_1, ?SPACE_2]) of
        true ->
            {ok, #gs_resp_graph{}};
        false ->
            ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"spaceId">>)
    end;

mock_graph_create(#gri{type = od_group, id = undefined, aspect = instance}, ?USER_GS_TOKEN_AUTH(_UserId), Data) ->
    case Data of
        #{<<"name">> := Name} when is_binary(Name) ->
            {ok, #gs_resp_graph{result = ?GROUP_PRIVATE_DATA_VALUE(?MOCK_CREATED_GROUP_ID)}};
        _ ->
            ?ERROR_BAD_VALUE_BINARY(<<"name">>)
    end;
mock_graph_create(#gri{type = od_group, id = undefined, aspect = join}, ?USER_GS_TOKEN_AUTH(_UserId), Data) ->
    #{<<"token">> := Token} = Data,
    case Token of
        ?MOCK_JOIN_GROUP_TOKEN ->
            {ok, #gs_resp_graph{result = ?GROUP_PRIVATE_DATA_VALUE(?MOCK_JOINED_GROUP_ID)}};
        _ ->
            ?ERROR_BAD_VALUE_TOKEN(<<"token">>)
    end;
mock_graph_create(#gri{type = od_group, id = GroupId, aspect = invite_user_token}, ?USER_GS_TOKEN_AUTH(_UserId), _) ->
    case lists:member(GroupId, [?GROUP_1, ?GROUP_2]) of
        true ->
            {ok, #gs_resp_graph{result = #{<<"data">> => ?MOCK_INVITE_USER_TOKEN}}};
        _ ->
            ?ERROR_NOT_FOUND
    end;
mock_graph_create(#gri{type = od_group, id = GroupId, aspect = invite_group_token}, ?USER_GS_TOKEN_AUTH(_UserId), _) ->
    case lists:member(GroupId, [?GROUP_1, ?GROUP_2]) of
        true ->
            {ok, #gs_resp_graph{result = #{<<"data">> => ?MOCK_INVITE_GROUP_TOKEN}}};
        _ ->
            ?ERROR_NOT_FOUND
    end;

mock_graph_create(#gri{type = od_space, id = undefined, aspect = instance}, ?USER_GS_TOKEN_AUTH(_UserId), Data) ->
    case Data of
        #{<<"name">> := Name} when is_binary(Name) ->
            {ok, #gs_resp_graph{result = ?SPACE_PRIVATE_DATA_VALUE(?MOCK_CREATED_SPACE_ID)}};
        _ ->
            ?ERROR_BAD_VALUE_BINARY(<<"name">>)
    end;
mock_graph_create(#gri{type = od_space, id = undefined, aspect = join}, ?USER_GS_TOKEN_AUTH(_UserId), Data) ->
    #{<<"token">> := Token} = Data,
    case Token of
        ?MOCK_JOIN_SPACE_TOKEN ->
            {ok, #gs_resp_graph{result = ?SPACE_PRIVATE_DATA_VALUE(?MOCK_JOINED_SPACE_ID)}};
        _ ->
            ?ERROR_BAD_VALUE_TOKEN(<<"token">>)
    end;
mock_graph_create(#gri{type = od_space, id = SpaceId, aspect = invite_user_token}, ?USER_GS_TOKEN_AUTH(_UserId), _) ->
    case lists:member(SpaceId, [?SPACE_1, ?SPACE_2]) of
        true ->
            {ok, #gs_resp_graph{result = #{<<"data">> => ?MOCK_INVITE_USER_TOKEN}}};
        _ ->
            ?ERROR_NOT_FOUND
    end;
mock_graph_create(#gri{type = od_space, id = SpaceId, aspect = invite_group_token}, ?USER_GS_TOKEN_AUTH(_UserId), _) ->
    case lists:member(SpaceId, [?SPACE_1, ?SPACE_2]) of
        true ->
            {ok, #gs_resp_graph{result = #{<<"data">> => ?MOCK_INVITE_GROUP_TOKEN}}};
        _ ->
            ?ERROR_NOT_FOUND
    end;
mock_graph_create(#gri{type = od_space, id = SpaceId, aspect = invite_provider_token}, ?USER_GS_TOKEN_AUTH(_UserId), _) ->
    case lists:member(SpaceId, [?SPACE_1, ?SPACE_1]) of
        true ->
            {ok, #gs_resp_graph{result = #{<<"data">> => ?MOCK_INVITE_PROVIDER_TOKEN}}};
        _ ->
            ?ERROR_NOT_FOUND
    end;

mock_graph_create(#gri{type = od_share, id = undefined, aspect = instance}, ?USER_GS_TOKEN_AUTH(_UserId), Data) ->
    #{
        <<"shareId">> := ShareId,
        <<"name">> := _Name,
        <<"rootFileId">> := _RootFileId,
        <<"spaceId">> := SpaceId
    } = Data,
    case lists:member(SpaceId, [?SPACE_1, ?SPACE_2]) of
        true ->
            {ok, #gs_resp_graph{result = ?SHARE_PRIVATE_DATA_VALUE(ShareId)}};
        _ ->
            ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"spaceId">>)
    end;

mock_graph_create(#gri{type = od_handle, id = undefined, aspect = instance}, ?USER_GS_TOKEN_AUTH(_UserId), Data) ->
    #{
        <<"handleServiceId">> := HandleServiceId,
        <<"resourceType">> := _ResourceType,
        <<"resourceId">> := _ResourceId,
        <<"metadata">> := _Metadata
    } = Data,
    case lists:member(HandleServiceId, [?HANDLE_SERVICE_1, ?HANDLE_SERVICE_2]) of
        true ->
            {ok, #gs_resp_graph{result = ?HANDLE_PRIVATE_DATA_VALUE(?MOCK_CREATED_HANDLE_ID)}};
        _ ->
            ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"handleServiceId">>)
    end.


mock_graph_update(#gri{type = od_group, id = _GroupId, aspect = instance}, ?USER_GS_TOKEN_AUTH(_UserId), Data) ->
    case Data of
        #{<<"name">> := Name} when is_binary(Name) ->
            {ok, #gs_resp_graph{result = ok}};
        _ ->
            ?ERROR_BAD_VALUE_BINARY(<<"name">>)
    end;
mock_graph_update(#gri{type = od_group, id = GroupId, aspect = {user_privileges, UserId}}, ?USER_GS_TOKEN_AUTH(_UserId), _Data) ->
    case maps:is_key(UserId, ?GROUP_EFF_USERS_VALUE(GroupId)) of
        true ->
            {ok, #gs_resp_graph{result = ok}};
        false ->
            ?ERROR_NOT_FOUND
    end;
mock_graph_update(#gri{type = od_group, id = GroupId, aspect = {child_privileges, ChildId}}, ?USER_GS_TOKEN_AUTH(_UserId), _Data) ->
    case maps:is_key(ChildId, ?GROUP_EFF_CHILDREN_VALUE(GroupId)) of
        true ->
            {ok, #gs_resp_graph{result = ok}};
        false ->
            ?ERROR_NOT_FOUND
    end;

mock_graph_update(#gri{type = od_space, id = _SpaceId, aspect = instance}, ?USER_GS_TOKEN_AUTH(_UserId), Data) ->
    case Data of
        #{<<"name">> := Name} when is_binary(Name) ->
            {ok, #gs_resp_graph{result = ok}};
        _ ->
            ?ERROR_BAD_VALUE_BINARY(<<"name">>)
    end;
mock_graph_update(#gri{type = od_space, id = SpaceId, aspect = {user_privileges, UserId}}, ?USER_GS_TOKEN_AUTH(_UserId), _Data) ->
    case maps:is_key(UserId, ?SPACE_EFF_USERS_VALUE(SpaceId)) of
        true ->
            {ok, #gs_resp_graph{result = ok}};
        false ->
            ?ERROR_NOT_FOUND
    end;
mock_graph_update(#gri{type = od_space, id = SpaceId, aspect = {group_privileges, GroupId}}, ?USER_GS_TOKEN_AUTH(_UserId), _Data) ->
    case maps:is_key(GroupId, ?SPACE_EFF_GROUPS_VALUE(SpaceId)) of
        true ->
            {ok, #gs_resp_graph{result = ok}};
        false ->
            ?ERROR_NOT_FOUND
    end;

mock_graph_update(#gri{type = od_share, id = _ShareId, aspect = instance}, ?USER_GS_TOKEN_AUTH(_UserId), Data) ->
    case Data of
        #{<<"name">> := Name} when is_binary(Name) ->
            {ok, #gs_resp_graph{result = ok}};
        _ ->
            ?ERROR_BAD_VALUE_BINARY(<<"name">>)
    end.


mock_graph_delete(#gri{type = od_user, id = UserId, aspect = {group, GroupId}}, ?USER_GS_TOKEN_AUTH(UserId)) ->
    case lists:member(GroupId, ?USER_EFF_GROUPS(UserId)) of
        true ->
            {ok, #gs_resp_graph{}};
        false ->
            ?ERROR_NOT_FOUND
    end;
mock_graph_delete(#gri{type = od_user, id = UserId, aspect = {space, SpaceId}}, ?USER_GS_TOKEN_AUTH(UserId)) ->
    case lists:member(SpaceId, ?USER_EFF_SPACES(UserId)) of
        true ->
            {ok, #gs_resp_graph{}};
        false ->
            ?ERROR_NOT_FOUND
    end;

mock_graph_delete(#gri{type = od_group, id = GroupId, aspect = instance}, ?USER_GS_TOKEN_AUTH(_UserId)) ->
    case lists:member(GroupId, [?GROUP_1, ?GROUP_2]) of
        true ->
            {ok, #gs_resp_graph{}};
        false ->
            ?ERROR_NOT_FOUND
    end;
mock_graph_delete(#gri{type = od_group, id = GroupId, aspect = {parent, ParentId}}, ?USER_GS_TOKEN_AUTH(_UserId)) ->
    case lists:member(ParentId, ?GROUP_DIRECT_PARENTS(GroupId)) of
        true ->
            {ok, #gs_resp_graph{}};
        false ->
            ?ERROR_NOT_FOUND
    end;
mock_graph_delete(#gri{type = od_group, id = GroupId, aspect = {space, SpaceId}}, ?USER_GS_TOKEN_AUTH(_UserId)) ->
    case lists:member(SpaceId, ?GROUP_EFF_SPACES(GroupId)) of
        true ->
            {ok, #gs_resp_graph{}};
        false ->
            ?ERROR_NOT_FOUND
    end;

mock_graph_delete(#gri{type = od_space, id = SpaceId, aspect = instance}, ?USER_GS_TOKEN_AUTH(_UserId)) ->
    case lists:member(SpaceId, [?SPACE_1, ?SPACE_2]) of
        true ->
            {ok, #gs_resp_graph{}};
        false ->
            ?ERROR_NOT_FOUND
    end;
mock_graph_delete(#gri{type = od_share, id = ShareId, aspect = instance}, ?USER_GS_TOKEN_AUTH(_UserId)) ->
    case lists:member(ShareId, [?SHARE_1, ?SHARE_2]) of
        true ->
            {ok, #gs_resp_graph{}};
        false ->
            ?ERROR_NOT_FOUND
    end.


mock_graph_get(GRI = #gri{type = od_user, id = Id, aspect = instance}, Authorization, AuthHint) ->
    UserId = case Id of
        ?SELF -> case Authorization of
            ?USER_GS_TOKEN_AUTH(ClientUserId) ->
                ClientUserId;
            _ ->
                ?ERROR_NOT_FOUND
        end;
        _ -> Id
    end,
    Authorized = case {Authorization, GRI#gri.scope, AuthHint} of
        {?USER_GS_TOKEN_AUTH(UserId), _, _} ->
            true;
        {?USER_GS_TOKEN_AUTH(ClientUser), shared, ?THROUGH_GROUP(_ThroughGrId)} ->
            lists:member(atom_to_binary(?GROUP_VIEW, utf8), maps:get(ClientUser, ?GROUP_EFF_USERS_VALUE(_ThroughGrId), [])) andalso
                maps:is_key(UserId, ?GROUP_EFF_USERS_VALUE(_ThroughGrId));
        {?USER_GS_TOKEN_AUTH(ClientUser), shared, ?THROUGH_SPACE(_ThroughSpId)} ->
            lists:member(atom_to_binary(?SPACE_VIEW, utf8), maps:get(ClientUser, ?SPACE_EFF_USERS_VALUE(_ThroughSpId), [])) andalso
                maps:is_key(UserId, ?SPACE_EFF_USERS_VALUE(_ThroughSpId));
        {?USER_GS_TOKEN_AUTH(_OtherUser), _, _} ->
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
            {ok, #gs_resp_graph{result = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_group, id = GroupId, aspect = instance}, Authorization, AuthHint) ->
    Authorized = case {Authorization, GRI#gri.scope, AuthHint} of
        {?USER_GS_TOKEN_AUTH(UserId), private, _} ->
            lists:member(atom_to_binary(?GROUP_VIEW, utf8), maps:get(UserId, ?GROUP_EFF_USERS_VALUE(GroupId), []));
        {?USER_GS_TOKEN_AUTH(UserId), protected, _} ->
            maps:is_key(UserId, ?GROUP_EFF_USERS_VALUE(GroupId));
        {?USER_GS_TOKEN_AUTH(UserId), shared, ?THROUGH_GROUP(_ThroughGrId)} ->
            lists:member(atom_to_binary(?GROUP_VIEW, utf8), maps:get(UserId, ?GROUP_EFF_USERS_VALUE(_ThroughGrId), [])) andalso
                maps:is_key(GroupId, ?GROUP_EFF_CHILDREN_VALUE(_ThroughGrId));
        {?USER_GS_TOKEN_AUTH(UserId), shared, ?THROUGH_SPACE(_ThroughSpId)} ->
            lists:member(atom_to_binary(?SPACE_VIEW, utf8), maps:get(UserId, ?SPACE_EFF_USERS_VALUE(_ThroughSpId), [])) andalso
                maps:is_key(GroupId, ?SPACE_EFF_GROUPS_VALUE(_ThroughSpId));
        {?USER_GS_TOKEN_AUTH(UserId), shared, _} ->
            maps:is_key(UserId, ?GROUP_EFF_USERS_VALUE(GroupId));
        {?USER_GS_TOKEN_AUTH(_UserId), _, _} ->
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
                shared -> ?GROUP_SHARED_DATA_VALUE(GroupId);
                protected -> ?GROUP_PROTECTED_DATA_VALUE(GroupId);
                private -> ?GROUP_PRIVATE_DATA_VALUE(GroupId)
            end,
            {ok, #gs_resp_graph{result = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_space, id = SpaceId, aspect = instance}, Authorization, _) ->
    Authorized = case {Authorization, GRI#gri.scope} of
        {?USER_GS_TOKEN_AUTH(UserId), private} ->
            lists:member(atom_to_binary(?SPACE_VIEW, utf8), maps:get(UserId, ?SPACE_EFF_USERS_VALUE(SpaceId), []));
        {?USER_GS_TOKEN_AUTH(UserId), protected} ->
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
            {ok, #gs_resp_graph{result = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_share, id = ShareId, aspect = instance}, Authorization, _) ->
    Authorized = case {Authorization, GRI#gri.scope} of
        {?USER_GS_TOKEN_AUTH(UserId), private} ->
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
            {ok, #gs_resp_graph{result = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_provider, id = ProviderId, aspect = instance}, Authorization, _) ->
    Authorized = case {Authorization, GRI#gri.scope} of
        {?USER_GS_TOKEN_AUTH(_UserId), private} ->
            false;
        {?USER_GS_TOKEN_AUTH(UserId), protected} ->
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
            {ok, #gs_resp_graph{result = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(#gri{type = od_handle_service, id = HServiceId, aspect = instance, scope = private}, Authorization, _) ->
    Authorized = case Authorization of
        ?USER_GS_TOKEN_AUTH(UserId) ->
            lists:member(atom_to_binary(?HANDLE_SERVICE_VIEW, utf8), maps:get(UserId, ?HANDLE_SERVICE_EFF_USERS_VALUE(HServiceId), []));
        % undefined Authorization means asking with provider's auth
        undefined ->
            false
    end,
    case Authorized of
        true ->
            {ok, #gs_resp_graph{result = ?HANDLE_SERVICE_PRIVATE_DATA_VALUE(HServiceId)}};
        false ->
            ?ERROR_FORBIDDEN
    end;

mock_graph_get(GRI = #gri{type = od_handle, id = HandleId, aspect = instance}, Authorization, _) ->
    Authorized = case {Authorization, GRI#gri.scope} of
        {?USER_GS_TOKEN_AUTH(UserId), private} ->
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
            {ok, #gs_resp_graph{result = Data}};
        false ->
            ?ERROR_FORBIDDEN
    end.
