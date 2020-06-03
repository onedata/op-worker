%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for initializing things like session or storage configuration.
%%% @end
%%%--------------------------------------------------------------------
-module(initializer).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/gui_paths.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/credentials.hrl").
-include("proto/common/clproto_message_id.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("test_utils/initializer.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("public_key/include/public_key.hrl").

%% API
-export([
    set_default_onezone_domain/1,
    create_access_token/1, create_access_token/2, create_access_token/3,
    create_identity_token/1,
    setup_session/3, teardown_session/2,
    setup_storage/1, setup_storage/2, teardown_storage/1,
    clean_test_users_and_spaces/1,
    remove_pending_messages/0, create_test_users_and_spaces/2,
    remove_pending_messages/1, clear_subscriptions/1,
    communicator_mock/1, clean_test_users_and_spaces_no_validate/1,
    domain_to_provider_id/1, mock_test_file_context/2, unmock_test_file_context/1
]).
-export([mock_auth_manager/1, mock_auth_manager/2, unmock_auth_manager/1]).
-export([mock_provider_ids/1, mock_provider_id/4, unmock_provider_ids/1]).
-export([unload_quota_mocks/1, disable_quota_limit/1]).
-export([testmaster_mock_space_user_privileges/4, node_get_mocked_space_user_privileges/2]).
-export([mock_share_logic/1, unmock_share_logic/1]).
-export([put_into_cache/1]).
-export([get_storage_id/1, get_supporting_storage_id/2]).
-export([local_ip_v4/0]).


-record(user_config, {
    id :: od_user:id(),
    name :: binary(),
    spaces :: [],
    groups :: [],
    token :: binary()
}).

-define(TIMEOUT, timer:seconds(5)).
-define(DEFAULT_GLOBAL_SETUP, [
    {<<"groups">>, [
        {<<"group1">>, [
            {<<"users">>, [<<"user1">>]}
        ]},
        {<<"group2">>, [
            {<<"users">>, [<<"user1">>, <<"user2">>]}
        ]},
        {<<"group3">>, [
            {<<"users">>, [<<"user1">>, <<"user2">>, <<"user3">>]}
        ]},
        {<<"group4">>, [
            {<<"users">>, [<<"user1">>, <<"user2">>, <<"user3">>, <<"user4">>]}
        ]}
    ]},
    {<<"spaces">>, [
        {<<"space_id1">>, [
            {<<"displayed_name">>, <<"space_name1">>},
            {<<"users">>, [<<"user1">>]},
            {<<"groups">>, [<<"group1">>]},
            {<<"providers">>, [
                {<<"p1">>, [
                    {<<"supported_size">>, 1000000000}
%%                        {<<"storage">>, <<"/mnt/st1">>}
                ]}
            ]}
        ]},
        {<<"space_id2">>, [
            {<<"displayed_name">>, <<"space_name2">>},
            {<<"users">>, [<<"user1">>, <<"user2">>]},
            {<<"groups">>, [<<"group1">>, <<"group2">>]},
            {<<"providers">>, [
                {<<"p1">>, [
                    {<<"supported_size">>, 1000000000}
%%                        {<<"storage">>, <<"/mnt/st1">>}

                ]}
            ]}
        ]},
        {<<"space_id3">>, [
            {<<"displayed_name">>, <<"space_name3">>},
            {<<"users">>, [<<"user1">>, <<"user2">>, <<"user3">>]},
            {<<"groups">>, [<<"group1">>, <<"group2">>, <<"group3">>]},
            {<<"providers">>, [
                {<<"p1">>, [
                    {<<"supported_size">>, 1000000000}
%%                        {<<"storage">>, <<"/mnt/st1">>}
                ]}
            ]}
        ]},
        {<<"space_id4">>, [
            {<<"displayed_name">>, <<"space_name4">>},
            {<<"users">>, [<<"user1">>, <<"user2">>, <<"user3">>, <<"user4">>]},
            {<<"groups">>, [<<"group1">>, <<"group2">>, <<"group3">>, <<"group4">>]},
            {<<"providers">>, [
                {<<"p1">>, [
                    {<<"supported_size">>, 1000000000}
%%                        {<<"storage">>, <<"/mnt/st1">>}
                ]}
            ]}
        ]}
    ]}
]).

-define(TOKENS_SECRET, <<"secret">>).
-define(TEMPORARY_TOKENS_GENERATION, 1).
-define(DEFAULT_ONEZONE_DOMAIN, <<"onezone.test">>).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns provider id based on worker's domain
%% @end
%%--------------------------------------------------------------------
-spec domain_to_provider_id(Domain :: atom()) -> binary().
domain_to_provider_id(Domain) ->
    atom_to_binary(Domain, utf8).

-spec set_default_onezone_domain(Config :: proplists:proplist()) -> ok.
set_default_onezone_domain(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:set_env(Workers, op_worker, oz_domain, ?DEFAULT_ONEZONE_DOMAIN).

%%--------------------------------------------------------------------
%% @doc
%% Setup and mocking related with users and spaces, done on each provider
%% @end
%%--------------------------------------------------------------------
-spec create_test_users_and_spaces(ConfigPath :: string(), JsonConfig :: list()) -> list().
create_test_users_and_spaces(ConfigPath, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    create_test_users_and_spaces(Workers, ConfigPath, Config).

%%--------------------------------------------------------------------
%% @doc
%% Cleanup and unmocking related with users and spaces
%% @end
%%--------------------------------------------------------------------
-spec clean_test_users_and_spaces(Config :: list()) -> term().
clean_test_users_and_spaces(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    DomainWorkers = get_different_domain_workers(Config),

    lists:foreach(fun(W) ->
        initializer:teardown_session(W, Config)
    end, DomainWorkers),

    test_utils:mock_validate_and_unload(Workers, [
        user_logic, group_logic,
        space_logic, provider_logic, cluster_logic, harvester_logic,
        auth_manager
    ]),
    unmock_provider_ids(Workers).

%%TODO this function can be deleted after resolving VFS-1811 and replacing call
%%to this function in cdmi_test_SUITE with call to clean_test_users_and_spaces.
%%--------------------------------------------------------------------
%% @doc
%% Cleanup and unmocking related with users and spaces
%% @end
%%--------------------------------------------------------------------
-spec clean_test_users_and_spaces_no_validate(Config :: list()) -> term().
clean_test_users_and_spaces_no_validate(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    DomainWorkers = get_different_domain_workers(Config),

    lists:foreach(fun(W) ->
        initializer:teardown_session(W, Config)
    end, DomainWorkers),

    test_utils:mock_unload(Workers, [user_logic, group_logic, space_logic,
        provider_logic, cluster_logic, harvester_logic, storage_logic, provider_auth]),
    unmock_provider_ids(Workers).

%%--------------------------------------------------------------------
%% @doc
%% @equiv remove_pending_messages(0)
%% @end
%%--------------------------------------------------------------------
-spec remove_pending_messages() -> ok.
remove_pending_messages() ->
    remove_pending_messages(0).

%%--------------------------------------------------------------------
%% @doc
%% Removes messages for process queue. Waits 'Timeout' milliseconds for the
%% next message.
%% @end
%%--------------------------------------------------------------------
-spec remove_pending_messages(Timeout :: timeout()) -> ok.
remove_pending_messages(Timeout) ->
    receive
        _ -> remove_pending_messages(Timeout)
    after
        Timeout -> ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes all subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec clear_subscriptions(Worker :: node()) -> ok.
clear_subscriptions(Worker) ->
    {ok, Docs} = ?assertMatch({ok, _}, rpc:call(Worker, subscription,
        list_durable_subscriptions, [])),
    lists:foreach(fun(#document{key = Key}) ->
        ?assertEqual(ok, rpc:call(Worker, subscription, delete, [Key]))
    end, Docs).


-spec create_access_token(od_user:id()) -> tokens:serialized().
create_access_token(UserId) ->
    create_access_token(UserId, []).


-spec create_access_token(od_user:id(), [caveats:caveat()]) -> tokens:serialized().
create_access_token(UserId, Caveats) ->
    create_access_token(UserId, Caveats, temporary).


-spec create_access_token(od_user:id(), [caveats:caveat()], Persistence :: named | temporary) ->
    tokens:serialized().
create_access_token(UserId, Caveats, Persistence) ->
    create_token(?ACCESS_TOKEN, UserId, Caveats, Persistence).


-spec create_identity_token(od_user:id()) -> tokens:serialized().
create_identity_token(UserId) ->
    create_token(?IDENTITY_TOKEN, UserId, [], temporary).


-spec create_token(tokens:type(), od_user:id(), [caveats:caveat()], Persistence :: named | temporary) ->
    tokens:serialized().
create_token(TokenType, UserId, Caveats, Persistence) ->
    {ok, SerializedToken} = ?assertMatch(
        {ok, _},
        tokens:serialize(tokens:construct(#token{
            onezone_domain = <<"zone">>,
            subject = ?SUB(user, UserId),
            id = UserId,
            type = TokenType,
            persistence = case Persistence of
                named -> named;
                temporary -> {temporary, ?TEMPORARY_TOKENS_GENERATION}
            end
        }, ?TOKENS_SECRET, Caveats))
    ),
    SerializedToken.


%%--------------------------------------------------------------------
%% @doc
%% Setup test users' sessions on server
%% @end
%%--------------------------------------------------------------------
-spec setup_session(Worker :: node(), [#user_config{}], Config :: term()) -> NewConfig :: term().
setup_session(_Worker, [], Config) ->
    Config;
setup_session(Worker, [{_, #user_config{
    id = UserId,
    spaces = Spaces,
    token = AccessToken,
    groups = Groups,
    name = UserName
}} | R], Config) ->

    Name = fun(Text, User) ->
        list_to_binary(Text ++ "_" ++ binary_to_list(User))
    end,

    Nonce = Name(atom_to_list(?GET_DOMAIN(Worker)) ++ "_nonce", UserId),

    Identity = ?SUB(user, UserId),
    TokenCredentials = auth_manager:build_token_credentials(
        AccessToken, undefined,
        local_ip_v4(), oneclient, allow_data_access_caveats
    ),
    {ok, SessId} = ?assertMatch({ok, _}, rpc:call(
        Worker,
        session_manager,
        reuse_or_create_fuse_session,
        [Nonce, Identity, TokenCredentials])
    ),

    lists:foreach(fun({SpaceId, SpaceName}) ->
        case get(SpaceName) of
            undefined -> put(SpaceName, [SessId]);
            SessIds -> put(SpaceName, [SessId | SessIds])
        end,
        rpc:call(Worker, session, set_direct_io, [SessId, SpaceId, false])
    end, Spaces),

    Ctx = rpc:call(Worker, user_ctx, new, [SessId]),
    [
        {{spaces, UserId}, Spaces},
        {{groups, UserId}, Groups},
        {{user_id, UserId}, UserId},
        {{user_name, UserId}, UserName},
        {{access_token, UserId}, AccessToken},
        {{token_credentials, UserId}, TokenCredentials},
        {{session_nonce, {UserId, ?GET_DOMAIN(Worker)}}, Nonce},
        {{session_id, {UserId, ?GET_DOMAIN(Worker)}}, SessId},
        {{fslogic_ctx, UserId}, Ctx}
        | setup_session(Worker, R, Config)
    ].

%%--------------------------------------------------------------------
%% @doc
%% Removes test users' sessions from server.
%% @end
%%--------------------------------------------------------------------
-spec teardown_session(Worker :: node(), Config :: term()) -> NewConfig :: term().
teardown_session(Worker, Config) ->
    lists:foldl(fun
        ({{session_id, _}, SessId}, Acc) ->
            case rpc:call(Worker, session, get_credentials, [SessId]) of
                {ok, Credentials} ->
                    rpc:call(Worker, auth_cache, delete_cache_entry, [Credentials]),
                    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId]));
                {error, not_found} ->
                    ok
            end,
            Acc;
        ({{spaces, _}, Spaces}, Acc) ->
            {SpaceIds, _SpaceNames} = lists:unzip(Spaces),
            lists:foreach(fun(SpaceId) ->
                rpc:call(Worker, internal_services_manager, stop_service,
                    [dbsync_worker, <<"dbsync_in_stream", SpaceId/binary>>, SpaceId]),
                rpc:call(Worker, internal_services_manager, stop_service,
                    [dbsync_worker, <<"dbsync_out_stream", SpaceId/binary>>, SpaceId]),
                rpc:call(Worker, file_meta, delete, [fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)])
            end, SpaceIds),
            Acc;
        ({{user_id, _}, UserId}, Acc) ->
            rpc:call(Worker, file_meta, delete, [fslogic_uuid:user_root_dir_uuid(UserId)]),
            Acc;
        ({{fslogic_ctx, _}, _}, Acc) ->
            Acc;
        (Elem, Acc) ->
            [Elem | Acc]
    end, [], Config).

%%--------------------------------------------------------------------
%% @doc
%% Setups test storage on server and creates test storage dir on each provider
%% @end
%%--------------------------------------------------------------------
-spec setup_storage(Config :: list()) -> list().
setup_storage(Config) ->
    DomainWorkers = get_different_domain_workers(Config),
    setup_storage(DomainWorkers, Config).

%%--------------------------------------------------------------------
%% @doc
%% Setups test storage on server and creates test storage dir on one provider
%% @end
%%--------------------------------------------------------------------
-spec setup_storage([node()], Config :: list()) -> list().
setup_storage([], Config) ->
    set_default_onezone_domain(Config),
    Config;
setup_storage([Worker | Rest], Config) ->
    TmpDir = generator:gen_storage_dir(),
    %% @todo: use shared storage
    "" = rpc:call(Worker, os, cmd, ["mkdir -p " ++ TmpDir ++ " -m 777"]),
    UserCtx = #{<<"uid">> => <<"0">>, <<"gid">> => <<"0">>},
    Args = #{<<"mountPoint">> => list_to_binary(TmpDir)},
    {ok, Helper} = helper:new_helper(
        ?POSIX_HELPER_NAME,
        Args,
        UserCtx,
        false,
        ?CANONICAL_STORAGE_PATH
    ),
    StorageName = <<"Test", (atom_to_binary(?GET_DOMAIN(Worker), utf8))/binary>>,
    {ok, StorageId} = rpc:call(Worker, storage_config, create, [StorageName, Helper, false, undefined, false]),
    storage_logic_mock_setup(Worker, #{?GET_DOMAIN_BIN(Worker) => #{StorageId => #{}}}, []),
    rpc:call(Worker, storage, on_storage_created, [StorageId]),
    [{{storage_id, ?GET_DOMAIN(Worker)}, StorageId}, {{storage_dir, ?GET_DOMAIN(Worker)}, TmpDir}] ++
    setup_storage(Rest, Config).

%%--------------------------------------------------------------------
%% @doc
%% Removes test storage dir on each provider
%% @end
%%--------------------------------------------------------------------
-spec teardown_storage(Config :: list()) -> ok.
teardown_storage(Config) ->
    DomainWorkers = get_different_domain_workers(Config),
    Workers = ?config(op_worker_nodes, Config),
    storage_mock_teardown(Workers),
    lists:foreach(fun(Worker) ->
        teardown_storage(Worker, Config) end, DomainWorkers).


%%--------------------------------------------------------------------
%% @doc
%% Mocks communicator module, so that it ignores all messages.
%% @end
%%--------------------------------------------------------------------
-spec communicator_mock(Workers :: node() | [node()]) -> ok.
communicator_mock(Workers) ->
    catch test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send_to_oneclient, fun(_, _) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Disables all quota checks. Should be unloaded via unload_quota_mocks/1.
%% @end
%%--------------------------------------------------------------------
-spec disable_quota_limit(Config :: list()) -> ok.
disable_quota_limit(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [space_quota]),
    test_utils:mock_expect(Workers, space_quota, assert_write,
        fun(_, _) -> ok end),
    test_utils:mock_expect(Workers, space_quota, assert_write,
        fun(_) -> ok end),

    test_utils:mock_expect(Workers, space_quota, available_size,
        fun(_) -> 100000000000000000 end),
    test_utils:mock_expect(Workers, space_quota, apply_size_change,
        fun(ID, _) -> {ok, ID} end).

%%--------------------------------------------------------------------
%% @doc
%% Unloads space_quota mock.
%% @end
%%--------------------------------------------------------------------
-spec unload_quota_mocks(Config :: list()) -> ok.
unload_quota_mocks(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [space_quota]).

%%--------------------------------------------------------------------
%% @doc
%% Mocks file context for test files with uuids that begin with given prefix.
%% The files will be seen as existing and non-root.
%% @end
%%--------------------------------------------------------------------
-spec mock_test_file_context(proplists:proplist(), binary()) -> ok.
mock_test_file_context(Config, UuidPrefix) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, file_ctx),
    test_utils:mock_expect(Workers, file_ctx, is_root_dir_const,
        fun(FileCtx) ->
            Uuid = file_ctx:get_uuid_const(FileCtx),
            case str_utils:binary_starts_with(Uuid, UuidPrefix) of
                true -> false;
                false -> meck:passthrough([FileCtx])
            end
        end),
    test_utils:mock_expect(Workers, file_ctx, file_exists_const,
        fun(FileCtx) ->
            Uuid = file_ctx:get_uuid_const(FileCtx),
            case str_utils:binary_starts_with(Uuid, UuidPrefix) of
                true -> true;
                false -> meck:passthrough([FileCtx])
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% Unmocks file context for test files
%% @end
%%--------------------------------------------------------------------
-spec unmock_test_file_context(proplists:proplist()) -> ok.
unmock_test_file_context(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, file_ctx).

%%--------------------------------------------------------------------
%% @doc
%% Mocks auth_manager functions for all providers in given environment.
%% @end
%%--------------------------------------------------------------------
-spec mock_auth_manager(proplists:proplist()) -> ok.
mock_auth_manager(Config) ->
    mock_auth_manager(Config, _CheckIfUserIsSupported = false).

%%--------------------------------------------------------------------
%% @doc
%% Mocks auth_manager functions for all providers in given environment.
%% @end
%%--------------------------------------------------------------------
-spec mock_auth_manager(proplists:proplist(), CheckIfUserIsSupported :: boolean()) -> ok.
mock_auth_manager(Config, CheckIfUserIsSupported) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, auth_manager, [passthrough]),
    test_utils:mock_expect(Workers, auth_manager, verify_credentials,
        fun(TokenCredentials) ->
            case tokens:deserialize(auth_manager:get_access_token(TokenCredentials)) of
                {ok, #token{subject = ?SUB(user, UserId)} = Token} ->
                    IsUserSupported = case CheckIfUserIsSupported of
                        true -> provider_logic:has_eff_user(UserId);
                        false -> true
                    end,
                    case IsUserSupported of
                        true ->
                            Consumer = case auth_manager:get_consumer_token(TokenCredentials) of
                                undefined ->
                                    undefined;
                                ConsumerToken ->
                                    {ok, #token{subject = Csm}} = tokens:deserialize(ConsumerToken),
                                    Csm
                            end,
                            AuthCtx = #auth_ctx{
                                current_timestamp = time_utils:cluster_time_seconds(),
                                ip = auth_manager:get_peer_ip(TokenCredentials),
                                interface = auth_manager:get_interface(TokenCredentials),
                                service = ?SERVICE(?OP_WORKER, oneprovider:get_id()),
                                consumer = Consumer,
                                data_access_caveats_policy = auth_manager:get_data_access_caveats_policy(TokenCredentials),
                                group_membership_checker = fun(_, _) -> false end
                            },
                            case tokens:verify(Token, ?TOKENS_SECRET, AuthCtx) of
                                {ok, Auth} ->
                                    {ok, Auth, undefined};
                                {error, _} = Err1 ->
                                    Err1
                            end;
                        false ->
                            ?ERROR_USER_NOT_SUPPORTED
                    end;
                {error, _} = Err2 ->
                    Err2
            end
        end
    ),
    test_utils:mock_expect(Workers, auth_manager, get_caveats, fun(TokenCredentials) ->
        AccessToken = auth_manager:get_access_token(TokenCredentials),
        case tokens:deserialize(AccessToken) of
            {ok, Token} ->
                {ok, tokens:get_caveats(Token)};
            {error, _} = Error ->
                Error
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Unmocks auth manager.
%% @end
%%--------------------------------------------------------------------
-spec unmock_auth_manager(proplists:proplist()) -> ok.
unmock_auth_manager(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, auth_manager).

%%--------------------------------------------------------------------
%% @doc
%% Mocks provider ids for all providers in given environment.
%% @end
%%--------------------------------------------------------------------
-spec mock_provider_ids(proplists:proplist()) -> ok.
mock_provider_ids(Config) ->
    AllWorkers = ?config(op_worker_nodes, Config),
    Domains = lists:usort([?GET_DOMAIN(W) || W <- AllWorkers]),
    lists:foreach(fun(Domain) ->
        CWorkers = get_same_domain_workers(Config, Domain),
        ProviderId = domain_to_provider_id(Domain),
        mock_provider_id(CWorkers, ProviderId, <<"AuthToken">>, <<"IdentityToken">>)
    end, Domains).

%%--------------------------------------------------------------------
%% @doc
%% Mocks provider ids for certain provider.
%% @end
%%--------------------------------------------------------------------
-spec mock_provider_id([node()], od_provider:id(), binary(), binary()) -> ok.
mock_provider_id(Workers, ProviderId, AccessToken, IdentityToken) ->
    ok = test_utils:mock_new(Workers, provider_auth),
    ok = test_utils:mock_expect(Workers, provider_auth, get_identity_token_for_consumer,
        fun(_Consumer) ->
            {ok, ?DUMMY_PROVIDER_IDENTITY_TOKEN(ProviderId)}
        end
    ),

    % Mock cached auth and identity tokens with large TTL
    ExpirationTime = time_utils:system_time_seconds() + 999999999,
    rpc:multicall(Workers, datastore_model, save, [#{model => provider_auth}, #document{
        key = <<"provider_auth">>,
        value = #provider_auth{
            provider_id = ProviderId,
            root_token = <<>>,
            cached_access_token = {ExpirationTime, AccessToken},
            cached_identity_token = {ExpirationTime, IdentityToken}
        }}]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Cleans up after mocking provider ids.
%% @end
%%--------------------------------------------------------------------
-spec unmock_provider_ids(proplists:proplist()) -> ok.
unmock_provider_ids(Workers) ->
    rpc:multicall(Workers, provider_auth, delete, []),
    ok.

-spec testmaster_mock_space_user_privileges([node()], od_space:id(), od_user:id(),
    [privileges:space_privilege()]) -> ok.
testmaster_mock_space_user_privileges(Workers, SpaceId, UserId, Privileges) ->
    rpc:multicall(Workers, simple_cache, put, [{privileges, {SpaceId, UserId}}, Privileges]),
    ok.

-spec node_get_mocked_space_user_privileges(od_space:id(), od_user:id()) -> [privileges:space_privilege()].
node_get_mocked_space_user_privileges(SpaceId, UserId) ->
    {ok, Privileges} = simple_cache:get({privileges, {SpaceId, UserId}}, fun() ->
        {false, privileges:space_admin()} end),
    Privileges.

-spec mock_share_logic(proplists:proplist()) -> ok.
mock_share_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, share_logic),
    test_utils:mock_expect(Workers, share_logic, create, fun(_Auth, ShareId, Name, SpaceId, ShareFileGuid, FileType) ->
        {ok, _} = put_into_cache(#document{key = ShareId, value = #od_share{
            name = Name,
            space = SpaceId,
            root_file = ShareFileGuid,
            public_url = <<ShareId/binary, "_public_url">>,
            file_type = FileType,
            handle = <<ShareId/binary, "_handle_id">>
        }}),
        {ok, ShareId}
    end),
    test_utils:mock_expect(Workers, share_logic, get, fun(_Auth, ShareId) ->
        od_share:get_from_cache(ShareId)
    end),
    test_utils:mock_expect(Workers, share_logic, delete, fun(_Auth, ShareId) ->
        ok = od_share:invalidate_cache(ShareId)
    end),
    test_utils:mock_expect(Workers, share_logic, update_name, fun(Auth, ShareId, NewName) ->
        {ok, #document{key = ShareId, value = Share}} = share_logic:get(Auth, ShareId),
        ok = od_share:invalidate_cache(ShareId),
        {ok, _} = put_into_cache(#document{key = ShareId, value = Share#od_share{name = NewName}}),
        ok
    end).

-spec unmock_share_logic(proplists:proplist()) -> ok.
unmock_share_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, share_logic).


%% Puts an od_* document in the provider's cache, which triggers posthooks
-spec put_into_cache(datastore:doc()) -> {ok, datastore:doc()}.
put_into_cache(Doc = #document{key = Id, value = Record}) ->
    Type = element(1, Record),
    Type:update_cache(Id, fun(_) -> {ok, Record} end, Doc).


-spec get_storage_id(node()) -> storage:id().
get_storage_id(Worker) ->
    {ok, [StorageId]} = rpc:call(Worker, provider_logic, get_storage_ids, []),
    StorageId.


-spec get_supporting_storage_id(node(), od_space:id()) -> storage:id().
get_supporting_storage_id(Worker, SpaceId) ->
    {ok, [StorageId]} = rpc:call(Worker, space_logic, get_local_storage_ids, [SpaceId]),
    StorageId.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Setup and mocking related with users and spaces on all given providers.
%% @end
%%--------------------------------------------------------------------
-spec create_test_users_and_spaces([Worker :: node()], ConfigPath :: string(), Config :: list()) -> list().
create_test_users_and_spaces(AllWorkers, ConfigPath, Config) ->
    try
        set_default_onezone_domain(Config),
        create_test_users_and_spaces_unsafe(AllWorkers, ConfigPath, Config)
    catch Type:Message ->
        ct:print("initializer:create_test_users_and_spaces crashed: ~p:~p~n~p", [
            Type, Message, erlang:get_stacktrace()
        ]),
        throw(cannot_create_test_users_and_spaces)
    end.


-spec create_test_users_and_spaces_unsafe([Worker :: node()], ConfigPath :: string(), Config :: list()) -> list().
create_test_users_and_spaces_unsafe(AllWorkers, ConfigPath, Config) ->
    timer:sleep(2000), % Sometimes the posthook starts too fast

    {ok, ConfigJSONBin} = file:read_file(ConfigPath),
    ConfigJSON = json_utils:decode_deprecated(ConfigJSONBin),

    GlobalSetup = proplists:get_value(<<"test_global_setup">>, ConfigJSON, ?DEFAULT_GLOBAL_SETUP),
    DomainMappings = [{atom_to_binary(K, utf8), V} || {K, V} <- ?config(domain_mappings, Config)],
    SpacesSetup = proplists:get_value(<<"spaces">>, GlobalSetup),
    HarvestersSetup = proplists:get_value(<<"harvesters">>, GlobalSetup, []),
    StoragesSetup = proplists:get_value(<<"storages">>, GlobalSetup, []),
    Domains = lists:usort([?GET_DOMAIN(W) || W <- AllWorkers]),

    lists:foreach(fun({_, SpaceConfig}) ->
        Providers0 = proplists:get_value(<<"providers">>, SpaceConfig),
        lists:foreach(fun({ProviderId, _}) ->
            Domain = proplists:get_value(ProviderId, DomainMappings),
            % If provider does not belong to any domain (was not started in env,
            % but included in space supports), just use its Id.
            Id = case proplists:get_value(ProviderId, DomainMappings) of
                undefined -> ProviderId;
                Domain -> domain_to_provider_id(Domain)
            end,
            case get_same_domain_workers(Config, Domain) of
                [] ->
                    ok;
                Workers ->
                    mock_provider_id(
                        Workers, Id, ?DUMMY_PROVIDER_ACCESS_TOKEN(Id), ?DUMMY_PROVIDER_IDENTITY_TOKEN(Id)
                    )
            end
        end, Providers0)
    end, SpacesSetup),

    StoragesSetupMap = lists:foldl(fun({P, Storages}, Acc) ->
        Acc#{atom_to_binary(proplists:get_value(P, DomainMappings), utf8) => json_utils:list_to_map(Storages)}
    end, #{}, StoragesSetup),

    MasterWorkers = lists:map(fun(Domain) ->
        [MWorker | _] = get_same_domain_workers(Config, Domain),
        MWorker
    end, Domains),

    Spaces = lists:map(fun({SpaceId, SpaceConfig}) ->
        DisplayName = proplists:get_value(<<"displayed_name">>, SpaceConfig),
        {SpaceId, DisplayName}
    end, SpacesSetup),

    Groups = [{GroupId, GroupId} || {GroupId, _} <- proplists:get_value(<<"groups">>, GlobalSetup)],

    SpaceUsers = lists:map(fun({SpaceId, SpaceConfig}) ->
        {SpaceId, proplists:get_value(<<"users">>, SpaceConfig)}
    end, SpacesSetup),

    GroupUsers = lists:map(fun({GroupId, GroupConfig}) ->
        {GroupId, proplists:get_value(<<"users">>, GroupConfig)}
    end, proplists:get_value(<<"groups">>, GlobalSetup)),

    UserToSpaces = lists:foldl(fun({SpaceId, Users}, AccIn) ->
        lists:foldl(fun(UserId, CAcc) ->
            maps:put(UserId, maps:get(UserId, CAcc, []) ++ [{SpaceId, proplists:get_value(SpaceId, Spaces)}], CAcc)
        end, AccIn, Users)
    end, #{}, SpaceUsers),

    UserToGroups = lists:foldl(fun({GroupId, Users}, AccIn) ->
        lists:foldl(fun(UserId, CAcc) ->
            maps:put(UserId, maps:get(UserId, CAcc, []) ++ [{GroupId, proplists:get_value(GroupId, Groups)}], CAcc)
        end, AccIn, Users)
    end, #{}, GroupUsers),

    CustomStorages = lists:filtermap(fun(Domain) ->
        case ?config({storage_id, Domain}, Config) of
            undefined -> false;
            StorageId -> {true, {StorageId, atom_to_binary(Domain, utf8)}}
        end
    end, Domains),

    SpacesSupports = lists:filtermap(fun({SpaceId, SpaceConfig}) ->
        Providers0 = proplists:get_value(<<"providers">>, SpaceConfig),
        Providers1 = lists:map(fun({CPid, Provider}) ->
            % If provider does not belong to any domain (was not started in env,
            % but included in space supports), just use its Id.
            Id = case proplists:get_value(CPid, DomainMappings) of
                undefined -> CPid;
                Domain -> domain_to_provider_id(Domain)
            end,
            {Id, Provider}
        end, Providers0),
        StorageSupp = maps:from_list(lists:filtermap(fun({PID, Info}) ->
            case proplists:get_value(<<"storage">>, Info) of
                undefined -> false;
                StorageName -> {true, {{StorageName, PID}, proplists:get_value(<<"supported_size">>, Info, 0)}}
            end
        end, Providers1)),
        case maps:size(StorageSupp) == 0 of
            true -> false;
            _ -> {true, {SpaceId, StorageSupp}}
        end
    end, SpacesSetup),

    Users = maps:fold(fun(UserId, SpacesList, AccIn) ->
        Name = fun(Text, User) ->
            list_to_binary(Text ++ "_" ++ binary_to_list(User)) end,
        AccIn ++ [{UserId, #user_config{
            id = UserId,
            name = Name("name", UserId),
            spaces = SpacesList,
            token = create_access_token(UserId),
            groups = maps:get(UserId, UserToGroups, [])
        }}
        ]
    end, [], UserToSpaces),

    SpacesHarvesters = maps:to_list(lists:foldl(
        fun({HarvesterId, HarvesterConfig}, SpacesHarvestersMap0) ->
            HarvesterSpaces = proplists:get_value(<<"spaces">>, HarvesterConfig),
            lists:foldl(fun(SpaceId, SpacesHarvestersMap00) ->
                maps:update_with(SpaceId, fun(SpaceHarvesters0) ->
                    lists:usort([HarvesterId | SpaceHarvesters0])
                end, [HarvesterId], SpacesHarvestersMap00)
            end, SpacesHarvestersMap0, HarvesterSpaces)
        end, #{}, HarvestersSetup
    )),

    user_logic_mock_setup(AllWorkers, Users),
    group_logic_mock_setup(AllWorkers, Groups, GroupUsers),
    space_logic_mock_setup(AllWorkers, Spaces, SpaceUsers, SpacesSupports, SpacesHarvesters, CustomStorages),
    provider_logic_mock_setup(Config, AllWorkers, DomainMappings, SpacesSetup, SpacesSupports, CustomStorages, StoragesSetupMap),

    lists:foreach(fun(DomainWorker) ->
        rpc:call(DomainWorker, fslogic_worker, init_paths_caches, [all])
    end, get_different_domain_workers(Config)),

    cluster_logic_mock_setup(AllWorkers),
    harvester_logic_mock_setup(AllWorkers, HarvestersSetup),
    storage_logic_mock_setup(AllWorkers, StoragesSetupMap, SpacesSupports),
    ok = init_qos_bounded_cache(Config),

    %% Setup storage
    lists:foreach(fun({_SpaceId, SpaceConfig}) ->
        Providers0 = proplists:get_value(<<"providers">>, SpaceConfig),
        lists:foreach(fun({PID, ProviderConfig}) ->
            Domain = proplists:get_value(PID, DomainMappings),
            case get_same_domain_workers(Config, Domain) of
                [Worker | _] ->
                    setup_storage(Worker, Domain, ProviderConfig, Config);
                _ -> ok
            end
        end, Providers0)
    end, SpacesSetup),

    %% Set expiration time for session to value specified in Config or to 1d.
    FuseSessionTTL = case ?config(fuse_session_grace_period_seconds, Config) of
        undefined ->
            240 * 60 * 60;
        Val ->
            Val
    end,
    {_, []} = rpc:multicall(AllWorkers, application, set_env, [?APP_NAME, session_validity_check_interval_seconds, 24 * 60 * 60]),
    {_, []} = rpc:multicall(AllWorkers, application, set_env, [?APP_NAME, fuse_session_grace_period_seconds, FuseSessionTTL]),

    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1))
    end, AllWorkers),
    rpc:multicall(AllWorkers, dbsync_worker, start_streams, []),

    lists:foreach(
        fun({_, #user_config{id = UserId, spaces = UserSpaces}}) ->
            [rpc:call(W, file_meta, setup_onedata_user, [UserId, proplists:get_keys(UserSpaces)]) || W <- AllWorkers]
        end, Users),

    proplists:compact(
        lists:flatten([{spaces, Spaces}] ++ [initializer:setup_session(W, Users, Config) || W <- MasterWorkers])
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get one worker from each provider domain.
%% @end
%%--------------------------------------------------------------------
-spec get_different_domain_workers(Config :: list()) -> [node()].
get_different_domain_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:usort(fun(W1, W2) -> ?GET_DOMAIN(W1) =< ?GET_DOMAIN(W2) end, Workers).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get workers with given domain
%% @end
%%--------------------------------------------------------------------
-spec get_same_domain_workers(Config :: list(), Domain :: atom()) -> [node()].
get_same_domain_workers(Config, Domain) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:filter(fun(W) -> ?GET_DOMAIN(W) =:= Domain end, Workers).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes test storage dir on given node
%% @end
%%--------------------------------------------------------------------
-spec teardown_storage(Worker :: node(), Config :: list()) -> string().
teardown_storage(Worker, Config) ->
    TmpDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    "" = rpc:call(Worker, os, cmd, ["rm -rf " ++ TmpDir]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks user_logic module, so that it returns user details, spaces and groups.
%% @end
%%--------------------------------------------------------------------
-spec user_logic_mock_setup(Workers :: node() | [node()], [{od_user:id(), #user_config{}}]) ->
    ok.
user_logic_mock_setup(Workers, Users) ->
    test_utils:mock_new(Workers, user_logic, [passthrough]),
    test_utils:mock_new(Workers, auth_manager, [passthrough]),

    UserConfigToUserDoc = fun(UserConfig) ->
        #user_config{
            name = UName, id = UID,
            groups = Groups,
            spaces = Spaces
        } = UserConfig,
        {SpaceIds, _} = lists:unzip(Spaces),
        {GroupIds, _} = lists:unzip(Groups),
        {ok, #document{key = UID, value = #od_user{
            full_name = UName,
            linked_accounts = [],
            emails = [],
            username = <<>>,
            eff_spaces = SpaceIds,
            eff_groups = GroupIds
        }}}
    end,

    UsersByToken = lists:map(fun({UserId, #user_config{token = Token}}) ->
        {Token, UserId}
    end, Users),

    GetUserFun = fun
        (_, _, ?ROOT_USER_ID) ->
            {ok, #document{key = ?ROOT_USER_ID, value = #od_user{full_name = <<"root">>}}};
        (_, _, ?GUEST_USER_ID) ->
            {ok, #document{key = ?GUEST_USER_ID, value = #od_user{full_name = <<"nobody">>}}};
        (_, ?GUEST_SESS_ID, _) ->
            {error, forbidden};
        (Scope, ?ROOT_SESS_ID, UserId) when Scope =:= shared orelse Scope =:= protected ->
            case proplists:get_value(UserId, Users) of
                undefined ->
                    {error, not_found};
                UserConfig2 ->
                    UserConfigToUserDoc(UserConfig2)
            end;
        (_, SessionId, UserId) when is_binary(SessionId) ->
            {ok, #document{value = #session{
                identity = ?SUB(user, SessionUserId)
            }}} = session:get(SessionId),
            case SessionUserId of
                UserId ->
                    case proplists:get_value(UserId, Users) of
                        undefined ->
                            {error, not_found};
                        UserConfig2 ->
                            UserConfigToUserDoc(UserConfig2)
                    end;
                _ ->
                    {error, forbidden}
            end;
        (_, TokenCredentials, UserId) ->
            AccessToken = auth_manager:get_access_token(TokenCredentials),
            case proplists:get_value(AccessToken, UsersByToken, undefined) of
                undefined ->
                    {error, not_found};
                UserId ->
                    case proplists:get_value(UserId, Users) of
                        undefined ->
                            {error, not_found};
                        UserConfig2 ->
                            UserConfigToUserDoc(UserConfig2)
                    end;
                _ ->
                    {error, forbidden}
            end
    end,

    test_utils:mock_expect(Workers, token_logic, verify_access_token, fun(UserToken, _, _, _, _) ->
        case proplists:get_value(UserToken, UsersByToken, undefined) of
            undefined -> {error, not_found};
            UserId -> {ok, ?SUB(user, UserId), undefined}
        end
    end),
    test_utils:mock_expect(Workers, token_logic, is_token_revoked, fun(_TokenId) ->
        {ok, false}
    end),
    test_utils:mock_expect(Workers, token_logic, get_temporary_tokens_generation, fun(_UserId) ->
        {ok, ?TEMPORARY_TOKENS_GENERATION}
    end),

    test_utils:mock_expect(Workers, auth_manager, get_caveats, fun(TokenCredentials) ->
        AccessToken = auth_manager:get_access_token(TokenCredentials),
        case tokens:deserialize(AccessToken) of
            {ok, Token} ->
                {ok, tokens:get_caveats(Token)};
            {error, _} = Error ->
                Error
        end
    end),

    test_utils:mock_expect(Workers, user_logic, get, fun(Client, UserId) ->
        GetUserFun(private, Client, UserId)
    end),

    test_utils:mock_expect(Workers, user_logic, exists, fun(Client, UserId) ->
        case GetUserFun(shared, Client, UserId) of
            {ok, _} -> true;
            _ -> false
        end
    end),

    test_utils:mock_expect(Workers, user_logic, get_full_name, fun(UserId) ->
        {ok, #document{value = #od_user{full_name = FullName}}} = GetUserFun(protected, ?ROOT_SESS_ID, UserId),
        {ok, FullName}
    end),

    GetEffSpacesFun = fun(Client, UserId) ->
        {ok, #document{value = #od_user{eff_spaces = EffSpaces}}} = GetUserFun(private, Client, UserId),
        {ok, EffSpaces}
    end,

    test_utils:mock_expect(Workers, user_logic, get_eff_spaces, GetEffSpacesFun),

    HasEffSpaceFun = fun(UserDoc, SpaceId) ->
        #document{value = #od_user{eff_spaces = EffSpaces}} = UserDoc,
        lists:member(SpaceId, EffSpaces)
    end,

    test_utils:mock_expect(Workers, user_logic, has_eff_space, HasEffSpaceFun),

    test_utils:mock_expect(Workers, user_logic, has_eff_space,
        fun(Client, UserId, SpaceId) ->
            {ok, Doc} = GetUserFun(private, Client, UserId),
            HasEffSpaceFun(Doc, SpaceId)
        end),

    test_utils:mock_expect(Workers, user_logic, get_space_by_name,
        fun
            F(Client, UserId, SpaceName) when is_binary(UserId) ->
                {ok, UserDoc} = GetUserFun(private, Client, UserId),
                F(Client, UserDoc, SpaceName);
            F(Client, #document{value = #od_user{eff_spaces = EffSpaces}}, SpaceName) ->
                lists:foldl(
                    fun
                        (_SpaceId, {true, Found}) ->
                            {true, Found};
                        (SpaceId, false) ->
                            case space_logic:get_name(Client, SpaceId) of
                                {ok, SpaceName} ->
                                    {true, SpaceId};
                                _ ->
                                    false
                            end
                    end, false, EffSpaces)
        end).

-spec group_logic_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}], [{binary(), [binary()]}]) -> ok.
group_logic_mock_setup(Workers, Groups, _Users) ->
    test_utils:mock_new(Workers, group_logic),

    test_utils:mock_expect(Workers, group_logic, get_name, fun(GroupId) ->
        GroupName = proplists:get_value(GroupId, Groups),
        {ok, GroupName}
    end).

-spec space_logic_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}], [{binary(), [binary()]}], [{binary(), [{binary(), non_neg_integer()}]}],
    [{binary(), [binary()]}], [{binary(), binary()}]) -> ok.
space_logic_mock_setup(Workers, Spaces, Users, SpacesToStorages, SpacesHarvesters, CustomStorages) ->
    test_utils:mock_new(Workers, space_logic),

    GetSpaceFun = fun(_, SpaceId) ->
        SpaceName = proplists:get_value(SpaceId, Spaces),
        UserIds = proplists:get_value(SpaceId, Users, []),
        EffUsers = maps:from_list(lists:map(fun(UID) ->
            {UID, node_get_mocked_space_user_privileges(SpaceId, UID)}
        end, UserIds)),
        Storages = proplists:get_value(SpaceId, SpacesToStorages,
            maps:from_list([{St, 1000000000} || St <- CustomStorages])),
        {ok, #document{key = SpaceId, value = #od_space{
            name = SpaceName,
            providers = maps:fold(fun({_StorageName, ProviderId}, Support, Acc) ->
                maps:update_with(ProviderId, fun(PrevSupport) -> PrevSupport + Support end, Support, Acc)
            end, #{}, Storages),
            harvesters = proplists:get_value(SpaceId, SpacesHarvesters, []),
            eff_users = EffUsers,
            eff_groups = #{},
            storages = maps:fold(fun({StorageName, _Provider}, Support, Acc) ->
                % StorageName is the same as Id
                Acc#{StorageName => Support}
            end, #{}, Storages)
        }}}
    end,

    test_utils:mock_expect(Workers, space_logic, get, GetSpaceFun),

    test_utils:mock_expect(Workers, space_logic, get_name, fun(Client, SpaceId) ->
        {ok, #document{value = #od_space{name = Name}}} = GetSpaceFun(Client, SpaceId),
        {ok, Name}
    end),

    test_utils:mock_expect(Workers, space_logic, get_eff_users, fun(Client, SpaceId) ->
        {ok, #document{value = #od_space{eff_users = EffUsers}}} = GetSpaceFun(Client, SpaceId),
        {ok, EffUsers}
    end),

    test_utils:mock_expect(Workers, space_logic, get_local_storage_ids, fun(SpaceId) ->
        {ok, #document{value = #od_space{storages = StorageIds}}} = GetSpaceFun(?ROOT_SESS_ID, SpaceId),
        {ok, #document{value = #od_provider{storages = ProviderStorageIds}}} = provider_logic:get(),
        {ok, [X || X <- ProviderStorageIds, Y <-maps:keys(StorageIds), X==Y]}
    end),
    
    test_utils:mock_expect(Workers, space_logic, get_local_storage_id, fun(SpaceId) ->
        {ok, [StorageId | _]} = space_logic:get_local_storage_ids(SpaceId),
        {ok, StorageId}
    end),

    test_utils:mock_expect(Workers, space_logic, get_all_storage_ids, fun(SpaceId) ->
        {ok, #document{value = #od_space{storages = StorageIds}}} = GetSpaceFun(?ROOT_SESS_ID, SpaceId),
        {ok, maps:keys(StorageIds)}
    end),
    
    test_utils:mock_expect(Workers, space_logic, get_provider_ids, fun(SpaceId) ->
        space_logic:get_provider_ids(?ROOT_SESS_ID, SpaceId)
    end),

    test_utils:mock_expect(Workers, space_logic, get_provider_ids, fun(SpaceId) ->
        {ok, #document{value = #od_space{providers = Providers}}} = GetSpaceFun(?ROOT_SESS_ID, SpaceId),
        {ok, maps:keys(Providers)}
    end),

    test_utils:mock_expect(Workers, space_logic, get_provider_ids, fun(Client, SpaceId) ->
        {ok, #document{value = #od_space{providers = Providers}}} = GetSpaceFun(Client, SpaceId),
        {ok, maps:keys(Providers)}
    end),

    test_utils:mock_expect(Workers, space_logic, has_eff_privilege, fun(SpaceId, UserId, Privilege) ->
        {ok, #document{value = #od_space{eff_users = EffUsers}}} = GetSpaceFun(none, SpaceId),
        lists:member(Privilege, maps:get(UserId, EffUsers, []))
    end),

    test_utils:mock_expect(Workers, space_logic, has_eff_privileges, fun(SpaceId, UserId, Privileges) ->
        {ok, #document{value = #od_space{eff_users = EffUsers}}} = GetSpaceFun(none, SpaceId),
        UserPrivileges = maps:get(UserId, EffUsers, []),
        lists:all(fun(Privilege) -> lists:member(Privilege, UserPrivileges) end, Privileges)
    end),

    test_utils:mock_expect(Workers, space_logic, is_supported, fun(?ROOT_SESS_ID, SpaceId, ProviderId) ->
        {ok, #document{value = #od_space{providers = Providers}}} = GetSpaceFun(?ROOT_SESS_ID, SpaceId),
        maps:is_key(ProviderId, Providers)
    end),

    test_utils:mock_expect(Workers, space_logic, get_harvesters, fun(SpaceId) ->
        {ok, proplists:get_value(SpaceId, SpacesHarvesters, [])}
    end),
    
    test_utils:mock_expect(Workers, space_logic, report_provider_sync_progress, fun(_SpaceId, _) ->
        ok
    end).

-spec provider_logic_mock_setup(Config :: list(), Workers :: node() | [node()],
    proplists:proplist(), proplists:proplist(), [{binary(), [{binary(), non_neg_integer()}]}],
    [{binary(), binary()}], #{binary() => [binary()]}) -> ok.
provider_logic_mock_setup(_Config, AllWorkers, DomainMappings, SpacesSetup,
    SpacesToStorages, CustomStorages, StoragesSetupMap
) ->
    test_utils:mock_new(AllWorkers, provider_logic),

    ProvMap = lists:foldl(fun({SpaceId, SpaceConfig}, AccIn) ->
        Providers0 = proplists:get_value(<<"providers">>, SpaceConfig),
        lists:foldl(fun({CPid, _}, CAcc) ->
            ProvId0 = domain_to_provider_id(proplists:get_value(CPid, DomainMappings)),
            maps:put(ProvId0, maps:get(ProvId0, CAcc, []) ++ [SpaceId], CAcc)
        end, AccIn, Providers0)
    end, #{}, SpacesSetup),

    % Different arity of below functions must be mocked separately, because
    % they contain calls to the same module, which overrides the mock.
    GetProviderFun = fun(?ROOT_SESS_ID, PID) ->
        case maps:get(PID, ProvMap, undefined) of
            undefined ->
                {error, not_found};
            Spaces ->
                {ok, #document{key = PID, value = #od_provider{
                    name = PID,
                    subdomain_delegation = false,
                    domain = PID,  % domain is the same as Id
                    eff_spaces = maps:from_list(lists:map(fun(SpaceId) ->
                        Storages = proplists:get_value(SpaceId, SpacesToStorages,
                            maps:from_list([{St, 1000000000} || St <- CustomStorages])),
                        ProvidersSupp = maps:fold(fun({_StorageName, ProviderId}, Support, Acc) ->
                            maps:update_with(ProviderId, fun(PrevSupport) -> PrevSupport + Support end, Support, Acc)
                        end, #{}, Storages),
                        {SpaceId, maps:get(PID, ProvidersSupp)}
                    end, Spaces)),
                    storages = lists:usort(lists:foldl(fun({_SpaceId, SupportMap}, AccOut) ->
                        AccOut ++ lists:foldl(fun({StorageName, P}, AccIn) when P == PID -> [StorageName | AccIn];
                                                 (_, AccIn) -> AccIn
                        end, [], maps:keys(SupportMap))
                    end, [], SpacesToStorages) ++
                    lists:filtermap(fun({StorageName, P}) when P == PID -> {true, StorageName};
                                       (_) -> false
                    end, CustomStorages) ++ maps:keys(maps:get(PID, StoragesSetupMap, #{}))),
                    longitude = 0.0,
                    latitude = 0.0
                }}}
        end
    end,

    GetNameFun = fun(?ROOT_SESS_ID, PID) ->
        {ok, #document{value = #od_provider{name = Name}}} = GetProviderFun(?ROOT_SESS_ID, PID),
        {ok, Name}
    end,

    GetDomainFun = fun(?ROOT_SESS_ID, PID) ->
        {ok, #document{value = #od_provider{domain = Domain}}} = GetProviderFun(?ROOT_SESS_ID, PID),
        {ok, Domain}
    end,

    GetNodesFun = fun(PID) ->
        {ok, Domain} = GetDomainFun(?ROOT_SESS_ID, PID),
        % Simulate the fact the some providers can be reached only by their domain,
        % but return the domain/IP consistently
        AllProviders = lists:sort(maps:keys(ProvMap)),
        case index_of(PID, AllProviders) rem 2 of
            0 ->
                {ok, [Domain]};
            1 ->
                {ok, IPsAtoms} = inet:getaddrs(binary_to_list(Domain), inet),
                {ok, [list_to_binary(inet:ntoa(IP)) || IP <- IPsAtoms]}
        end
    end,

    GetSupportsFun = fun(?ROOT_SESS_ID, PID) ->
        {ok, #document{value = #od_provider{eff_spaces = Supports}}} = GetProviderFun(?ROOT_SESS_ID, PID),
        {ok, Supports}
    end,

    GetSpacesFun = fun(?ROOT_SESS_ID, PID) ->
        {ok, Supports} = GetSupportsFun(?ROOT_SESS_ID, PID),
        {ok, maps:keys(Supports)}
    end,

    SupportsSpaceFun = fun(?ROOT_SESS_ID, ProviderId, SpaceId) ->
        {ok, Spaces} = GetSpacesFun(?ROOT_SESS_ID, ProviderId),
        lists:member(SpaceId, Spaces)
    end,

    GetSupportSizeFun = fun(?ROOT_SESS_ID, PID, SpaceId) ->
        {ok, Supports} = GetSupportsFun(?ROOT_SESS_ID, PID),
        case maps:find(SpaceId, Supports) of
            {ok, Support} -> {ok, Support};
            error -> {error, not_found}
        end
    end,

    GetStorageIdsFun = fun(ProviderId) ->
        {ok, #document{value = #od_provider{storages = StorageIds}}} =
            GetProviderFun(?ROOT_SESS_ID, ProviderId),
        {ok, StorageIds}
    end,

    HasEffUserFun = fun(?ROOT_SESS_ID, ProviderId, UserId) ->
        {ok, Spaces} = GetSpacesFun(?ROOT_SESS_ID, ProviderId),
        lists:any(fun(SpaceId) ->
            {ok, Users} = space_logic:get_eff_users(?ROOT_SESS_ID, SpaceId),
            maps:is_key(UserId, Users)
        end, Spaces)
    end,

    test_utils:mock_expect(AllWorkers, provider_logic, get, GetProviderFun),

    test_utils:mock_expect(AllWorkers, provider_logic, get,
        fun(PID) ->
            GetProviderFun(?ROOT_SESS_ID, PID)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get,
        fun() ->
            GetProviderFun(?ROOT_SESS_ID, oneprovider:get_id())
        end),


    test_utils:mock_expect(AllWorkers, provider_logic, get_protected_data, fun(?ROOT_SESS_ID, PID) ->
        case GetProviderFun(?ROOT_SESS_ID, PID) of
            {ok, Doc = #document{value = Provider}} ->
                {ok, Doc#document{value = Provider#od_provider{
                    subdomain_delegation = undefined,
                    subdomain = undefined,
                    eff_spaces = #{},
                    eff_users = [],
                    eff_groups = []
                }}};
            Error ->
                Error
        end
    end),


    test_utils:mock_expect(AllWorkers, provider_logic, get_name, GetNameFun),

    test_utils:mock_expect(AllWorkers, provider_logic, get_name,
        fun(PID) ->
            GetNameFun(?ROOT_SESS_ID, PID)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_name,
        fun() ->
            GetNameFun(?ROOT_SESS_ID, oneprovider:get_id())
        end),


    test_utils:mock_expect(AllWorkers, provider_logic, get_domain, GetDomainFun),

    test_utils:mock_expect(AllWorkers, provider_logic, get_domain,
        fun(PID) ->
            GetDomainFun(?ROOT_SESS_ID, PID)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_domain,
        fun() ->
            GetDomainFun(?ROOT_SESS_ID, oneprovider:get_id())
        end),


    test_utils:mock_expect(AllWorkers, provider_logic, get_nodes, GetNodesFun),


    test_utils:mock_expect(AllWorkers, provider_logic, get_spaces, GetSpacesFun),

    test_utils:mock_expect(AllWorkers, provider_logic, get_spaces,
        fun(PID) ->
            GetSpacesFun(?ROOT_SESS_ID, PID)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_spaces,
        fun() ->
            GetSpacesFun(?ROOT_SESS_ID, oneprovider:get_id())
        end),


    test_utils:mock_expect(AllWorkers, provider_logic, get_storage_ids,
        fun(PID) ->
            GetStorageIdsFun(PID)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_storage_ids,
        fun() ->
            GetStorageIdsFun(oneprovider:get_id())
        end),


    test_utils:mock_expect(AllWorkers, provider_logic, get_storage_ids,
        fun(PID) ->
            GetStorageIdsFun(PID)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_storage_ids,
        fun() ->
            GetStorageIdsFun(oneprovider:get_id())
        end),


    test_utils:mock_expect(AllWorkers, provider_logic, has_eff_user,
        fun(UserId) ->
            HasEffUserFun(?ROOT_SESS_ID, oneprovider:get_id(), UserId)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, has_eff_user,
        HasEffUserFun
    ),


    test_utils:mock_expect(AllWorkers, provider_logic, supports_space,
        fun(SpaceId) ->
            SupportsSpaceFun(?ROOT_SESS_ID, oneprovider:get_id(), SpaceId)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, supports_space,
        SupportsSpaceFun
    ),


    test_utils:mock_expect(AllWorkers, provider_logic, get_support_size,
        fun(SpaceId) ->
            GetSupportSizeFun(?ROOT_SESS_ID, oneprovider:get_id(), SpaceId)
        end),


    test_utils:mock_expect(AllWorkers, provider_logic, zone_time_seconds,
        fun() ->
            time_utils:cluster_time_seconds()
        end),


    test_utils:mock_expect(AllWorkers, provider_logic, zone_time_seconds,
        fun() ->
            time_utils:cluster_time_seconds()
        end),


    test_utils:mock_expect(AllWorkers, provider_logic, assert_zone_compatibility,
        fun() ->
            ok
        end),

    VerifyProviderIdentityFun = fun
        (?DUMMY_PROVIDER_IDENTITY_TOKEN(ProviderId)) ->
            {ok, ?SUB(?ONEPROVIDER, ProviderId)};
        (_) ->
            ?ERROR_BAD_TOKEN
    end,

    test_utils:mock_expect(AllWorkers, provider_logic, verify_provider_identity, fun(_) -> ok end),

    test_utils:mock_expect(AllWorkers, token_logic, verify_provider_identity_token, VerifyProviderIdentityFun).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks cluster_logic module.
%% @end
%%--------------------------------------------------------------------
-spec cluster_logic_mock_setup(Workers :: node() | [node()]) -> ok.
cluster_logic_mock_setup(AllWorkers) ->
    test_utils:mock_new(AllWorkers, cluster_logic),
    test_utils:mock_expect(AllWorkers, cluster_logic, update_version_info, fun(_, _, _) ->
        ok
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks harvester_logic module.
%% @end
%%--------------------------------------------------------------------
-spec harvester_logic_mock_setup(Workers :: node() | [node()],
    proplists:proplist()) -> ok.
harvester_logic_mock_setup(Workers, HarvestersSetup) ->
    ok = test_utils:mock_new(Workers, harvester_logic),
    ok = test_utils:mock_expect(Workers, harvester_logic, get, fun(HarvesterId) ->
        Setup = proplists:get_value(HarvesterId, HarvestersSetup, []),
        Doc = #document{
            key = HarvesterId,
            value = #od_harvester{
                spaces = proplists:get_value(<<"spaces">>, Setup, []),
                indices = proplists:get_value(<<"indices">>, Setup, [])
            }},
        {ok, _} = put_into_cache(Doc),
        {ok, Doc}
    end).


-spec storage_logic_mock_setup(node() | [node()], map(),
    [{binary(), [#{{binary(), binary()} => non_neg_integer()}]}]) -> ok.
storage_logic_mock_setup(Workers, StoragesSetupMap, SpacesToStorages) ->
    StorageMap = maps:fold(fun(ProviderId, InitialStorageDesc, Acc) ->
        NewStorageDesc = maps:map(fun(StorageId, Desc) ->
            Desc1 = case Desc of
                [] -> #{};
                _ -> Desc
            end,
            QosParameters = maps:get(<<"qos_parameters">>, Desc1, #{}),
            ExtendedQosParameters = QosParameters#{
                <<"storageId">> => StorageId,
                <<"providerId">> => ProviderId
            },
            Desc1#{<<"provider_id">> => ProviderId, <<"qos_parameters">> => ExtendedQosParameters}
        end, InitialStorageDesc),
        maps:merge(Acc, NewStorageDesc)
    end, #{}, StoragesSetupMap),


    StoragesToSpaces = lists:foldl(fun({SpaceId, Map}, AccOut) ->
        Storages = lists:map(fun({S, _}) -> S end, maps:keys(Map)),
        lists:foldl(fun(Storage, AccIn) ->
            maps:update_with(Storage, fun(Spaces) -> [SpaceId | Spaces] end, [SpaceId], AccIn)
        end, AccOut, Storages)
    end, #{}, SpacesToStorages),

    GetStorageFun = fun(SM) ->
        fun (<<"all">>) ->
                % This is useful when changing storage QoS parameters. Used only in mock.
                {ok, SM};
            (StorageId) ->
                StorageDesc = maps:get(StorageId, SM, #{}),
                {ok, #document{value = #od_storage{
                    % storage name is equal to its id
                    name = StorageId,
                    qos_parameters = maps:get(<<"qos_parameters">>, StorageDesc, #{})
                }}}
        end
    end,

    GetQosParametersFun = fun(StorageId) ->
        {ok, #document{value = #od_storage{qos_parameters = QosParameters}}} = storage_logic:get(StorageId),
        {ok, QosParameters}
    end,

    ok = test_utils:mock_new(Workers, storage_logic),

    ok = test_utils:mock_expect(Workers, storage_logic, get, GetStorageFun(StorageMap)),

    ok = test_utils:mock_expect(Workers, storage_logic, get_qos_parameters_of_local_storage, GetQosParametersFun),

    ok = test_utils:mock_expect(Workers, storage_logic, get_qos_parameters_of_remote_storage,
        fun(StorageId,_) ->
            GetQosParametersFun(StorageId)
        end),

    ok = test_utils:mock_expect(Workers, storage_logic, get_provider,
        fun(StorageId) ->
            {ok, maps:get(<<"provider_id">>, maps:get(StorageId, StorageMap, #{}), #{})}
        end),

    ok = test_utils:mock_expect(Workers, storage_logic, get_name,
        % storage name is equal to its id
        fun(#document{key = Id}) -> {ok, Id};
            (Id) -> {ok, Id}
        end),

    ok = test_utils:mock_expect(Workers, storage_logic, get_spaces,
        fun(StorageId) -> {ok, maps:get(StorageId, StoragesToSpaces, [])} end),

    % NOTE this function changes qos parameters only on the node where it was executed
    ok = test_utils:mock_expect(Workers, storage_logic, set_qos_parameters,
        fun(StorageId, QosParameters) ->
            % erlang:apply needed to evade dialyzer warnings
            {ok, PreviousStorageMap} = erlang:apply(storage_logic, get, [<<"all">>]),
            PreviousStorageDesc = maps:get(StorageId, PreviousStorageMap, #{}),
            NewStorageDesc = PreviousStorageDesc#{<<"qos_parameters">> => QosParameters},
            NewStorageMap = PreviousStorageMap#{StorageId => NewStorageDesc},
            ok = meck:expect(storage_logic, get, GetStorageFun(NewStorageMap))
        end).


-spec storage_mock_teardown(Workers :: node() | [node()]) -> ok.
storage_mock_teardown(Workers) ->
    test_utils:mock_unload(Workers, storage_logic).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns true if storage configured by ProviderConfig should have
%% imported storage value set to true.
%% @end
%%--------------------------------------------------------------------
-spec maybe_set_imported_storage_value(proplists:proplist()) -> boolean().
maybe_set_imported_storage_value(ProviderConfig) ->
    case proplists:get_value(<<"imported_storage">>, ProviderConfig) of
        <<"true">> -> true;
        _ -> false
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Setup test storage.
%% @end
%%--------------------------------------------------------------------
-spec setup_storage(atom(), atom(), proplists:proplist(), list()) -> {ok, od_space:id()} | ok.
setup_storage(Worker, Domain, ProviderConfig, Config) ->
    case proplists:get_value(<<"storage">>, ProviderConfig) of
        undefined ->
            case ?config({storage_id, Domain}, Config) of
                undefined ->
                    ok;
                StorageId ->
                    on_space_supported(Worker, StorageId, maybe_set_imported_storage_value(ProviderConfig))
            end;
        StorageName ->
            StorageId = case ?config({storage_id, Domain}, Config) of
                %if storage is not mocked, get StorageId
                undefined -> StorageName;
                StId -> StId
            end,
            on_space_supported(Worker, StorageId, maybe_set_imported_storage_value(ProviderConfig))
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Add space storage mapping
%% @end
%%--------------------------------------------------------------------
-spec on_space_supported(atom(), storage:id(), boolean()) -> any().
on_space_supported(Worker, StorageId, ImportedStorage) ->
    case ImportedStorage of
        true -> ok = rpc:call(Worker, storage_config, set_imported_storage, [StorageId, true]);
        false -> ok
    end.


-spec index_of(term(), [term()]) -> not_found | integer().
index_of(Value, List) ->
    WithIndices = lists:zip(List, lists:seq(1, length(List))),
    case lists:keyfind(Value, 1, WithIndices) of
        {Value, Index} -> Index;
        false -> not_found
    end.

-spec init_qos_bounded_cache(list()) -> ok.
init_qos_bounded_cache(Config) ->
    DifferentProvidersWorkers = get_different_domain_workers(Config),
    {Results, BadNodes} = rpc:multicall(
        DifferentProvidersWorkers, qos_bounded_cache, ensure_exists_for_all_spaces, []
    ),
    ?assertMatch([], BadNodes),
    lists:foreach(fun(Result) -> ?assertMatch(ok, Result) end, Results).


%% @private
local_ip_v4() ->
    {ok, Addrs} = inet:getifaddrs(),
    hd([
        Addr || {_, Opts} <- Addrs, {addr, Addr} <- Opts,
        size(Addr) == 4, Addr =/= {127,0,0,1}
    ]).
