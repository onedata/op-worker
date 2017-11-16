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

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("public_key/include/public_key.hrl").

%% API
-export([setup_session/3, teardown_session/2, setup_storage/1, setup_storage/2, teardown_storage/1, clean_test_users_and_spaces/1,
    basic_session_setup/5, basic_session_teardown/2, remove_pending_messages/0, create_test_users_and_spaces/2,
    remove_pending_messages/1, clear_models/2, space_storage_mock/2,
    communicator_mock/1, clean_test_users_and_spaces_no_validate/1,
    domain_to_provider_id/1, mock_test_file_context/2, unmock_test_file_context/1]).
-export([enable_grpca_based_communication/1, disable_grpca_based_communication/1]).
-export([mock_provider_ids/1, unmock_provider_ids/1]).
-export([unload_quota_mocks/1, disable_quota_limit/1]).

-define(DUMMY_MACAROON(__UserId), <<"DUMMY-MACAROON-", __UserId/binary>>).

-record(user_config, {
    id :: od_user:id(),
    name :: binary(),
    default_space :: binary(),
    spaces :: [],
    groups :: [],
    macaroon :: binary()
}).

-define(TIMEOUT, timer:seconds(5)).
-define(DEFAULT_GLOBAL_SETUP, [
    {<<"users">>, [
        {<<"user1">>, [
            {<<"default_space">>, <<"space_id1">>}
        ]},
        {<<"user2">>, [
            {<<"default_space">>, <<"space_id2">>}
        ]},
        {<<"user3">>, [
            {<<"default_space">>, <<"space_id3">>}
        ]},
        {<<"user4">>, [
            {<<"default_space">>, <<"space_id4">>}
        ]}
    ]},
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
    atom_to_binary(Domain, unicode).

%%-------------------------------------------------------------------
%% @doc
%% Returns domain based on worker's provider id
%% @end
%%--------------------------------------------------------------------
-spec provider_id_to_domain(ProviderId :: binary()) -> atom().
provider_id_to_domain(ProviderId) ->
    binary_to_atom(ProviderId, unicode).

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

    test_utils:mock_validate_and_unload(Workers, [user_logic, group_logic,
        space_logic, provider_logic, space_storage]),
    unmock_provider_ids(Config).

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
        provider_logic, space_storage]).

%%--------------------------------------------------------------------
%% @doc
%% Creates basic test session.
%% @end
%%--------------------------------------------------------------------
-spec basic_session_setup(Worker :: node(), SessId :: session:id(),
    Iden :: session:identity(), Con :: pid(), Config :: term()) -> NewConfig :: term().
basic_session_setup(Worker, SessId, Iden, Con, Config) ->
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
        reuse_or_create_fuse_session, [SessId, Iden, Con])),
    [{session_id, SessId}, {identity, Iden} | Config].

%%--------------------------------------------------------------------
%% @doc
%% Removes basic test session.
%% @end
%%--------------------------------------------------------------------
-spec basic_session_teardown(Worker :: node(), Config :: term()) -> NewConfig :: term().
basic_session_teardown(Worker, Config) ->
    SessId = proplists:get_value(session_id, Config),
    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])).

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
%% Removes all records from models.
%% @end
%%--------------------------------------------------------------------
-spec clear_models(Worker :: node(), Names :: [atom()]) -> ok.
clear_models(Worker, Names) ->
    lists:foreach(fun(Name) ->
        {ok, Docs} = ?assertMatch({ok, _}, rpc:call(Worker, Name, list, [])),
        lists:foreach(fun(#document{key = Key}) ->
            ?assertEqual(ok, rpc:call(Worker, Name, delete, [Key]))
        end, Docs)
    end, Names).

%%--------------------------------------------------------------------
%% @doc
%% Setup test users' sessions on server
%% @end
%%--------------------------------------------------------------------
-spec setup_session(Worker :: node(), [#user_config{}], Config :: term()) -> NewConfig :: term().
setup_session(_Worker, [], Config) ->
    Config;
setup_session(Worker, [{_, #user_config{id = UserId, spaces = Spaces,
    macaroon = Macaroon, groups = Groups, name = UserName}} | R], Config) ->

    Name = fun(Text, User) ->
        list_to_binary(Text ++ "_" ++ binary_to_list(User)) end,

    SessId = Name(atom_to_list(?GET_DOMAIN(Worker)) ++ "_session_id", UserId),
    Iden = #user_identity{user_id = UserId},

    lists:foreach(fun({_, SpaceName}) ->
        case get(SpaceName) of
            undefined -> put(SpaceName, [SessId]);
            SessIds -> put(SpaceName, [SessId | SessIds])
        end
    end, Spaces),

    Auth = #macaroon_auth{macaroon = Macaroon},
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, fuse, Iden, Auth, []])),
    Ctx = rpc:call(Worker, user_ctx, new, [SessId]),
    [
        {{spaces, UserId}, Spaces},
        {{groups, UserId}, Groups},
        {{user_id, UserId}, UserId},
        {{auth, UserId}, Auth},
        {{user_name, UserId}, UserName},
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
            case rpc:call(Worker, session, get_auth, [SessId]) of
                {ok, Auth} ->
                    rpc:call(Worker, user_identity, delete, [Auth]),
                    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId]));
                {error, not_found} ->
                    ok
            end,
            Acc;
        ({{spaces, _}, Spaces}, Acc) ->
            {SpaceIds, _SpaceNames} = lists:unzip(Spaces),
            lists:foreach(fun(SpaceId) ->
                ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)]))
            end, SpaceIds),
            Acc;
        ({{user_id, _}, UserId}, Acc) ->
            ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [fslogic_uuid:user_root_dir_uuid(UserId)])),
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
    Config;
setup_storage([Worker | Rest], Config) ->
    TmpDir = generator:gen_storage_dir(),
    %% @todo: use shared storage
    "" = rpc:call(Worker, os, cmd, ["mkdir -p " ++ TmpDir]),
    StorageDoc = storage:new(
        <<"Test", (list_to_binary(atom_to_list(?GET_DOMAIN(Worker))))/binary>>,
        [helper:new_posix_helper(
            list_to_binary(TmpDir),
            #{},
            helper:new_posix_user_ctx(0, 0)
        )]
    ),
    {ok, StorageId} = rpc:call(Worker, storage, create, [StorageDoc]),
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
    lists:foreach(fun(Worker) ->
        teardown_storage(Worker, Config) end, DomainWorkers).

%%--------------------------------------------------------------------
%% @doc
%% Mocks space_storage module, so that it returns default storage for all spaces.
%% @end
%%--------------------------------------------------------------------
-spec space_storage_mock(Workers :: node() | [node()], StorageId :: storage:id()) -> ok.
space_storage_mock(Workers, StorageId) ->
    test_utils:mock_new(Workers, space_storage),
    test_utils:mock_new(Workers, space_strategies),
    test_utils:mock_expect(Workers, space_storage, get, fun(_) ->
        {ok, #document{value = #space_storage{
            storage_ids = [StorageId]
        }}}
    end),
    test_utils:mock_expect(Workers, space_storage, get_storage_ids,
        fun(_) -> [StorageId] end),
    test_utils:mock_expect(Workers, space_strategies, get, fun(_) ->
        {ok, #document{
            value = #space_strategies{
                storage_strategies = maps:put(StorageId, #storage_strategies{}, #{})
            }
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Mocks communicator module, so that it ignores all messages.
%% @end
%%--------------------------------------------------------------------
-spec communicator_mock(Workers :: node() | [node()]) -> ok.
communicator_mock(Workers) ->
    catch test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun(_, _) -> ok end),
    test_utils:mock_expect(Workers, communicator, send, fun(_, _, _) -> ok end).


%%--------------------------------------------------------------------
%% @doc
%% Can only be used after create_test_users_and_spaces is run, which mocks
%% modules required by provider listener to run.
%% @end
%%--------------------------------------------------------------------
-spec enable_grpca_based_communication(Config :: list()) -> ok.
enable_grpca_based_communication(Config) ->
    AllWorkers = ?config(op_worker_nodes, Config),
    DomainMappings = [{atom_to_binary(K, utf8), V} || {K, V} <- ?config(domain_mappings, Config)],

    %% Enable grp certs
    test_utils:mock_new(AllWorkers, [oz_plugin, provider_auth_manager]),
    CertMappings = lists:map(fun({ProvKey, Domain}) ->
        CertPath0 = ?TEST_FILE(Config, binary_to_list(ProvKey) ++ "_" ++ "cert.pem"),
        KeyPath0 = ?TEST_FILE(Config, binary_to_list(ProvKey) ++ "_" ++ "key.pem"),
        CertPath = re:replace(CertPath0, ".*/test_distributed/", "../../build/test_distributed/", [{return, list}]),
        KeyPath = re:replace(KeyPath0, ".*/test_distributed/", "../../build/test_distributed/", [{return, list}]),


        test_utils:mock_expect(get_same_domain_workers(Config, Domain), oz_plugin, get_cert_file,
            fun() -> CertPath end),
        test_utils:mock_expect(get_same_domain_workers(Config, Domain), oz_plugin, get_key_file,
            fun() -> KeyPath end),

        {ok, PEMBin} = file:read_file(CertPath0),
        [{_, CertDer, _}] = public_key:pem_decode(PEMBin),
        {CertDer, Domain}
    end, DomainMappings),

    test_utils:mock_expect(AllWorkers, provider_auth_manager, is_provider,
        fun(CertToCheck) ->
            case lists:keyfind(CertToCheck, 1, CertMappings) of
                false -> false;
                _ -> true
            end
        end),

    test_utils:mock_expect(AllWorkers, provider_auth_manager, get_provider_id,
        fun(CertToCheck) ->
            domain_to_provider_id(proplists:get_value(CertToCheck, CertMappings))
        end),

    % Save OZ CA cert (that signed provider certs) under expected location
    % on all nodes and run the listeners.
    OzCaPath = rpc:call(hd(AllWorkers), oz_plugin, get_oz_cacert_path, []),
    {ok, OzCaPem} = file:read_file(?TEST_FILE(Config, "ozp_cacert.pem")),
    lists:foreach(
        fun(Worker) ->
            rpc:call(Worker, utils, save_file, [OzCaPath, OzCaPem]),
            rpc:call(Worker, provider_listener, ensure_started, [])
        end, AllWorkers).

-spec disable_grpca_based_communication(Config :: list()) -> ok.
disable_grpca_based_communication(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [oz_plugin, provider_auth_manager]).

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
    test_utils:mock_expect(Workers, space_quota, soft_assert_write,
        fun(_, _) -> ok end),

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
%% Mocks oneprovider module which makes the providers think they have been given
%% certain ID by onezone.
%% @end
%%--------------------------------------------------------------------
-spec mock_provider_ids(proplists:proplist()) -> ok.
mock_provider_ids(Config) ->
    AllWorkers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(AllWorkers, oneprovider),
    Domains = lists:usort([?GET_DOMAIN(W) || W <- AllWorkers]),
    lists:foreach(fun(Domain) ->
        CWorkers = get_same_domain_workers(Config, Domain),
        ProviderId = domain_to_provider_id(Domain),
        test_utils:mock_expect(CWorkers, oneprovider, get_provider_id, fun() ->
            ProviderId
        end),
        ok = test_utils:mock_expect(CWorkers, oneprovider, is_registered, fun() ->
            true
        end)
    end, Domains).

%%--------------------------------------------------------------------
%% @doc
%% Cleans up after mocking provider ids.
%% @end
%%--------------------------------------------------------------------
-spec unmock_provider_ids(proplists:proplist()) -> ok.
unmock_provider_ids(Config) ->
    AllWorkers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(AllWorkers, [oneprovider]).

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
    ConfigJSON = json_utils:decode(ConfigJSONBin),

    GlobalSetup = proplists:get_value(<<"test_global_setup">>, ConfigJSON, ?DEFAULT_GLOBAL_SETUP),
    DomainMappings = [{atom_to_binary(K, utf8), V} || {K, V} <- ?config(domain_mappings, Config)],
    SpacesSetup = proplists:get_value(<<"spaces">>, GlobalSetup),
    UsersSetup = proplists:get_value(<<"users">>, GlobalSetup),
    Domains = lists:usort([?GET_DOMAIN(W) || W <- AllWorkers]),

    mock_provider_ids(Config),
    MasterWorkers = lists:map(fun(Domain) ->
        [MWorker | _] = CWorkers = get_same_domain_workers(Config, Domain),
        case ?config({storage_id, Domain}, Config) of
            undefined -> ok;
            StorageId ->
                case ?config(space_storage_mock, Config, false) of
                    true ->
%%                        % If storage mock was configured, mock space_storage model
                        initializer:space_storage_mock(CWorkers, StorageId);
                    _ -> ok
                end
        end,
        MWorker
    end, Domains),

    %% Setup storage
    lists:foreach(fun({SpaceId, _}) ->
        rpc:multicall(MasterWorkers, space_storage, delete, [SpaceId])
    end, SpacesSetup),
    lists:foreach(fun({SpaceId, SpaceConfig}) ->
        Providers0 = proplists:get_value(<<"providers">>, SpaceConfig),
        lists:foreach(fun({PID, ProviderConfig}) ->
            Domain = proplists:get_value(PID, DomainMappings),
            case get_same_domain_workers(Config, Domain) of
                [Worker | _] ->
                    setup_storage(Worker, SpaceId, Domain, ProviderConfig, Config);
                _ -> ok
            end
        end, Providers0)
    end, SpacesSetup),

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

    UserToSpaces0 = lists:foldl(fun({SpaceId, Users}, AccIn) ->
        lists:foldl(fun(UserId, CAcc) ->
            maps:put(UserId, maps:get(UserId, CAcc, []) ++ [{SpaceId, proplists:get_value(SpaceId, Spaces)}], CAcc)
        end, AccIn, Users)
    end, #{}, SpaceUsers),

    UserToSpaces = maps:map(fun(UserId, SpacesList) ->
        UserConfig = proplists:get_value(UserId, UsersSetup),
        DefaultSpaceId = proplists:get_value(<<"default_space">>, UserConfig),
        DefaultSpace = {DefaultSpaceId, proplists:get_value(DefaultSpaceId, SpacesList)},
        [DefaultSpace | SpacesList -- [DefaultSpace]]
    end, UserToSpaces0),

    UserToGroups = lists:foldl(fun({GroupId, Users}, AccIn) ->
        lists:foldl(fun(UserId, CAcc) ->
            maps:put(UserId, maps:get(UserId, CAcc, []) ++ [{GroupId, proplists:get_value(GroupId, Groups)}], CAcc)
        end, AccIn, Users)
    end, #{}, GroupUsers),

    SpacesToProviders = lists:map(fun({SpaceId, SpaceConfig}) ->
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
        ProviderSupp = maps:from_list(lists:map(fun({PID, Info}) ->
            {PID, proplists:get_value(<<"supported_size">>, Info, 0)}
        end, Providers1)),
        {SpaceId, ProviderSupp}
    end, SpacesSetup),

    Users = maps:fold(fun(UserId, SpacesList, AccIn) ->
        UserConfig = proplists:get_value(UserId, UsersSetup),
        DefaultSpaceId = proplists:get_value(<<"default_space">>, UserConfig),
        Macaroon = ?DUMMY_MACAROON(UserId),
        Name = fun(Text, User) ->
            list_to_binary(Text ++ "_" ++ binary_to_list(User)) end,
        AccIn ++ [{UserId, #user_config{
            id = UserId,
            name = Name("name", UserId),
            spaces = SpacesList,
            macaroon = Macaroon,
            default_space = DefaultSpaceId,
            groups = maps:get(UserId, UserToGroups, [])
        }}
        ]
    end, [], UserToSpaces),

    user_logic_mock_setup(AllWorkers, Users),
    group_logic_mock_setup(AllWorkers, Groups, GroupUsers),
    space_logic_mock_setup(AllWorkers, Spaces, SpaceUsers, SpacesToProviders),
    provider_logic_mock_setup(Config, AllWorkers, DomainMappings, SpacesSetup),

    %% Set expiration time for session to 1d.
    {_, []} = rpc:multicall(AllWorkers, application, set_env, [?APP_NAME, fuse_session_ttl_seconds, 240 * 60 * 60]),

    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1))
    end, AllWorkers),
    rpc:multicall(AllWorkers, worker_proxy, call, [dbsync_worker, streams_healthcheck]),

    lists:foreach(
        fun({_, #user_config{id = UserId, spaces = Spaces}}) ->
            [rpc:call(W, file_meta, setup_onedata_user, [UserId, proplists:get_keys(Spaces)]) || W <- AllWorkers]
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
-spec user_logic_mock_setup(Workers :: node() | [node()],
    [{UserNum :: integer(), Spaces :: [{binary(), binary()}],
        DefaultSpace :: binary(), Groups :: [{binary(), binary()}]}]) ->
    ok.
user_logic_mock_setup(Workers, Users) ->
    test_utils:mock_new(Workers, user_logic),

    UserConfigToUserDoc = fun(UserConfig) ->
        #user_config{
            name = UName, id = UID,
            groups = Groups,
            spaces = Spaces, default_space = DefaultSpaceId
        } = UserConfig,
        {SpaceIds, _} = lists:unzip(Spaces),
        {GroupIds, _} = lists:unzip(Groups),
        {ok, #document{key = UID, value = #od_user{
            name = UName,
            linked_accounts = [],
            email_list = [],
            login = <<>>,
            default_space = DefaultSpaceId,
            eff_spaces = SpaceIds,
            eff_groups = GroupIds
        }}}
    end,

    UsersByAuth = lists:flatmap(
        fun({UserId, #user_config{macaroon = Macaroon}}) -> [
            % Allow both for macaroons and tokens
            {#macaroon_auth{macaroon = Macaroon}, UserId},
            {#token_auth{token = Macaroon}, UserId}
        ]
        end, Users),

    GetUserFun = fun
        F(_, _, ?ROOT_USER_ID) ->
            {ok, #document{key = ?ROOT_USER_ID, value = #od_user{name = <<"root">>}}};
        F(_, _, ?GUEST_USER_ID) ->
            {ok, #document{key = ?GUEST_USER_ID, value = #od_user{name = <<"nobody">>}}};
        F(Scope, ?ROOT_SESS_ID, UserId) when Scope =:= shared orelse Scope =:= protected ->
            case proplists:get_value(UserId, Users) of
                undefined ->
                    {error, not_found};
                UserConfig2 ->
                    UserConfigToUserDoc(UserConfig2)
            end;
        F(_, SessionId, UserId) ->
            {ok, #document{value = #session{
                identity = #user_identity{user_id = SessionUserId}
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
            end
    end,

    test_utils:mock_expect(Workers, user_logic, get_by_auth, fun(Auth) ->
        case proplists:get_value(Auth, UsersByAuth, undefined) of
            undefined ->
                {error, not_found};
            UserId ->
                UserConfigToUserDoc(proplists:get_value(UserId, Users))
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

    test_utils:mock_expect(Workers, user_logic, get_name, fun(Client, UserId) ->
        {ok, #document{value = #od_user{name = Name}}} = GetUserFun(protected, Client, UserId),
        {ok, Name}
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks group_logic module, so that it returns default group details for
%% default group Id.
%% @end
%%--------------------------------------------------------------------
-spec group_logic_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}], [{binary(), [binary()]}]) -> ok.
group_logic_mock_setup(Workers, Groups, Users) ->
    test_utils:mock_new(Workers, group_logic),

    GetGroupFun = fun(_, GroupId) ->
        GroupName = proplists:get_value(GroupId, Groups),
        UserIds = proplists:get_value(GroupId, Users, []),
        EffUsers = maps:from_list(
            [{UID, privileges:group_privileges()} || UID <- UserIds]
        ),
        Doc = #document{key = GroupId, value = #od_group{
            name = GroupName,
            eff_users = EffUsers,
            eff_children = #{}
        }},
        {ok, Doc}
    end,

    test_utils:mock_expect(Workers, group_logic, get, GetGroupFun),

    test_utils:mock_expect(Workers, group_logic, get_name, fun(Client, GroupId) ->
        {ok, #document{value = #od_group{name = Name}}} = GetGroupFun(Client, GroupId),
        {ok, Name}
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks space_logic module, so that it returns default space details for default
%% space ID.
%% @end
%%--------------------------------------------------------------------
-spec space_logic_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}], [{binary(), [binary()]}], [{binary(), [{binary(), non_neg_integer()}]}]) ->
    ok.
space_logic_mock_setup(Workers, Spaces, Users, SpacesToProviders) ->
    Domains = lists:usort([?GET_DOMAIN(W) || W <- Workers]),
    test_utils:mock_new(Workers, space_logic),

    GetSpaceFun = fun(_, SpaceId) ->
        SpaceName = proplists:get_value(SpaceId, Spaces),
        UserIds = proplists:get_value(SpaceId, Users, []),
        EffUsers = maps:from_list(
            [{UID, privileges:space_privileges()} || UID <- UserIds]
        ),
        {ok, #document{key = SpaceId, value = #od_space{
            name = SpaceName,
            providers = proplists:get_value(SpaceId, SpacesToProviders, maps:from_list([{domain_to_provider_id(D), 1000000000} || D <- Domains])),
            eff_users = EffUsers,
            eff_groups = #{}
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

    test_utils:mock_expect(Workers, space_logic, get_provider_ids, fun(Client, SpaceId) ->
        {ok, #document{value = #od_space{providers = Providers}}} = GetSpaceFun(Client, SpaceId),
        {ok, maps:keys(Providers)}
    end),

    test_utils:mock_expect(Workers, space_logic, get_providers_supports, fun(Client, SpaceId) ->
        {ok, #document{value = #od_space{providers = Providers}}} = GetSpaceFun(Client, SpaceId),
        {ok, Providers}
    end),

    test_utils:mock_expect(Workers, space_logic, has_eff_privilege, fun(Client, SpaceId, UserId, Privilege) ->
        {ok, #document{value = #od_space{eff_users = EffUsers}}} = GetSpaceFun(Client, SpaceId),
        lists:member(Privilege, maps:get(UserId, EffUsers, []))
    end),

    test_utils:mock_expect(Workers, space_logic, is_supported, fun(?ROOT_SESS_ID, SpaceId, ProviderId) ->
        Providers = proplists:get_value(SpaceId, SpacesToProviders),
        lists:member(ProviderId, maps:keys(Providers))
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks provider_logic module, so that it returns default provider details for
%% default provider Id.
%% @end
%%--------------------------------------------------------------------
-spec provider_logic_mock_setup(Config :: list(), Workers :: node() | [node()],
    proplists:proplist(), proplists:proplist()) -> ok.
provider_logic_mock_setup(Config, AllWorkers, DomainMappings, SpacesSetup) ->
    test_utils:mock_new(AllWorkers, provider_logic),

    ProvMap = lists:foldl(fun({SpaceId, SpaceConfig}, AccIn) ->
        Providers0 = proplists:get_value(<<"providers">>, SpaceConfig),
        lists:foldl(fun({CPid, _}, CAcc) ->
            ProvId0 = domain_to_provider_id(proplists:get_value(CPid, DomainMappings)),
            maps:put(ProvId0, maps:get(ProvId0, CAcc, []) ++ [SpaceId], CAcc)
        end, AccIn, Providers0)
    end, #{}, SpacesSetup),

    % Different arities of below functions must be mocked separately, because
    % they contain calls to the same module, which overrides the mock.
    GetProviderFun = fun(?ROOT_SESS_ID, PID) ->
        Domain = provider_id_to_domain(PID),
        Workers = get_same_domain_workers(Config, Domain),

        case maps:get(PID, ProvMap, undefined) of
            undefined ->
                {error, not_found};
            Spaces ->
                {ok, #document{key = PID, value = #od_provider{
                    name = PID,
                    urls = [list_to_binary(utils:get_host(Worker)) || Worker <- Workers],
                    spaces = maps:from_list([{S, 1000000000} || S <- Spaces])
                }}}
        end
    end,

    GetNameFun = fun(?ROOT_SESS_ID, PID) ->
        {ok, #document{value = #od_provider{name = Name}}} = GetProviderFun(?ROOT_SESS_ID, PID),
        {ok, Name}
    end,

    GetURLsFun = fun(?ROOT_SESS_ID, PID) ->
        {ok, #document{value = #od_provider{urls = URLs}}} = GetProviderFun(?ROOT_SESS_ID, PID),
        {ok, URLs}
    end,

    GetSpacesFun = fun(?ROOT_SESS_ID, PID) ->
        {ok, #document{value = #od_provider{spaces = Spaces}}} = GetProviderFun(?ROOT_SESS_ID, PID),
        {ok, maps:keys(Spaces)}
    end,

    test_utils:mock_expect(AllWorkers, provider_logic, get, GetProviderFun),

    test_utils:mock_expect(AllWorkers, provider_logic, get,
        fun(PID) ->
            GetProviderFun(?ROOT_SESS_ID, PID)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get,
        fun() ->
            GetProviderFun(?ROOT_SESS_ID, oneprovider:get_provider_id())
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_name, GetNameFun),

    test_utils:mock_expect(AllWorkers, provider_logic, get_name,
        fun(PID) ->
            GetNameFun(?ROOT_SESS_ID, PID)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_name,
        fun() ->
            GetNameFun(?ROOT_SESS_ID, oneprovider:get_provider_id())
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_urls, GetURLsFun),

    test_utils:mock_expect(AllWorkers, provider_logic, get_urls,
        fun(PID) ->
            GetURLsFun(?ROOT_SESS_ID, PID)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_urls,
        fun() ->
            GetURLsFun(?ROOT_SESS_ID, oneprovider:get_provider_id())
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_spaces, GetSpacesFun),

    test_utils:mock_expect(AllWorkers, provider_logic, get_spaces,
        fun(PID) ->
            GetSpacesFun(?ROOT_SESS_ID, PID)
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, get_spaces,
        fun() ->
            GetSpacesFun(?ROOT_SESS_ID, oneprovider:get_provider_id())
        end),

    test_utils:mock_expect(AllWorkers, provider_logic, supports_space,
        fun(?ROOT_SESS_ID, ProviderId, SpaceId) ->
            case maps:get(ProviderId, ProvMap, undefined) of
                undefined ->
                    false;
                Spaces ->
                    lists:member(SpaceId, Spaces)
            end
        end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns true if space configured by ProviderConfig should be mounted
%% in root.
%% @end
%%--------------------------------------------------------------------
-spec maybe_mount_in_root(proplists:proplist()) -> boolean().
maybe_mount_in_root(ProviderConfig) ->
    case proplists:get_value(<<"mounted_in_root">>, ProviderConfig) of
        <<"true">> -> true;
        _ -> false
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Setup test storage.
%% @end
%%--------------------------------------------------------------------
-spec setup_storage(atom(), od_space:id(), atom(), proplists:proplist(), list()) ->
    {ok, od_space:id()} | ok.
setup_storage(Worker, SpaceId, Domain, ProviderConfig, Config) ->
    case proplists:get_value(<<"storage">>, ProviderConfig) of
        undefined ->
            case ?config({storage_id, Domain}, Config) of
                undefined ->
                    ok;
                StorageId ->
                    add_space_storage(Worker, SpaceId, StorageId,
                        maybe_mount_in_root(ProviderConfig))
            end;
        StorageName ->
            StorageId = case ?config({storage_id, Domain}, Config) of
                %if storage is not mocked, get StorageId
                undefined ->
                    {ok, Storage} = ?assertMatch({ok, _},
                        rpc:call(Worker, storage, select, [StorageName])),
                    rpc:call(Worker, storage, get_id, [Storage]);
                StId ->
                    StId
            end,
            add_space_storage(Worker, SpaceId, StorageId,
                maybe_mount_in_root(ProviderConfig))
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Add space storage mapping
%% @end
%%--------------------------------------------------------------------
-spec add_space_storage(atom(), od_space:id(), storage:id(), boolean()) -> any().
add_space_storage(Worker, SpaceId, StorageId, MountInRoot) ->
    ?assertMatch({ok, _},
        rpc:call(Worker, space_storage, add, [SpaceId, StorageId, MountInRoot])
    ).