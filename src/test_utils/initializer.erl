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

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("ctool/include/oz/oz_groups.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/oz/oz_providers.hrl").
-include_lib("public_key/include/public_key.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/client_messages.hrl").

%% API
-export([setup_session/3, teardown_sesion/2, setup_storage/1, setup_storage/2, teardown_storage/1, clean_test_users_and_spaces/1,
    basic_session_setup/5, basic_session_teardown/2, remove_pending_messages/0, create_test_users_and_spaces/2,
    remove_pending_messages/1, clear_models/2, space_storage_mock/2,
    communicator_mock/1, clean_test_users_and_spaces_no_validate/1,
    domain_to_provider_id/1, assume_all_files_in_space/2, clear_assume_all_files_in_space/1]).
-export([enable_grpca_based_communication/1, disable_grpca_based_communication/1]).

-record(user_config, {
    id :: onedata_user:id(),
    name :: binary(),
    default_space :: binary(),
    spaces :: [],
    groups :: [],
    macaroon :: macaroon:macaroon()
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
                        {<<"storage">>, <<"/mnt/st1">>}
                    ]}
                ]}
            ]},
            {<<"space_id2">>, [
                {<<"displayed_name">>, <<"space_name2">>},
                {<<"users">>, [<<"user1">>, <<"user2">>]},
                {<<"groups">>, [<<"group1">>, <<"group2">>]},
                {<<"providers">>, [
                    {<<"p1">>, [
                        {<<"storage">>, <<"/mnt/st1">>}
                    ]}
                ]}
            ]},
            {<<"space_id3">>, [
                {<<"displayed_name">>, <<"space_name3">>},
                {<<"users">>, [<<"user1">>, <<"user2">>, <<"user3">>]},
                {<<"groups">>, [<<"group1">>, <<"group2">>, <<"group3">>]},
                {<<"providers">>, [
                    {<<"p1">>, [
                        {<<"storage">>, <<"/mnt/st1">>}
                    ]}
                ]}
            ]},
            {<<"space_id4">>, [
                {<<"displayed_name">>, <<"space_name4">>},
                {<<"users">>, [<<"user1">>, <<"user2">>, <<"user3">>, <<"user4">>]},
                {<<"groups">>, [<<"group1">>, <<"group2">>, <<"group3">>, <<"group4">>]},
                {<<"providers">>, [
                    {<<"p1">>, [
                        {<<"storage">>, <<"/mnt/st1">>}
                    ]}
                ]}
            ]}
        ]}
]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc Makes workers 'think' that all files belong to given SpaceId.
%%--------------------------------------------------------------------
-spec assume_all_files_in_space(Config :: list(), SpaceId :: binary()) -> ok.
assume_all_files_in_space(Config, SpaceId) ->
    SpaceDoc = #document{key = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId), value = #file_meta{}},
    Workers = ?config(op_worker_nodes, Config),
    catch test_utils:mock_new(Workers, fslogic_spaces),
    test_utils:mock_expect(Workers, fslogic_spaces, get_space,
        fun(_, _) ->
            {ok, SpaceDoc}
        end).


%%-------------------------------------------------------------------
%% @doc Reverses assume_all_files_in_space/2
%%--------------------------------------------------------------------
-spec clear_assume_all_files_in_space(Config :: list()) -> ok.
clear_assume_all_files_in_space(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [fslogic_spaces]).


%%-------------------------------------------------------------------
%% @doc Returns provider id based on worker's domain
%%--------------------------------------------------------------------
-spec domain_to_provider_id(Domain :: atom()) -> binary().
domain_to_provider_id(Domain) ->
    atom_to_binary(Domain, unicode).

%%-------------------------------------------------------------------
%% @doc Returns domain based on worker's provider id
%%--------------------------------------------------------------------
-spec provider_id_to_domain(ProviderId :: binary()) -> atom().
provider_id_to_domain(ProviderId) ->
    binary_to_atom(ProviderId, unicode).


%%--------------------------------------------------------------------
%% @doc Setup and mocking related with users and spaces, done on each provider
%%--------------------------------------------------------------------
-spec create_test_users_and_spaces(ConfigPath :: string(), JsonConfig :: list()) -> list().
create_test_users_and_spaces(ConfigPath, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    create_test_users_and_spaces(Workers, ConfigPath, Config).

%%--------------------------------------------------------------------
%% @doc Cleanup and unmocking related with users and spaces
%%--------------------------------------------------------------------
-spec clean_test_users_and_spaces(Config :: list()) -> term().
clean_test_users_and_spaces(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    DomainWorkers = get_different_domain_workers(Config),

    lists:foreach(fun(W) ->
        initializer:teardown_sesion(W, Config),
        clear_cache(W)
    end, DomainWorkers),
    test_utils:mock_validate_and_unload(Workers, [file_meta, oz_spaces, oz_users,
        oz_groups, space_storage, oneprovider, oz_providers]).


%%TODO this function can be deleted after resolving VFS-1811 and replacing call
%%to this function in cdmi_test_SUITE with call to clean_test_users_and_spaces.
%%--------------------------------------------------------------------
%% @doc Cleanup and unmocking related with users and spaces
%%--------------------------------------------------------------------
-spec clean_test_users_and_spaces_no_validate(Config :: list()) -> term().
clean_test_users_and_spaces_no_validate(Config) ->
    Workers = ?config(op_worker_nodes, Config),

    lists:foreach(fun(W) ->
        initializer:teardown_sesion(W, Config),
        clear_cache(W)
    end, Workers),
    test_utils:mock_unload(Workers, [file_meta, oz_spaces, oz_groups, space_storage, oneprovider, oz_providers]).


clear_cache(W) ->
    A1 = rpc:call(W, caches_controller, wait_for_cache_dump, []),
%%     A2 = gen_server:call({?NODE_MANAGER_NAME, W}, clear_mem_synch, 60000),
%%     A3 = gen_server:call({?NODE_MANAGER_NAME, W}, force_clear_node, 60000),
%%     ?assertMatch({ok, ok, {ok, ok}}, {A1, A2, A3}).
    ok.

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
%% @doc Setup test users' sessions on server
%%--------------------------------------------------------------------
-spec setup_session(Worker :: node(), [#user_config{}], Config :: term()) -> NewConfig :: term().
setup_session(_Worker, [], Config) ->
    Config;
setup_session(Worker, [{_, #user_config{id = UserId, spaces = Spaces,
    macaroon = Macaroon, groups = Groups, name = UserName}} | R], Config) ->

    Name = fun(Text, User) -> list_to_binary(Text ++ "_" ++ binary_to_list(User))  end,

    SessId = Name("session_id", UserId),
    UserId = UserId,
    Iden = #identity{user_id = UserId},

    lists:foreach(fun({_, SpaceName}) ->
        case get(SpaceName) of
            undefined -> put(SpaceName, [SessId]);
            SessIds -> put(SpaceName, [SessId | SessIds])
        end
    end, Spaces),

    Auth = #auth{macaroon = Macaroon},
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, fuse, Iden, Auth, []])),
    {ok, #document{value = Session}} = rpc:call(Worker, session, get, [SessId]),
    {ok, _} = rpc:call(Worker, onedata_user, fetch, [{user, {Macaroon, []}}]),
    ?assertReceivedMatch(onedata_user_setup, ?TIMEOUT),
    [
        {{spaces, UserId}, Spaces},
        {{groups, UserId}, Groups},
        {{user_id, UserId}, UserId},
        {{auth, UserId}, Auth},
        {{user_name, UserId}, UserName},
        {{session_id, UserId}, SessId},
        {{fslogic_ctx, UserId}, #fslogic_ctx{session = Session}}
        | setup_session(Worker, R, Config)
    ].

%%--------------------------------------------------------------------
%% @doc Removes test users' sessions from server.
%%--------------------------------------------------------------------
-spec teardown_sesion(Worker :: node(), Config :: term()) -> NewConfig :: term().
teardown_sesion(Worker, Config) ->
    lists:foldl(fun
        ({{session_id, _}, SessId}, Acc) ->
            ?assertEqual(ok,
                rpc:call(Worker, session_manager, remove_session, [SessId])),
            Acc;
        ({{spaces, _}, Spaces}, Acc) ->
            {SpaceIds, _SpaceNames} = lists:unzip(Spaces),
            lists:foreach(fun(SpaceId) ->
                ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)]))
            end, SpaceIds),
            Acc;
        ({{user_id, _}, UserId}, Acc) ->
            ?assertEqual(ok, rpc:call(Worker, onedata_user, delete, [UserId])),
            ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [fslogic_uuid:default_space_uuid(UserId)])),
            ?assertEqual(ok,
                rpc:call(Worker, file_meta, delete,
                    [fslogic_uuid:spaces_uuid(UserId)]
                )),
            Acc;
        ({{fslogic_ctx, _}, _}, Acc) ->
            Acc;
        (Elem, Acc) ->
            [Elem | Acc]
    end, [], Config).

%%--------------------------------------------------------------------
%% @doc Setups test storage on server and creates test storage dir on each provider
%%--------------------------------------------------------------------
-spec setup_storage(Config :: list()) -> list().
setup_storage(Config) ->
    DomainWorkers = get_different_domain_workers(Config),
    setup_storage(DomainWorkers, Config).

%%--------------------------------------------------------------------
%% @doc Setups test storage on server and creates test storage dir on one provider
%%--------------------------------------------------------------------
-spec setup_storage([node()], Config :: list()) -> list().
setup_storage([], Config) ->
    Config;
setup_storage([Worker | Rest], Config) ->
    TmpDir = generator:gen_storage_dir(),
    %% @todo: use shared storage
    "" = rpc:call(Worker, os, cmd, ["mkdir -p " ++ TmpDir]),
    {ok, StorageId} = rpc:call(
        Worker, storage, create, [
            #document{value = fslogic_storage:new_storage(
                <<"Test", (list_to_binary(atom_to_list(?GET_DOMAIN(Worker))))/binary>>,
                [fslogic_storage:new_helper_init(
                    <<"DirectIO">>,
                    #{<<"root_path">> => list_to_binary(TmpDir)}
                )]
            )}]),
    [{{storage_id, ?GET_DOMAIN(Worker)}, StorageId}, {{storage_dir, ?GET_DOMAIN(Worker)}, TmpDir}] ++
    setup_storage(Rest, Config).

%%--------------------------------------------------------------------
%% @doc Removes test storage dir on each provider
%%--------------------------------------------------------------------
-spec teardown_storage(Config :: list()) -> ok.
teardown_storage(Config) ->
    DomainWorkers = get_different_domain_workers(Config),
    lists:foreach(fun(Worker) ->
        teardown_storage(Worker, Config) end, DomainWorkers).

%%--------------------------------------------------------------------
%% @doc Mocks space_storage module, so that it returns default storage for all spaces.
%%--------------------------------------------------------------------
-spec space_storage_mock(Workers :: node() | [node()], StorageId :: storage:id()) -> ok.
space_storage_mock(Workers, StorageId) ->
    test_utils:mock_new(Workers, space_storage),
    test_utils:mock_expect(Workers, space_storage, get, fun(_) ->
        {ok, #document{value = #space_storage{storage_ids = [StorageId]}}}
    end).

%%--------------------------------------------------------------------
%% @doc Mocks communicator module, so that it ignores all messages.
%%--------------------------------------------------------------------
-spec communicator_mock(Workers :: node() | [node()]) -> ok.
communicator_mock(Workers) ->
    catch test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun(_, _) -> ok end),
    test_utils:mock_expect(Workers, communicator, send, fun(_, _, _) -> ok end).


-spec enable_grpca_based_communication(Config :: list()) -> ok.
enable_grpca_based_communication(Config) ->
    AllWorkers = ?config(op_worker_nodes, Config),
    DomainMappings = [{atom_to_binary(K, utf8), V} || {K, V} <- ?config(domain_mappings, Config)],

    %% Enable grp certs
    test_utils:mock_new(AllWorkers, [oz_plugin, provider_auth_manager]),
    CertMappings = lists:map(fun({ProvKey, Domain}) ->
        CertPath0 = ?TEST_FILE(Config, binary_to_list(ProvKey) ++ "_" ++ "cert.pem"),
        KeyPath0 = ?TEST_FILE(Config, binary_to_list(ProvKey) ++ "_" ++ "key.pem"),
        CertPath = re:replace(CertPath0, ".*/test_distributed/", "../../build/test_distributed/", [{return,list}]),
        KeyPath = re:replace(KeyPath0, ".*/test_distributed/", "../../build/test_distributed/", [{return,list}]),


        test_utils:mock_expect(get_same_domain_workers(Config, Domain), oz_plugin, get_cert_path,
            fun() -> CertPath end),
        test_utils:mock_expect(get_same_domain_workers(Config, Domain), oz_plugin, get_key_path,
            fun() -> KeyPath end),

        {ok, PEMBin} = file:read_file(CertPath0),
        [{_, DER, _}] = public_key:pem_decode(PEMBin),
        Cert = #'OTPCertificate'{} = public_key:pkix_decode_cert(DER, otp),

        {Cert, Domain}
    end, DomainMappings),

    test_utils:mock_expect(AllWorkers, provider_auth_manager, is_provider,
        fun(CertToCheck) ->
            case lists:keyfind(CertToCheck, 1, CertMappings) of
                false -> false;
                _     -> true
            end
        end),

    test_utils:mock_expect(AllWorkers, provider_auth_manager, get_provider_id,
        fun(CertToCheck) ->
            domain_to_provider_id(proplists:get_value(CertToCheck, CertMappings))
        end),

    ok.

-spec disable_grpca_based_communication(Config :: list()) -> ok.
disable_grpca_based_communication(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [oz_plugin, provider_auth_manager]).



%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Setup and mocking related with users and spaces on all given providers.
%%--------------------------------------------------------------------
-spec create_test_users_and_spaces([Worker :: node()], ConfigPath :: string(), Config :: list()) -> list().
create_test_users_and_spaces(AllWorkers, ConfigPath, Config) ->
    {ok, ConfigJSONBin} = file:read_file(ConfigPath),
    ConfigJSON = json_utils:decode(ConfigJSONBin),

    GlobalSetup = proplists:get_value(<<"test_global_setup">>, ConfigJSON, ?DEFAULT_GLOBAL_SETUP),
    DomainMappings = [{atom_to_binary(K, utf8), V} || {K, V} <- ?config(domain_mappings, Config)],
    SpacesSetup = proplists:get_value(<<"spaces">>, GlobalSetup),
    UsersSetup = proplists:get_value(<<"users">>, GlobalSetup),
    Domains = lists:usort([?GET_DOMAIN(W) || W <- AllWorkers]),
    MasterWorkers = lists:map(fun(Domain) ->
        [MWorker | _] = CWorkers = get_same_domain_workers(Config, Domain),
        ProviderId = domain_to_provider_id(Domain),
        test_utils:mock_expect(CWorkers, oneprovider, get_provider_id,
            fun() ->
                ProviderId
            end),

        case ?config({storage_id, Domain}, Config) of %% If storage mock was configured, mock space_storage model
            undefined -> ok;
            StorageId ->
                initializer:space_storage_mock(CWorkers, StorageId)
        end,

        MWorker
    end, Domains),

    catch test_utils:mock_new(AllWorkers, oneprovider),
    catch test_utils:mock_new(AllWorkers, oz_providers),

    %% Setup storage
    lists:foreach(fun({SpaceId, _}) ->
        rpc:multicall(MasterWorkers, space_storage, delete, [SpaceId])
    end, SpacesSetup),
    lists:foreach(fun({SpaceId, SpaceConfig}) ->
        Providers0 = proplists:get_value(<<"providers">>, SpaceConfig),
        lists:foreach(fun({PID, ProviderConfig}) ->
            Domain = proplists:get_value(PID, DomainMappings),
            [Worker | _] = get_same_domain_workers(Config, Domain),
            StorageName = proplists:get_value(<<"storage">>, ProviderConfig),
            {ok, Storage} = ?assertMatch({ok, _}, rpc:call(Worker,
                storage, get_by_name, [StorageName])),
            StorageId = rpc:call(Worker, storage, id, [Storage]),
            {ok, _} = ?assertMatch({ok, _}, rpc:call(Worker,
                space_storage, add, [SpaceId, StorageId]))
        end, Providers0)
    end, SpacesSetup),

    test_utils:mock_expect(AllWorkers, oz_providers, get_spaces,
        fun(PID) ->
            ProvMap = lists:foldl(fun({SpaceId, SpaceConfig}, AccIn) ->
                Providers0 = proplists:get_value(<<"providers">>, SpaceConfig),
                lists:foldl(fun({CPid, _}, CAcc) ->
                    ProvId0 = domain_to_provider_id(proplists:get_value(CPid, DomainMappings)),
                    maps:put(ProvId0, maps:get(ProvId0, CAcc, []) ++ [SpaceId], CAcc)
                end, AccIn, Providers0)
            end, #{}, SpacesSetup),

            {ok, maps:get(PID, ProvMap)}
        end),

    test_utils:mock_expect(AllWorkers, oz_providers, get_details,
        fun(_, PID) ->
            Domain = provider_id_to_domain(PID),
            Workers = get_same_domain_workers(Config, Domain),
            {ok, #provider_details{id = PID, urls = [utils:get_host(Worker) || Worker <- Workers]}}
        end),



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

    UserToSpaces = maps:map(fun(UserId, Spaces) ->
        UserConfig = proplists:get_value(UserId, UsersSetup),
        DefaultSpaceId = proplists:get_value(<<"default_space">>, UserConfig),
        DefaultSpace = {DefaultSpaceId, proplists:get_value(DefaultSpaceId, Spaces)},
        [DefaultSpace | Spaces -- [DefaultSpace]]
    end, UserToSpaces0),

    UserToGroups = lists:foldl(fun({GroupId, Users}, AccIn) ->
        lists:foldl(fun(UserId, CAcc) ->
            maps:put(UserId, maps:get(UserId, CAcc, []) ++ [{GroupId, proplists:get_value(GroupId, Groups)}], CAcc)
        end, AccIn, Users)
    end, #{}, GroupUsers),

    SpacesToProviders = lists:map(fun({SpaceId, SpaceConfig}) ->
        Providers = proplists:get_value(<<"providers">>, SpaceConfig),
        ProviderIds = [domain_to_provider_id(proplists:get_value(CPid, DomainMappings)) || {CPid, _} <- Providers],
        {SpaceId, ProviderIds}
    end, SpacesSetup),

    Users = maps:fold(fun(UserId, Spaces, AccIn) ->
        UserConfig = proplists:get_value(UserId, UsersSetup),
        DefaultSpaceId = proplists:get_value(<<"default_space">>, UserConfig),
        Macaroon = macaroon:create(<<"test">>, <<"key">>, UserId),
        Name = fun(Text, User) -> list_to_binary(Text ++ "_" ++ binary_to_list(User))  end,
        AccIn ++ [{Macaroon, #user_config{
                id = UserId,
                name = Name("name", UserId),
                spaces = Spaces,
                macaroon = Macaroon,
                default_space = DefaultSpaceId,
                groups = maps:get(UserId, UserToGroups, [])
            }}
        ]
    end, [], UserToSpaces),

    file_meta_mock_setup(AllWorkers),
    oz_users_mock_setup(AllWorkers, Users),
    oz_spaces_mock_setup(AllWorkers, Spaces, SpaceUsers, SpacesToProviders),
    oz_groups_mock_setup(AllWorkers, Groups, GroupUsers),

    proplists:compact(
        lists:flatten([initializer:setup_session(W, Users, Config) || W <- MasterWorkers])
    ).

%%--------------------------------------------------------------------
%% @doc
%% Get one worker from each provider domain.
%% @end
%%--------------------------------------------------------------------
-spec get_different_domain_workers(Config :: list()) -> [node()].
get_different_domain_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:usort(fun(W1, W2) -> ?GET_DOMAIN(W1) =< ?GET_DOMAIN(W2) end, Workers).


%%--------------------------------------------------------------------
%% @doc
%% Get workers with given domain
%% @end
%%--------------------------------------------------------------------
-spec get_same_domain_workers(Config :: list(), Domain :: atom()) -> [node()].
get_same_domain_workers(Config, Domain) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:filter(fun(W) -> ?GET_DOMAIN(W) =:= Domain end, Workers).

%%--------------------------------------------------------------------
%% @doc Removes test storage dir on given node
%%--------------------------------------------------------------------
-spec teardown_storage(Worker :: node(), Config :: list()) -> string().
teardown_storage(Worker, Config) ->
    TmpDir = ?config({storage_dir, ?GET_DOMAIN(Worker)}, Config),
    "" = rpc:call(Worker, os, cmd, ["rm -rf " ++ TmpDir]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks oz_users module, so that it returns user details, spaces and groups.
%% @end
%%--------------------------------------------------------------------
-spec oz_users_mock_setup(Workers :: node() | [node()],
    [{UserNum :: integer(), Spaces :: [{binary(), binary()}],
        DefaultSpace :: binary(), Groups :: [{binary(), binary()}]}]) ->
    ok.
oz_users_mock_setup(Workers, Users) ->
    test_utils:mock_new(Workers, oz_users),
    test_utils:mock_expect(Workers, oz_users, get_details, fun({user, {Macaroon, _}}) ->
        {_, #user_config{name = UName, id = UID}} = lists:keyfind(Macaroon, 1, Users),
        {ok, #user_details{
            id = UID,
            name = UName
        }}
    end),

    test_utils:mock_expect(Workers, oz_users, get_spaces, fun({user, {Macaroon, _}}) ->
        {_, #user_config{spaces = Spaces, default_space = DefaultSpaceId}} = lists:keyfind(Macaroon, 1, Users),
        {SpaceIds, _} = lists:unzip(Spaces),
        {ok, #user_spaces{ids = SpaceIds, default = DefaultSpaceId}}
    end),

    test_utils:mock_expect(Workers, oz_users, get_groups, fun({user, {Macaroon, _}}) ->
        {_, #user_config{groups = Groups}} = lists:keyfind(Macaroon, 1, Users),
        {GroupIds, _} = lists:unzip(Groups),
        {ok, GroupIds}
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks oz_spaces module, so that it returns default space details for default
%% space ID.
%% @end
%%--------------------------------------------------------------------
-spec oz_spaces_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}], [{binary(), [binary()]}], [{binary(), [binary()]}]) ->
    ok.
oz_spaces_mock_setup(Workers, Spaces, Users, SpacesToProviders) ->
    Domains = lists:usort([?GET_DOMAIN(W) || W <- Workers]),
    test_utils:mock_new(Workers, oz_spaces),
    test_utils:mock_expect(Workers, oz_spaces, get_details,
        fun(_, SpaceId) ->
            SpaceName = proplists:get_value(SpaceId, Spaces),
            {ok, #space_details{id = SpaceId, name = SpaceName}}
        end
    ),

    test_utils:mock_expect(Workers, oz_spaces, get_providers,
        fun(_, SpaceId) ->
            {ok, proplists:get_value(SpaceId, SpacesToProviders, [domain_to_provider_id(D) || D <- Domains])}
        end
    ),

    test_utils:mock_expect(Workers, oz_spaces, get_users,
        fun(_, SpaceId) ->
            {ok, proplists:get_value(SpaceId, Users, [])} end),
    test_utils:mock_expect(Workers, oz_spaces, get_groups,
        fun(_, _) -> {ok, []} end),

    test_utils:mock_expect(Workers, oz_spaces, get_user_privileges,
        fun(_, _, _) -> {ok, privileges:space_privileges()} end),
    test_utils:mock_expect(Workers, oz_spaces, get_group_privileges,
        fun(_, _, _) -> {ok, privileges:group_privileges()} end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks oz_groups module, so that it returns default group details for default
%% group ID.
%% @end
%%--------------------------------------------------------------------
-spec oz_groups_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}], [{binary(), [binary()]}]) -> ok.
oz_groups_mock_setup(Workers, Groups, Users) ->
    test_utils:mock_new(Workers, oz_groups),
    test_utils:mock_expect(Workers, oz_groups, get_details,
        fun({user, _}, GroupId) ->
            GroupName = proplists:get_value(GroupId, Groups),
            {ok, #group_details{id = GroupId, name = GroupName}}
        end
    ),

    test_utils:mock_expect(Workers, oz_groups, get_users,
        fun(_, GroupId) ->
            {ok, proplists:get_value(GroupId, Users)} end),
    test_utils:mock_expect(Workers, oz_groups, get_spaces,
        fun(_, _) -> {ok, []} end),

    test_utils:mock_expect(Workers, oz_groups, get_user_privileges,
        fun(_, _, _) -> {ok, privileges:space_privileges()} end).

%%--------------------------------------------------------------------
%% @private
%% @doc Mocks file_meta module, so that creation of onedata user sends notification.
%%--------------------------------------------------------------------
-spec file_meta_mock_setup(Workers :: node() | [node()]) -> ok.
file_meta_mock_setup(Workers) ->
    Self = self(),
    Handler = fun(UUID) ->
        file_meta:setup_onedata_user(provider, UUID),
        Self ! onedata_user_setup
    end,
    test_utils:mock_new(Workers, file_meta),
    test_utils:mock_expect(Workers, file_meta, 'after', fun
        (onedata_user, create_or_update, _, _, {ok, UUID}) -> Handler(UUID);
        (onedata_user, create, _, _, {ok, UUID}) -> Handler(UUID);
        (onedata_user, save, _, _, {ok, UUID}) -> Handler(UUID)
    end).