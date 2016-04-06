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
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/client_messages.hrl").

%% API
-export([setup_session/3, teardown_sesion/2, setup_storage/1, teardown_storage/1,
    create_test_users_and_spaces/1, clean_test_users_and_spaces/1,
    basic_session_setup/5, basic_session_teardown/2, remove_pending_messages/0,
    remove_pending_messages/1, clear_models/2, space_storage_mock/2,
    communicator_mock/1, clean_test_users_and_spaces_no_validate/1]).

-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Setup and mocking related with users and spaces, done on each provider
%%--------------------------------------------------------------------
-spec create_test_users_and_spaces(Config :: list()) -> list().
create_test_users_and_spaces(Config) ->
    DomainWorkers = get_different_domain_workers(Config),
    create_test_users_and_spaces(DomainWorkers, Config).

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
    test_utils:mock_validate_and_unload(Workers, [file_meta, oz_spaces, oz_groups, space_storage]).


%%TODO this function can be deleted after resolving VFS-1811 and replacing call
%%to this function in cdmi_test_SUITE with call to clean_test_users_and_spaces.
%%--------------------------------------------------------------------
%% @doc Cleanup and unmocking related with users and spaces
%%--------------------------------------------------------------------
-spec clean_test_users_and_spaces_no_validate(Config :: list()) -> term().
clean_test_users_and_spaces_no_validate(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    DomainWorkers = get_different_domain_workers(Config),

    lists:foreach(fun(W) ->
        initializer:teardown_sesion(W, Config),
        clear_cache(W)
    end, DomainWorkers),
    test_utils:mock_unload(Workers, [file_meta, oz_spaces, oz_groups, space_storage]).


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
    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
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
-spec setup_session(Worker :: node(), [{UserNum :: non_neg_integer(),
    [Spaces :: {binary(), binary()}], [Groups :: {binary(), binary()}]}], Config :: term()) -> NewConfig :: term().
setup_session(_Worker, [], Config) ->
    Config;
setup_session(Worker, [{UserNum, Spaces, Groups} | R], Config) ->
    Self = self(),

    {SpaceIds, _SpaceNames} = lists:unzip(Spaces),
    {GroupIds, _GroupNames} = lists:unzip(Groups),

    Name = fun(Text, Num) -> name(Text, Num) end,

    SessId = Name("session_id", UserNum),
    UserId = Name("user_id", UserNum),
    Iden = #identity{user_id = UserId},
    UserName = Name("username", UserNum),

    ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
        reuse_or_create_fuse_session, [SessId, Iden, Self])),
    {ok, #document{value = Session}} = rpc:call(Worker, session, get, [SessId]),
    {ok, _} = rpc:call(Worker, onedata_user, create, [
        #document{key = UserId, value = #onedata_user{
            name = UserName, space_ids = SpaceIds, group_ids = GroupIds
        }}
    ]),
    [{ok, _} = rpc:call(Worker, onedata_group, get_or_fetch, [Id, #auth{}]) || Id <- GroupIds],
    ?assertReceivedMatch(onedata_user_setup, ?TIMEOUT),
    [
        {{spaces, UserNum}, Spaces},
        {{groups, UserNum}, Groups},
        {{user_id, UserNum}, UserId},
        {{user_name, UserNum}, UserName},
        {{session_id, UserNum}, SessId},
        {{fslogic_ctx, UserNum}, #fslogic_ctx{session = Session}}
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
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun(_, _) -> ok end),
    test_utils:mock_expect(Workers, communicator, send, fun(_, _, _) -> ok end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Setup and mocking related with users and spaces, done on one provider
%%--------------------------------------------------------------------
-spec create_test_users_and_spaces([Worker :: node()], Config :: list()) -> list().
create_test_users_and_spaces([], Config) ->
    Config;
create_test_users_and_spaces([Worker | Rest], Config) ->
    StorageId = ?config({storage_id, ?GET_DOMAIN(Worker)}, Config),
    SameDomainWorkers = get_same_domain_workers(Config, ?GET_DOMAIN(Worker)),
    initializer:space_storage_mock(SameDomainWorkers, StorageId),

    Space1 = {<<"space_id1">>, <<"space_name1">>},
    Space2 = {<<"space_id2">>, <<"space_name2">>},
    Space3 = {<<"space_id3">>, <<"space_name3">>},
    Space4 = {<<"space_id4">>, <<"space_name4">>},
    Space5 = {<<"space_id5">>, <<"space_name">>},
    Space6 = {<<"space_id6">>, <<"space_name">>},
    Spaces = [Space1, Space2, Space3, Space4, Space5, Space6],

    Group1 = {<<"group_id1">>, <<"group_name1">>},
    Group2 = {<<"group_id2">>, <<"group_name2">>},
    Group3 = {<<"group_id3">>, <<"group_name3">>},
    Group4 = {<<"group_id4">>, <<"group_name4">>},
    Groups = [Group1, Group2, Group3, Group4],

    User1 = {1, [Space1, Space2, Space3, Space4], [Group1, Group2, Group3, Group4]},
    User2 = {2, [Space2, Space3, Space4], [Group2, Group3, Group4]},
    User3 = {3, [Space3, Space4], [Group3, Group4]},
    User4 = {4, [Space4], [Group4]},
    User5 = {5, [Space5, Space6], []},

    SpaceUsers = [
        {<<"space_id1">>, [<<"user_id1">>]},
        {<<"space_id2">>, [<<"user_id1">>, <<"user_id2">>]},
        {<<"space_id3">>, [<<"user_id1">>, <<"user_id2">>, <<"user_id3">>]},
        {<<"space_id4">>, [<<"user_id1">>, <<"user_id2">>, <<"user_id3">>, <<"user_id4">>]},
        {<<"space_id5">>, [<<"user_id5">>]},
        {<<"space_id6">>, [<<"user_id5">>]}
    ],

    GroupUsers = [
        {<<"group_id1">>, [<<"user_id1">>]},
        {<<"group_id2">>, [<<"user_id1">>, <<"user_id2">>]},
        {<<"group_id3">>, [<<"user_id1">>, <<"user_id2">>, <<"user_id3">>]},
        {<<"group_id4">>, [<<"user_id1">>, <<"user_id2">>, <<"user_id3">>, <<"user_id4">>]}
    ],

    file_meta_mock_setup(SameDomainWorkers),
    oz_spaces_mock_setup(SameDomainWorkers, Spaces, SpaceUsers),
    oz_groups_mock_setup(SameDomainWorkers, Groups, GroupUsers),

    proplists:compact(
        initializer:setup_session(Worker, [User1, User2, User3, User4, User5], Config)
        ++ create_test_users_and_spaces(Rest, Config)
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
%% @doc Appends Num to Text and converts it to binary.
%%--------------------------------------------------------------------
-spec name(Text :: string(), Num :: integer()) -> binary().
name(Text, Num) ->
    list_to_binary(Text ++ integer_to_list(Num)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks oz_spaces module, so that it returns default space details for default
%% space ID.
%% @end
%%--------------------------------------------------------------------
-spec oz_spaces_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}], [{binary(), [binary()]}]) ->
    ok.
oz_spaces_mock_setup(Workers, Spaces, Users) ->
    test_utils:mock_new(Workers, oz_spaces),
    test_utils:mock_expect(Workers, oz_spaces, get_details,
        fun(provider, SpaceId) ->
            SpaceName = proplists:get_value(SpaceId, Spaces),
            {ok, #space_details{id = SpaceId, name = SpaceName}}
        end
    ),

    test_utils:mock_expect(Workers, oz_spaces, get_users,
        fun(provider, SpaceId) ->
            {ok, proplists:get_value(SpaceId, Users)} end),
    test_utils:mock_expect(Workers, oz_spaces, get_groups,
        fun(provider, _) -> {ok, []} end),

    test_utils:mock_expect(Workers, oz_spaces, get_user_privileges,
        fun(provider, _, _) -> {ok, privileges:space_privileges()} end),
    test_utils:mock_expect(Workers, oz_spaces, get_group_privileges,
        fun(provider, _, _) -> {ok, privileges:group_privileges()} end).

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
        fun({user, _}, GroupId) ->
            {ok, proplists:get_value(GroupId, Users)} end),
    test_utils:mock_expect(Workers, oz_groups, get_spaces,
        fun({user, _}, _) -> {ok, []} end),

    test_utils:mock_expect(Workers, oz_groups, get_user_privileges,
        fun({user, _}, _, _) -> {ok, privileges:space_privileges()} end).

%%--------------------------------------------------------------------
%% @private
%% @doc Mocks file_meta module, so that creation of onedata user sends notification.
%%--------------------------------------------------------------------
-spec file_meta_mock_setup(Workers :: node() | [node()]) -> ok.
file_meta_mock_setup(Workers) ->
    Self = self(),
    Handler = fun(UUID) ->
        file_meta:setup_onedata_user(UUID),
        Self ! onedata_user_setup
    end,
    test_utils:mock_new(Workers, file_meta),
    test_utils:mock_expect(Workers, file_meta, 'after', fun
        (onedata_user, create_or_update, _, _, {ok, UUID}) -> Handler(UUID);
        (onedata_user, create, _, _, {ok, UUID}) -> Handler(UUID)
    end).