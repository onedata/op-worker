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
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").

%% API
-export([setup_session/3, teardown_sesion/2, setup_storage/1, teardown_storage/1,
    create_test_users_and_spaces/1, clean_test_users_and_spaces/1,
    basic_session_setup/5, basic_session_teardown/2, remove_pending_messages/0,
    remove_pending_messages/1, clear_models/2, space_storage_mock/2,
    communicator_mock/1]).

-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Setup and mocking related with users and spaces
%%--------------------------------------------------------------------
-spec create_test_users_and_spaces(Config :: list()) -> list().
create_test_users_and_spaces(Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    StorageId = ?config(storage_id, Config),

    initializer:space_storage_mock(Workers, StorageId),

    Space1 = {<<"space_id1">>, <<"space_name1">>},
    Space2 = {<<"space_id2">>, <<"space_name2">>},
    Space3 = {<<"space_id3">>, <<"space_name3">>},
    Space4 = {<<"space_id4">>, <<"space_name4">>},
    Space5 = {<<"space_id5">>, <<"space_name">>},
    Space6 = {<<"space_id6">>, <<"space_name">>},

    Group1 = {<<"group_id1">>, <<"group_name1">>},
    Group2 = {<<"group_id2">>, <<"group_name2">>},
    Group3 = {<<"group_id3">>, <<"group_name3">>},
    Group4 = {<<"group_id4">>, <<"group_name4">>},

    User1 = {1, [Space1, Space2, Space3, Space4], [Group1, Group2, Group3, Group4]},
    User2 = {2, [Space2, Space3, Space4], [Group2, Group3, Group4]},
    User3 = {3, [Space3, Space4], [Group3, Group4]},
    User4 = {4, [Space4], [Group4]},
    User5 = {5, [Space5, Space6], []},

    file_meta_mock_setup(Workers),
    oz_spaces_mock_setup(Workers, [Space1, Space2, Space3, Space4, Space5, Space6]),
    oz_groups_mock_setup(Workers, [Group1, Group2, Group3, Group4]),

    initializer:setup_session(Worker, [User1, User2, User3, User4, User5], Config).

%%--------------------------------------------------------------------
%% @doc Cleanup and unmocking related with users and spaces
%%--------------------------------------------------------------------
-spec clean_test_users_and_spaces(Config :: list()) -> term().
clean_test_users_and_spaces(Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),

    initializer:teardown_sesion(Worker, Config),
    test_utils:mock_validate_and_unload(Workers, [file_meta, oz_spaces, oz_groups, space_storage]).

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
%% @doc Setups test storage on server and creates test storage dir
%%--------------------------------------------------------------------
-spec setup_storage(Config :: list()) -> list().
setup_storage(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    TmpDir = generator:gen_storage_dir(Config),
    %% @todo: use shared storage
    "" = rpc:call(Worker, os, cmd, ["mkdir -p " ++ TmpDir]),
    {ok, StorageId} = rpc:call(
        Worker, storage, create, [
            #document{value = fslogic_storage:new_storage(
                <<"Test">>,
                [fslogic_storage:new_helper_init(
                    <<"DirectIO">>,
                    #{<<"root_path">> => list_to_binary(TmpDir)}
                )]
            )}]),
    [{storage_id, StorageId}, {storage_dir, TmpDir} | Config].

%%--------------------------------------------------------------------
%% @doc Removes test storage dir
%%--------------------------------------------------------------------
-spec teardown_storage(Config :: list()) -> string().
teardown_storage(Config) ->
    TmpDir = ?config(storage_dir, Config),
    [Worker | _] = ?config(op_worker_nodes, Config),
    "" = rpc:call(Worker, os, cmd, ["rm -rf " ++ TmpDir]).

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
%% @private
%% @doc Appends Num to Text and converts it to binary.
%%--------------------------------------------------------------------
-spec name(Text :: string(), Num :: integer()) -> binary().
name(Text, Num) ->
    list_to_binary(Text ++ "_" ++ integer_to_list(Num)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks oz_spaces module, so that it returns default space details for default
%% space ID.
%% @end
%%--------------------------------------------------------------------
-spec oz_spaces_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}]) -> ok.
oz_spaces_mock_setup(Workers, Spaces) ->
    test_utils:mock_new(Workers, oz_spaces),
    test_utils:mock_expect(Workers, oz_spaces, get_details,
        fun(provider, SpaceId) ->
            SpaceName = proplists:get_value(SpaceId, Spaces),
            {ok, #space_details{id = SpaceId, name = SpaceName}}
        end
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks oz_groups module, so that it returns default group details for default
%% group ID.
%% @end
%%--------------------------------------------------------------------
-spec oz_groups_mock_setup(Workers :: node() | [node()],
    [{binary(), binary()}]) -> ok.
oz_groups_mock_setup(Workers, Groups) ->
    test_utils:mock_new(Workers, oz_groups),
    test_utils:mock_expect(Workers, oz_groups, get_details,
        fun({user, _}, GroupId) ->
            GroupName = proplists:get_value(GroupId, Groups),
            {ok, #group_details{id = GroupId, name = GroupName}}
        end
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc Mocks file_meta module, so that creation of onedata user sends notification.
%%--------------------------------------------------------------------
-spec file_meta_mock_setup(Workers :: node() | [node()]) -> ok.
file_meta_mock_setup(Workers) ->
    Self = self(),
    test_utils:mock_new(Workers, file_meta),
    test_utils:mock_expect(Workers, file_meta, 'after',
        fun(onedata_user, create, _, _, {ok, UUID}) ->
            file_meta:setup_onedata_user(UUID),
            Self ! onedata_user_setup
        end
    ).