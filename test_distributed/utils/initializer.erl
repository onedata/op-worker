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
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([setup_session/3, teardown_sesion/2, setup_storage/1, teardown_storage/1,
    create_test_users_and_spaces/1, clean_test_users_and_spaces/1]).

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

    file_meta_mock_setup(Workers),
    Space1 = {<<"space_id1">>, <<"space_name1">>},
    Space2 = {<<"space_id2">>, <<"space_name2">>},
    Space3 = {<<"space_id3">>, <<"space_name3">>},
    Space4 = {<<"space_id4">>, <<"space_name4">>},
    gr_spaces_mock_setup(Workers, [Space1, Space2, Space3, Space4]),

    User1 = {1, [Space1, Space2, Space3, Space4]},
    User2 = {2, [Space2, Space3, Space4]},
    User3 = {3, [Space3, Space4]},
    User4 = {4, [Space4]},

    initializer:setup_session(Worker, [User1, User2, User3, User4], Config).

%%--------------------------------------------------------------------
%% @doc Cleanup and unmocking related with users and spaces
%%--------------------------------------------------------------------
-spec clean_test_users_and_spaces(Config :: list()) -> term().
clean_test_users_and_spaces(Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),

    initializer:teardown_sesion(Worker, Config),
    mocks_teardown(Workers, [file_meta, gr_spaces]).

%%--------------------------------------------------------------------
%% @doc Setup test users' sessions on server
%%--------------------------------------------------------------------
-spec setup_session(Worker :: node(), [{UserNum :: non_neg_integer(),
    [Spaces :: {binary(), binary()}]}], Config :: term()) -> NewConfig :: term().
setup_session(_Worker, [], Config) ->
    Config;
setup_session(Worker, [{UserNum, Spaces} | R], Config) ->
    Self = self(),

    {SpaceIds, _SpaceNames} = lists:unzip(Spaces),

    Name = fun(Text, Num) -> name(Text, Num) end,

    SessId = Name("session_id", UserNum),
    UserId = Name("user_id", UserNum),
    Iden = #identity{user_id = UserId},
    UserName = Name("username", UserNum),

    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Self])),
    {ok, #document{value = Session}} = rpc:call(Worker, session, get, [SessId]),
    {ok, _} = rpc:call(Worker, onedata_user, create, [
        #document{key = UserId, value = #onedata_user{
            name = UserName, space_ids = SpaceIds
        }}
    ]),
    ?assertReceivedMatch(onedata_user_setup, ?TIMEOUT),
    [
        {{spaces, UserNum}, Spaces}, {{user_id, UserNum}, UserId}, {{session_id, UserNum}, SessId},
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
            ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])),
            Acc;
        ({{spaces, _}, Spaces}, Acc) ->
            {SpaceIds, _SpaceNames} = lists:unzip(Spaces),
            lists:foreach(fun(SpaceId) ->
                ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [SpaceId]))
            end, SpaceIds),
            Acc;
        ({{user_id, _}, UserId}, Acc) ->
            ?assertEqual(ok, rpc:call(Worker, onedata_user, delete, [UserId])),
            ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [UserId])),
            ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [fslogic_path:spaces_uuid(UserId)])),
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
    {ok, StorageId} = rpc:call(Worker, storage, create, [#document{value = fslogic_storage:new_storage(<<"Test">>,
        [fslogic_storage:new_helper_init(<<"DirectIO">>, #{<<"root_path">> => list_to_binary(TmpDir)})])}]),
    [{storage_id, StorageId}, {storage_dir, TmpDir} | Config].

%%--------------------------------------------------------------------
%% @doc Removes test storage dir
%%--------------------------------------------------------------------
-spec teardown_storage(Config :: list()) -> string().
teardown_storage(Config) ->
    TmpDir = ?config(storage_dir, Config),
    [Worker | _] = ?config(op_worker_nodes, Config),
    "" = rpc:call(Worker, os, cmd, ["rm -rf " ++ TmpDir]).

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
%% Mocks gr_spaces module, so that it returns default space details for default
%% space ID.
%% @end
%%--------------------------------------------------------------------
-spec gr_spaces_mock_setup(Workers :: node() | [node()],
  [{binary(), binary()}]) -> ok.
gr_spaces_mock_setup(Workers, Spaces) ->
    test_utils:mock_new(Workers, gr_spaces),
    test_utils:mock_expect(Workers, gr_spaces, get_details,
        fun(provider, SpaceId) ->
            {_, SpaceName} = lists:keyfind(SpaceId, 1, Spaces),
            {ok, #space_details{name = SpaceName}}
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

%%--------------------------------------------------------------------
%% @private
%% @doc Validates and unloads mocks.
%%--------------------------------------------------------------------
-spec mocks_teardown(Workers :: node() | [node()],
  Modules :: module() | [module()]) -> ok.
mocks_teardown(Workers, Modules) ->
    test_utils:mock_validate(Workers, Modules),
    test_utils:mock_unload(Workers, Modules).