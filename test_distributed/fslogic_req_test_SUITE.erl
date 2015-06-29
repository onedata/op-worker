%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of sequencer manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_test_SUITE).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    fslogic_get_file_attr_test/1,
    fslogic_mkdir_and_rmdir_test/1,
    fslogic_read_dir_test/1
]).

-performance({test_cases, []}).
all() -> [
    fslogic_get_file_attr_test,
%%     fslogic_mkdir_and_rmdir_test,
%%     fslogic_read_dir_test
].

-define(TIMEOUT, timer:seconds(5)).

-define(req(W, SessId, FuseRequest), rpc:call(W, worker_proxy, call, [fslogic_worker, {fuse_request, SessId, FuseRequest}])).

%%%====================================================================
%%% Test function
%%%====================================================================

fslogic_get_file_attr_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),

    {SessId1, UserId1} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},
    {SessId2, UserId2} = {?config({session_id, 2}, Config), ?config({user_id, 2}, Config)},

    lists:foreach(fun({SessId, Name, Mode, UID, Path}) ->
        ?assertMatch(#fuse_response{status = #status{code = ?OK},
            fuse_response = #file_attr{
                name = Name, type = ?DIRECTORY_TYPE, mode = Mode,
                uid = UID
            }
        }, ?req(Worker, SessId, #get_file_attr{entry = {path, Path}}))
    end, [
        {SessId1, UserId1, 8#770, UserId1, <<"/">>},
        {SessId2, UserId2, 8#770, UserId2, <<"/">>},
        {SessId1, <<"spaces">>, 8#755, 0, <<"/spaces">>},
        {SessId2, <<"spaces">>, 8#755, 0, <<"/spaces">>},
        {SessId1, <<"space_name1">>, 8#770, 0, <<"/spaces/space_name1">>},
        {SessId2, <<"space_name2">>, 8#770, 0, <<"/spaces/space_name2">>}
    ]),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, ?req(Worker,
        SessId1, #get_file_attr{entry = {path, <<"/spaces/space_name1/dir">>}}
    )).

fslogic_mkdir_and_rmdir_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    Ctx = ?config(fslogic_ctx, Config),

    RootFileAttr = rpc:call(Worker, fslogic_req_generic, get_file_attr, [Ctx, {path, <<"/">>}]),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr),
    #fuse_response{fuse_response = #file_attr{uuid = RootUUID}} = RootFileAttr,

    {_, _, UUIDs} = lists:foldl(fun(Leaf, {Path, ParentUUID, FileUUIDs}) ->
        NewPath = <<Path/binary, "/", Leaf/binary>>,

        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, rpc:call(
            Worker, fslogic_req_special, mkdir, [Ctx, ParentUUID, Leaf, 8#644]
        )),

        FileAttr = rpc:call(Worker, fslogic_req_generic, get_file_attr, [Ctx, {path, NewPath}]),
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
        ?assertEqual(FileAttr, rpc:call(Worker, fslogic_req_generic, get_file_attr,
            [Ctx, {path, <<"/spaces/default_space_name", NewPath/binary>>}])),
        #fuse_response{fuse_response = #file_attr{uuid = FileUUID}} = FileAttr,

        {NewPath, FileUUID, [FileUUID | FileUUIDs]}
    end, {<<>>, RootUUID, []}, [<<"dir1">>, <<"dir2">>, <<"dir3">>]),

    lists:foreach(fun(UUID) ->
        ?assertMatch(#fuse_response{status = #status{code = ?ENOTEMPTY}},
            rpc:call(Worker, fslogic_req_generic, delete_file, [Ctx, {uuid, UUID}]))
    end, lists:reverse(tl(UUIDs))),

    lists:foreach(fun(UUID) ->
        ?assertMatch(#fuse_response{status = #status{code = ?OK}},
            rpc:call(Worker, fslogic_req_generic, delete_file, [Ctx, {uuid, UUID}]))
    end, UUIDs).

fslogic_read_dir_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    Ctx = ?config(fslogic_ctx, Config),

    lists:foreach(fun({Name, Path}) ->
        FileAttr = rpc:call(Worker, fslogic_req_generic, get_file_attr, [Ctx, {path, Path}]),
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, FileAttr),
        #fuse_response{fuse_response = #file_attr{uuid = FileUUID}} = FileAttr,
        ?assertMatch(#fuse_response{status = #status{code = ?OK},
            fuse_response = #file_children{
                child_links = [#child_link{uuid = _, name = Name}]
            }
        }, rpc:call(
            Worker, fslogic_req_special, read_dir, [Ctx, {uuid, FileUUID}, 0, 10]
        ))
    end, [
        {<<"spaces">>, <<"/">>},
        {<<"default_space_name">>, <<"/spaces">>}
    ]),

    RootFileAttr = rpc:call(Worker, fslogic_req_generic, get_file_attr, [Ctx, {path, <<"/">>}]),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootFileAttr),
    #fuse_response{fuse_response = #file_attr{uuid = RootUUID}} = RootFileAttr,

    DefaultSpaceFileAttr = rpc:call(Worker, fslogic_req_generic, get_file_attr,
        [Ctx, {path, <<"/spaces/default_space_name">>}]),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, DefaultSpaceFileAttr),
    #fuse_response{fuse_response = #file_attr{uuid = DefaultSpaceUUID}} = DefaultSpaceFileAttr,

    lists:foreach(fun(Name) ->
        ?assertMatch(#fuse_response{status = #status{code = ?OK}}, rpc:call(
            Worker, fslogic_req_special, mkdir, [Ctx, RootUUID, Name, 8#644]
        ))
    end, [<<"dir1">>, <<"dir2">>, <<"dir3">>, <<"dir4">>, <<"dir5">>]),

    RootChildren = rpc:call(Worker, fslogic_req_special, read_dir,
        [Ctx, {uuid, RootUUID}, 0, 10]),
    DefaultSpaceChildren = rpc:call(Worker, fslogic_req_special, read_dir,
        [Ctx, {uuid, DefaultSpaceUUID}, 0, 10]),

    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, RootChildren),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}}, DefaultSpaceChildren),

    ?assertEqual(6, length(RootChildren#fuse_response.fuse_response#file_children.child_links)),

    ?assertEqual(length(RootChildren#fuse_response.fuse_response#file_children.child_links),
        length(DefaultSpaceChildren#fuse_response.fuse_response#file_children.child_links) + 1).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),

    file_meta_mock_setup(Workers),
    Space1 = {<<"space_id1">>, <<"space_name1">>},
    Space2 = {<<"space_id2">>, <<"space_name2">>},
    Space3 = {<<"space_id3">>, <<"space_name3">>},
    Space4 = {<<"space_id4">>, <<"space_name4">>},
    gr_spaces_mock_setup(Workers, [Space1, Space2, Space3, Space4]),

    User1 = {1, [<<"space_id1">>, <<"space_id2">>, <<"space_id3">>, <<"space_id4">>]},
    User2 = {2, [<<"space_id2">>, <<"space_id3">>, <<"space_id4">>]},
    User3 = {3, [<<"space_id3">>, <<"space_id4">>]},

    session_setup(Worker, [User1, User2, User3], Config).

end_per_testcase(_, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    session_teardown(Worker, Config),
    mocks_teardown(Workers, [file_meta, gr_spaces]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new test session.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node(), [{UserNum :: non_neg_integer(), [SpaceIds :: binary()]}], Config :: term()) -> NewConfig :: term().
session_setup(_Worker, [], Config) ->
    Config;
session_setup(Worker, [{UserNum, SpaceIds} | R], Config) ->
    Self = self(),

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
    ?assertEqual({ok, onedata_user_setup}, test_utils:receive_msg(
        onedata_user_setup, ?TIMEOUT)),
    [{{user_id, UserNum}, UserId}, {{session_id, UserNum}, SessId}, {{fslogic_ctx, UserNum}, #fslogic_ctx{session = Session}}
        | session_setup(Worker, R, Config)].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes existing test session.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), Config :: term()) -> NewConfig :: term().
session_teardown(Worker, Config) ->
    lists:foldl(fun
        ({{session_id, _}, SessId}, Acc) ->
            ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])),
            Acc;
        ({{user_id, _}, UserId}, Acc) ->
            ?assertEqual(ok, rpc:call(Worker, onedata_user, delete, [UserId])),
            Acc;
        ({{fslogic_ctx, _}, _}, Acc) ->
            Acc;
        (Elem, Acc) ->
            [Elem | Acc]
    end, [], Config).

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
%% @doc
%% Mocks file_meta module, so that creation of onedata user sends notification.
%% @end
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
%% @doc
%% Validates and unloads mocks.
%% @end
%%--------------------------------------------------------------------
-spec mocks_teardown(Workers :: node() | [node()],
    Modules :: module() | [module()]) -> ok.
mocks_teardown(Workers, Modules) ->
    test_utils:mock_validate(Workers, Modules),
    test_utils:mock_unload(Workers, Modules).

name(Text, Num) ->
    list_to_binary(Text ++ "_" ++ integer_to_list(Num)).