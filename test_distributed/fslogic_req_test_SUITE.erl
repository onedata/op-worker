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
    fslogic_mkdir_and_rmdir_test,
    fslogic_read_dir_test
].

-define(TIMEOUT, timer:seconds(5)).

%%%====================================================================
%%% Test function
%%%====================================================================

fslogic_get_file_attr_test(Config) ->
    [Worker, _] = ?config(op_worker_nodes, Config),
    Ctx = ?config(fslogic_ctx, Config),
    lists:foreach(fun({Name, Path}) ->
        ?assertMatch(#fuse_response{status = #status{code = ?OK},
            fuse_response = #file_attr{
                name = Name, type = ?DIRECTORY_TYPE, mode = 8#644
            }
        }, rpc:call(
            Worker, fslogic_req_generic, get_file_attr, [Ctx, {path, Path}]
        ))
    end, [
        {<<"user_id">>, <<"/">>}, {<<"spaces">>, <<"/spaces">>},
        {<<"default_space_name">>, <<"/spaces/default_space_name">>}
    ]),
    ?assertMatch(#fuse_response{status = #status{code = ?ENOENT}}, rpc:call(
        Worker, fslogic_req_generic, get_file_attr, [Ctx,
            {path, <<"/spaces/default_space_name/dir">>}]
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
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    file_meta_mock_setup(Workers),
    gr_spaces_mock_setup(Workers, <<"default_space_id">>, <<"default_space_name">>),
    session_setup(Worker, SessId, Iden, Config).

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
-spec session_setup(Worker :: node(), SessId :: session:id(),
    Iden :: session:identity(), Config :: term()) -> NewConfig :: term().
session_setup(Worker, SessId, Iden, Config) ->
    Self = self(),
    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Self])),
    {ok, #document{value = Session}} = rpc:call(Worker, session, get, [SessId]),
    {ok, _} = rpc:call(Worker, onedata_user, create, [
        #document{key = <<"user_id">>, value = #onedata_user{
            name = <<"test_user">>, space_ids = [<<"default_space_id">>]
        }}
    ]),
    ?assertEqual({ok, onedata_user_setup}, test_utils:receive_msg(
        onedata_user_setup, ?TIMEOUT)),
    [{session_id, SessId}, {fslogic_ctx, #fslogic_ctx{session = Session}} | Config].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes existing test session.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), Config :: term()) -> NewConfig :: term().
session_teardown(Worker, Config) ->
    SessId = ?config(session_id, Config),
    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])),
    ?assertEqual(ok, rpc:call(Worker, onedata_user, delete, [<<"user_id">>])),
    proplists:delete(session_id, proplists:delete(fslogic_ctx, Config)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks gr_spaces module, so that it returns default space details for default
%% space ID.
%% @end
%%--------------------------------------------------------------------
-spec gr_spaces_mock_setup(Workers :: node() | [node()],
    DefaultSpaceId :: binary(), DefaultSpaceName :: binary()) -> ok.
gr_spaces_mock_setup(Workers, DefaultSpaceId, DefaultSpaceName) ->
    test_utils:mock_new(Workers, gr_spaces),
    test_utils:mock_expect(Workers, gr_spaces, get_details,
        fun(provider, SpaceId) when SpaceId =:= DefaultSpaceId ->
            {ok, #space_details{name = DefaultSpaceName}}
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