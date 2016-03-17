%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of rename
%%% @end
%%%-------------------------------------------------------------------
-module(rename_test_SUITE).
-author("Mateusz Paciorek").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    tmp_test/1
]).

all() ->
    ?ALL([
        tmp_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================
tmp_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),

    {ok, TargetDirUuid} = lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name2/target_dir">>),

    {ok, MovedDirUuid} = lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/moved_dir">>),
    {ok, StaticFile1Uuid} = lfm_proxy:create(W, SessId, <<"/spaces/space_name1/moved_dir/static_file1">>, 8#770),

    {ok, RenamedDirUuid} = lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/renamed_dir">>),
    {ok, StaticFile2Uuid} = lfm_proxy:create(W, SessId, <<"/spaces/space_name1/renamed_dir/static_file2">>, 8#770),

    {ok, MovedFileUuid} = lfm_proxy:create(W, SessId, <<"/spaces/space_name1/moved_file">>, 8#770),

    {ok, RenamedFileUuid} = lfm_proxy:create(W, SessId, <<"/spaces/space_name1/renamed_file">>, 8#770),

    {ok, #document{key = SourceRootUuid}} = rpc:call(W, file_meta, get_parent, [MovedDirUuid]),
    {ok, #document{key = TargetRootUuid}} = rpc:call(W, file_meta, get_parent, [TargetDirUuid]),

    print_dir_structures(W, SessId, SourceRootUuid, "SourceRoot"),
    print_dir_structures(W, SessId, TargetRootUuid, "TargetRoot"),
    print_dir_structures(W, SessId, TargetDirUuid, "TargetDir"),
    print_dir_structures(W, SessId, MovedDirUuid, "MovedDir"),
    print_dir_structures(W, SessId, RenamedDirUuid, "RenamedDir"),
    print_reg_structures(W, StaticFile1Uuid, "StaticFile1"),
    print_reg_structures(W, StaticFile2Uuid, "StaticFile2"),
    print_reg_structures(W, MovedFileUuid, "MovedFile"),
    print_reg_structures(W, RenamedFileUuid, "RenamedFile"),

    ct:print("###############################################"),
    timer:sleep(timer:seconds(5)),

    %% Rename file
    lfm_proxy:create(W, SessId, <<"/spaces/space_name1/renamed_file_target">>, 8#770), %% with overwrite
    ok = lfm_proxy:mv(W, SessId, {uuid, RenamedFileUuid}, <<"/spaces/space_name1/renamed_file_target">>),

    %% Move file
    lfm_proxy:create(W, SessId, <<"/spaces/space_name1/target_dir/moved_file_target">>, 8#770), %% with overwrite
    ok = lfm_proxy:mv(W, SessId, {uuid, MovedFileUuid}, <<"/spaces/space_name2/target_dir/moved_file_target">>),

    %% Rename dir
    lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/renamed_dir_target">>), %% with overwrite
    ok = lfm_proxy:mv(W, SessId, {uuid, RenamedDirUuid}, <<"/spaces/space_name1/renamed_dir_target">>),

    %% Move dir
    lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/target_dir/moved_dir_target">>, 8#770), %% with overwrite
    ok = lfm_proxy:mv(W, SessId, {uuid, MovedDirUuid}, <<"/spaces/space_name2/target_dir/moved_dir_target">>),

    ct:print("###############################################"),

    print_dir_structures(W, SessId, SourceRootUuid, "SourceRoot"),
    print_dir_structures(W, SessId, TargetRootUuid, "TargetRoot"),
    print_dir_structures(W, SessId, MovedDirUuid, "MovedDir"),
    print_dir_structures(W, SessId, RenamedDirUuid, "RenamedDir"),
    print_reg_structures(W, StaticFile1Uuid, "StaticFile1"),
    print_reg_structures(W, StaticFile2Uuid, "StaticFile2"),
    print_reg_structures(W, MovedFileUuid, "MovedFile"),
    print_reg_structures(W, RenamedFileUuid, "RenamedFile"),

%%    ct:print("Dir: ~p", [rpc:call(W, fslogic_rename, get_all_regular_files, [RootUuid])]),
%%    ct:print("File: ~p", [rpc:call(W, fslogic_rename, get_all_regular_files, [MovedFileUuid])]),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

print_reg_structures(W, Uuid, Name) ->
    {ok, Meta} = rpc:call(W, file_meta, get, [Uuid]),
    ct:print("~p Meta ~p", [Name, Meta]),

    {ok, [LocId | _]} = rpc:call(W, file_meta, get_locations, [Uuid]),
    ct:print("~p LocId ~p", [Name, LocId]),
    {ok, Loc} = rpc:call(W, file_location, get, [LocId]),
    ct:print("~p Loc ~p", [Name, Loc]),
    {ok, LocLinks} = rpc:call(W, datastore, foreach_link, [globally_cached, Loc, fun(LinkName, LinkTarget, List) -> [{LinkName, LinkTarget} | List] end, []]),
    ct:print("~p LocLinks ~p", [Name, LocLinks]),

    {ok, MetaLinks} = rpc:call(W, datastore, foreach_link, [globally_cached, Meta, fun(LinkName, LinkTarget, List) -> [{LinkName, LinkTarget} | List] end, []]),
    ct:print("~p MetaLinks ~p", [Name, MetaLinks]),
    ok.

print_dir_structures(W, SessId, Uuid, Name) ->
    {ok, Meta} = rpc:call(W, file_meta, get, [Uuid]),
    ct:print("~p Meta ~p", [Name, Meta]),

    {ok, Ls} = lfm_proxy:ls(W, SessId, {uuid, Uuid}, 0, 10),
    ct:print("~p Ls ~p", [Name, Ls]),

    {ok, MetaLinks} = rpc:call(W, datastore, foreach_link, [globally_cached, Meta, fun(LinkName, LinkTarget, List) -> [{LinkName, LinkTarget} | List] end, []]),
    ct:print("~p MetaLinks ~p", [Name, MetaLinks]),
    ok.