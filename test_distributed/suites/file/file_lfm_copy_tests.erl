%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of copying files and directories through lfm. A copy target can be
%%% given either as a parent and a name or as a whole path, and both forms are
%%% covered here. Copying a directory copies its entire subtree.
%%%
%%% The bodies are shared by file_lfm_posix_test_SUITE and file_lfm_s3_test_SUITE.
%%% Each test works within its own, randomly named directory in the space, which
%%% the suites empty between test cases.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lfm_copy_tests).
-author("Bartosz Walkowicz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("file/file_lfm_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% tests
-export([
    cp_file_test/0,
    cp_empty_dir_test/0,
    cp_dir_with_children_test/0,

    cp_dir_into_itself_fails_test/0,
    mv_dir_into_symlink_to_itself_fails_test/0
]).

%% suite setup helpers
-export([ensure_default_ls_batch_limit/0]).

% The value the provider is deployed with (see default_ls_batch_limit in
% rel/files/app.config). cp_dir_with_children_test lowers it and restores it
% itself - see ensure_default_ls_batch_limit/0.
-define(DEPLOYED_LS_BATCH_LIMIT, 5000).

-define(TEST_DATA, <<"test data">>).


%%%===================================================================
%%% Tests of copying
%%%===================================================================


cp_file_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    SourceName = generator:gen_name(),
    SourcePath = filename:join([RootDirPath, SourceName]),
    {ok, {SourceGuid, Handle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, SourcePath, ?DEFAULT_FILE_PERMS)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),

    % target given as a parent and a name
    TargetParentPath1 = filename:join([RootDirPath, generator:gen_name()]),
    {ok, TargetParentGuid1} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, TargetParentPath1)),
    TargetName1 = generator:gen_name(),
    {ok, TargetGuid1} = ?assertMatch({ok, _}, lfm_proxy:cp(
        Node, SessId, ?FILE_REF(SourceGuid), {path, TargetParentPath1}, TargetName1)),
    assert_is_only_child(Node, SessId, TargetParentGuid1, TargetGuid1, TargetName1),
    assert_file_content(Node, SessId, filename:join([TargetParentPath1, TargetName1]), TargetGuid1),

    % target given as a whole path, going through a directory named just like the
    % source file - the name must be resolved against the target parent, not the source
    TargetParentRelPath2 = filename:join([generator:gen_name(), SourceName]),
    {ok, #file_attr{guid = TargetParentGuid2}} = ?assertMatch({ok, _}, lfm_proxy:create_dir_at_path(
        Node, SessId, RootDirGuid, TargetParentRelPath2)),
    TargetName2 = generator:gen_name(),
    TargetPath2 = filename:join([RootDirPath, TargetParentRelPath2, TargetName2]),
    {ok, TargetGuid2} = ?assertMatch({ok, _}, lfm_proxy:cp(Node, SessId, ?FILE_REF(SourceGuid), TargetPath2)),
    assert_is_only_child(Node, SessId, TargetParentGuid2, TargetGuid2, TargetName2),
    assert_file_content(Node, SessId, TargetPath2, TargetGuid2),

    ok.


cp_empty_dir_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, SourceGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),

    % target given as a parent and a name
    TargetParentPath1 = filename:join([RootDirPath, generator:gen_name()]),
    {ok, TargetParentGuid1} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, TargetParentPath1)),
    TargetName1 = generator:gen_name(),
    {ok, TargetGuid1} = ?assertMatch({ok, _}, lfm_proxy:cp(
        Node, SessId, ?FILE_REF(SourceGuid), {path, TargetParentPath1}, TargetName1)),
    assert_is_only_child(Node, SessId, TargetParentGuid1, TargetGuid1, TargetName1),

    % target given as a whole path
    TargetParentPath2 = filename:join([RootDirPath, generator:gen_name()]),
    {ok, TargetParentGuid2} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, TargetParentPath2)),
    TargetName2 = generator:gen_name(),
    {ok, TargetGuid2} = ?assertMatch({ok, _}, lfm_proxy:cp(
        Node, SessId, ?FILE_REF(SourceGuid), filename:join([TargetParentPath2, TargetName2]))),
    assert_is_only_child(Node, SessId, TargetParentGuid2, TargetGuid2, TargetName2),

    ok.


cp_dir_with_children_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, SourceGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),
    [ChildName1, ChildName2] = ChildNames = lists:sort(
        lists_utils:generate(fun generator:gen_name/0, 2)),
    lists:foreach(fun(ChildName) ->
        {ok, {_, Handle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
            Node, SessId, SourceGuid, ChildName, ?DEFAULT_FILE_PERMS)),
        ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle, 0, ?TEST_DATA)),
        ?assertEqual(ok, lfm_proxy:close(Node, Handle))
    end, ChildNames),

    % the whole subtree must be copied even with no write permission on the source
    SourceMode = 8#555,
    ?assertEqual(ok, lfm_proxy:set_perms(Node, SessId, ?FILE_REF(SourceGuid), SourceMode)),

    TargetParentPath = filename:join([RootDirPath, generator:gen_name()]),
    {ok, TargetParentGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, TargetParentPath)),
    TargetName = generator:gen_name(),
    TargetPath = filename:join([TargetParentPath, TargetName]),

    % the children must be copied even when they do not fit in a single listing batch
    PreviousLsBatchLimit = get_ls_batch_limit(Node),
    set_ls_batch_limit(Node, 1),
    TargetGuid = try
        {ok, Guid} = ?assertMatch({ok, _}, lfm_proxy:cp(
            Node, SessId, ?FILE_REF(SourceGuid), {path, TargetParentPath}, TargetName)),
        Guid
    after
        set_ls_batch_limit(Node, PreviousLsBatchLimit)
    end,

    assert_is_only_child(Node, SessId, TargetParentGuid, TargetGuid, TargetName),
    ?assertMatch({ok, #file_attr{guid = TargetGuid, mode = SourceMode}},
        lfm_proxy:stat(Node, SessId, {path, TargetPath})),

    ?assertMatch({ok, [{_, ChildName1}, {_, ChildName2}], _}, lfm_proxy:get_children(
        Node, SessId, ?FILE_REF(TargetGuid),
        #{offset => 0, limit => 10, tune_for_large_continuous_listing => false})),
    lists:foreach(fun(ChildName) ->
        ChildPath = filename:join([TargetPath, ChildName]),
        {ok, #file_attr{guid = ChildGuid}} = ?assertMatch({ok, #file_attr{name = ChildName}},
            lfm_proxy:stat(Node, SessId, {path, ChildPath})),
        assert_file_content(Node, SessId, ChildPath, ChildGuid)
    end, ChildNames),

    ok.


%%%===================================================================
%%% Tests of self referential copy and move targets
%%%===================================================================


cp_dir_into_itself_fails_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    SourceName = generator:gen_name(),
    SourcePath = filename:join([RootDirPath, SourceName]),
    {ok, SourceGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, SourcePath)),

    % NOTE: lfm_proxy:mkdir creates only the leaf, so the whole branch is made at once
    {ok, #file_attr{guid = GrandChildGuid}} = ?assertMatch({ok, _}, lfm_proxy:create_dir_at_path(
        Node, SessId, SourceGuid, filename:join(lists_utils:generate(fun generator:gen_name/0, 2)))),

    SymlinkGuid = create_symlink_to(Node, SessId, RootDirPath, SourceGuid),

    ?assertMatch({error, ?EINVAL}, lfm_proxy:cp(
        Node, SessId, ?FILE_REF(SourceGuid), {path, SourcePath}, SourceName)),
    ?assertMatch({error, ?EINVAL}, lfm_proxy:cp(
        Node, SessId, ?FILE_REF(SourceGuid), ?FILE_REF(GrandChildGuid), SourceName)),
    ?assertMatch({error, ?EINVAL}, lfm_proxy:cp(
        Node, SessId, ?FILE_REF(SourceGuid), ?FILE_REF(SymlinkGuid), generator:gen_name())),

    ok.


mv_dir_into_symlink_to_itself_fails_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, SourceGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),
    SymlinkGuid = create_symlink_to(Node, SessId, RootDirPath, SourceGuid),

    ?assertMatch({error, ?EINVAL}, lfm_proxy:mv(
        Node, SessId, ?FILE_REF(SourceGuid), ?FILE_REF(SymlinkGuid), generator:gen_name())),

    ok.


%%%===================================================================
%%% Suite setup helpers
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% cp_dir_with_children_test lowers the children batch limit for the duration of
%% a single copy and restores it itself. A run interrupted in between would leave
%% it at 1 on a reused deployment, making every later listing crawl - hence this
%% defensive reset, to be called from the init_per_suite posthook.
%% @end
%%--------------------------------------------------------------------
-spec ensure_default_ls_batch_limit() -> ok.
ensure_default_ls_batch_limit() ->
    lists:foreach(fun(Node) ->
        case get_ls_batch_limit(Node) of
            1 -> set_ls_batch_limit(Node, ?DEPLOYED_LS_BATCH_LIMIT);
            _ -> ok
        end
    end, oct_background:get_provider_nodes(?PROVIDER_SELECTOR)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_symlink_to(node(), session:id(), file_meta:path(), file_id:file_guid()) ->
    file_id:file_guid().
create_symlink_to(Node, SessId, ParentPath, TargetGuid) ->
    {ok, #file_attr{guid = SymlinkGuid}} = ?assertMatch({ok, _}, lfm_proxy:make_symlink(
        Node, SessId, {path, ParentPath}, generator:gen_name(),
        file_tree_test_utils:prepare_symlink_value(Node, SessId, TargetGuid)
    )),
    SymlinkGuid.


%% @private
-spec assert_is_only_child(node(), session:id(), file_id:file_guid(),
    file_id:file_guid(), file_meta:name()) -> ok.
assert_is_only_child(Node, SessId, ParentGuid, ChildGuid, ChildName) ->
    ?assertMatch({ok, [{ChildGuid, ChildName}], _}, lfm_proxy:get_children(
        Node, SessId, ?FILE_REF(ParentGuid),
        #{offset => 0, limit => 10, tune_for_large_continuous_listing => false})),
    ok.


%% @private
-spec assert_file_content(node(), session:id(), file_meta:path(), file_id:file_guid()) -> ok.
assert_file_content(Node, SessId, FilePath, FileGuid) ->
    ?assertMatch({ok, #file_attr{guid = FileGuid}}, lfm_proxy:stat(Node, SessId, {path, FilePath})),

    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {path, FilePath}, read)),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(Node, Handle, 0, byte_size(?TEST_DATA))),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),
    ok.


%% @private
-spec get_ls_batch_limit(node()) -> pos_integer().
get_ls_batch_limit(Node) ->
    {ok, Limit} = test_utils:get_env(Node, op_worker, default_ls_batch_limit),
    Limit.


%% @private
-spec set_ls_batch_limit(node(), pos_integer()) -> ok.
set_ls_batch_limit(Node, Limit) ->
    test_utils:set_env(Node, op_worker, default_ls_batch_limit, Limit).
