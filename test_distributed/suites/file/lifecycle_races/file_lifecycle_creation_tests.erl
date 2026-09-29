%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of what happens when a file is opened while it is still being created.
%%% Creating a file is not atomic - the file doc is written first, its times are
%%% recorded next, and the storage file is created later still - so an open may
%%% land in any of those gaps. Whichever gap it lands in, the file must end up
%%% with exactly one storage file behind it, and an open that came too early must
%%% fail rather than create a second one.
%%%
%%% Each case suspends the creation at the point it is about to test, squeezes an
%%% open in exactly there, and then checks both what the callers got back and
%%% what was left on the storage.
%%%
%%% The bodies are dispatched to from file_lifecycle_races_test_SUITE, which gives every
%%% case a space of its own backed by a freshly created storage - hence the
%%% storage contents can be asserted in absolute terms.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lifecycle_creation_tests).
-author("Michal Wrzeszcz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("file/file_lifecycle_test.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% tests
-export([
    concurrent_opens_create_one_storage_file_test/1,
    open_during_create_test/1,
    open_before_file_doc_is_saved_test/1,
    open_during_create_and_open_test/1,
    open_before_creation_times_are_reported_test/1,
    cancelled_create_leaves_no_storage_file_test/1,

    create_file_existing_on_disk_test/1
]).


%%%====================================================================
%%% Test functions
%%%====================================================================


concurrent_opens_create_one_storage_file_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    % widen the window in which both opens see the storage file as missing
    file_lifecycle_test_utils:mock(Node, sd_utils, generic_create_deferred,
        fun(UserCtx, FileCtx, IgnoreEexist) ->
            Ans = meck:passthrough([UserCtx, FileCtx, IgnoreEexist]),
            case file_ctx:get_space_id_const(FileCtx) of
                SpaceId -> timer:sleep(2000);
                _ -> ok
            end,
            Ans
        end
    ),

    {FileGuid, _FilePath} = file_lifecycle_test_utils:create_file(Node, SessId, SpaceId),

    [OpenResult1, OpenResult2] = file_lifecycle_test_utils:open_concurrently(Node, SessId, FileGuid, 2),
    {ok, Handle1} = ?assertMatch({ok, _}, OpenResult1),
    {ok, Handle2} = ?assertMatch({ok, _}, OpenResult2),
    ?assertEqual(
        file_lifecycle_test_utils:get_handle_storage_file_id(Node, Handle1),
        file_lifecycle_test_utils:get_handle_storage_file_id(Node, Handle2)
    ),

    ?assertEqual(1, file_lifecycle_test_utils:count_space_files_on_storage(Node, SpaceId)).


open_during_create_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),
    Master = self(),

    FilePath = file_lifecycle_test_utils:build_file_path(Node, SessId, SpaceId),

    % suspend the creation right after the file doc has been written
    mock_file_doc_creation(Node, filename:basename(FilePath), Master),

    CreateResult = file_lifecycle_test_utils:create_asynchronously(Node, SessId, FilePath),

    SuspendedProc = file_lifecycle_test_utils:await_suspension(),
    OpenResult = lfm_proxy:open(Node, SessId, {path, FilePath}, read),
    file_lifecycle_test_utils:resume(SuspendedProc),
    ?assertMatch({ok, _}, CreateResult()),

    {ok, Handle1} = ?assertMatch({ok, _}, OpenResult),
    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {path, FilePath}, read)),
    ?assertEqual(
        file_lifecycle_test_utils:get_handle_storage_file_id(Node, Handle1),
        file_lifecycle_test_utils:get_handle_storage_file_id(Node, Handle2)
    ),

    ?assertEqual(1, file_lifecycle_test_utils:count_space_files_on_storage(Node, SpaceId)).


open_before_file_doc_is_saved_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),
    Master = self(),

    FilePath = file_lifecycle_test_utils:build_file_path(Node, SessId, SpaceId),
    FileName = filename:basename(FilePath),

    % suspend the creation before the file doc is written, so that the file
    % cannot be found by the concurrent open
    file_lifecycle_test_utils:mock(Node, file_meta, save, fun
        (#document{value = #file_meta{name = Name}} = FileDoc) when Name =:= FileName ->
            ?SUSPEND_UNTIL_RESUMED(Master),
            meck:passthrough([FileDoc]);
        (FileDoc) ->
            meck:passthrough([FileDoc])
    end),

    CreateResult = file_lifecycle_test_utils:create_asynchronously(Node, SessId, FilePath),

    SuspendedProc = file_lifecycle_test_utils:await_suspension(),
    OpenResult = lfm_proxy:open(Node, SessId, {path, FilePath}, read),
    file_lifecycle_test_utils:resume(SuspendedProc),
    ?assertMatch({ok, _}, CreateResult()),

    ?assertMatch({error, ?ENOENT}, OpenResult),
    ?assertEqual(0, file_lifecycle_test_utils:count_space_files_on_storage(Node, SpaceId)).


open_during_create_and_open_test(Config) ->
    Master = self(),

    open_during_create_and_open_test_base(Config, fun(Node, _SpaceId, FileName) ->
        % suspend the creation right after the file doc has been written
        mock_file_doc_creation(Node, FileName, Master)
    end).


open_before_creation_times_are_reported_test(Config) ->
    Master = self(),

    open_during_create_and_open_test_base(Config, fun(Node, SpaceId, _FileName) ->
        % suspend the creation after the file doc has been written, but before
        % its times are recorded
        file_lifecycle_test_utils:mock(Node, times_api, report_file_created, fun(FileCtx) ->
            case file_ctx:get_space_id_const(FileCtx) of
                SpaceId -> ?SUSPEND_UNTIL_RESUMED(Master);
                _ -> ok
            end,
            meck:passthrough([FileCtx])
        end)
    end).


cancelled_create_leaves_no_storage_file_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    % emulate the file being deleted while it is still being created
    file_lifecycle_test_utils:mock(Node, fslogic_event_emitter, emit_file_attr_changed,
        fun(FileCtx, FileAttr, ExcludedSessions) ->
            case file_ctx:get_space_id_const(FileCtx) of
                SpaceId -> {error, not_found};
                _ -> meck:passthrough([FileCtx, FileAttr, ExcludedSessions])
            end
        end
    ),

    FilePath = file_lifecycle_test_utils:build_file_path(Node, SessId, SpaceId),
    ?assertMatch({error, ecanceled}, lfm_proxy:create_and_open(Node, SessId, FilePath)),

    % TODO VFS-5274 - verify if all the documents (e.g. file_location) are cleared
    ?assertEqual(0, file_lifecycle_test_utils:count_space_files_on_storage(Node, SpaceId)).


%%%====================================================================
%%% Test functions awaiting the product to catch up
%%%====================================================================


%% TODO VFS-5271 - creating a file whose storage file already exists must be
%% rejected (see sd_utils:handle_conflicting_directory)
create_file_existing_on_disk_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    FileName = generator:gen_name(),
    StoragePath = storage_test_utils:file_path(Node, SpaceId, FileName),
    {ok, FD} = ?assertMatch({ok, _}, rpc:call(Node, file, open, [StoragePath, [write]])),
    ok = rpc:call(Node, file, close, [FD]),

    FilePath = filename:join([
        file_lifecycle_test_utils:build_space_path(Node, SessId, SpaceId), FileName
    ]),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:create_and_open(Node, SessId, FilePath)).


%%%===================================================================
%%% Test base functions
%%%===================================================================


%% @private
%% @doc
%% Opens a file while it is being created and opened by another process, and
%% checks that all three handles ended up pointing to the same storage file.
%% MockFun suspends the creation at the point the case is about to test.
%% @end
-spec open_during_create_and_open_test_base(
    test_config:config(),
    fun((node(), od_space:id(), file_meta:name()) -> ok)
) ->
    ok | no_return().
open_during_create_and_open_test_base(Config, InstallMockFun) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    FilePath = file_lifecycle_test_utils:build_file_path(Node, SessId, SpaceId),
    ok = InstallMockFun(Node, SpaceId, filename:basename(FilePath)),

    CreateResult = file_lifecycle_test_utils:create_and_open_asynchronously(Node, SessId, FilePath),

    SuspendedProc = file_lifecycle_test_utils:await_suspension(),
    {ok, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {path, FilePath}, rdwr)),
    file_lifecycle_test_utils:resume(SuspendedProc),
    {ok, {_, CreationHandle}} = ?assertMatch({ok, _}, CreateResult()),

    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {path, FilePath}, rdwr)),
    ?assertEqual(
        file_lifecycle_test_utils:get_handle_storage_file_id(Node, Handle1),
        file_lifecycle_test_utils:get_handle_storage_file_id(Node, Handle2)
    ),
    ?assertEqual(
        file_lifecycle_test_utils:get_handle_storage_file_id(Node, Handle1),
        file_lifecycle_test_utils:get_handle_storage_file_id(Node, CreationHandle)
    ),

    ?assertEqual(1, file_lifecycle_test_utils:count_space_files_on_storage(Node, SpaceId)),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
%% @doc
%% Mocks the creation of the file doc so that it is suspended right after the doc
%% has been written - the file is already visible to other operations, but its
%% creation has not finished yet.
%% @end
-spec mock_file_doc_creation(node(), file_meta:name(), pid()) -> ok.
mock_file_doc_creation(Node, FileName, Master) ->
    file_lifecycle_test_utils:mock(Node, file_req, create_file_doc,
        fun(UserCtx, ParentFileCtx, Name, Mode) ->
            Ans = meck:passthrough([UserCtx, ParentFileCtx, Name, Mode]),
            case Name of
                FileName -> ?SUSPEND_UNTIL_RESUMED(Master);
                _ -> ok
            end,
            Ans
        end
    ).
