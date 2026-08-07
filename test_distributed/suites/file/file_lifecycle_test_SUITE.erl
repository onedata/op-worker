%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of races between the operations that make up the lifecycle of a file -
%%% creating it, opening it and deleting it. Each case suspends the operation
%%% under test inside the provider, squeezes another one in exactly at that
%%% point, and then checks both what the caller got back and what was left on
%%% the storage.
%%%
%%% Every case runs in a space of its own, backed by a freshly created storage,
%%% which lets the storage contents be asserted in absolute terms.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lifecycle_test_SUITE).
-author("Michal Wrzeszcz").

-include("env/space_setup_utils.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_delete.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    concurrent_opens_create_one_storage_file_test/1,
    open_during_create_test/1,
    open_before_file_doc_is_saved_test/1,
    open_during_create_and_open_test/1,
    open_before_creation_times_are_reported_test/1,
    cancelled_create_leaves_no_storage_file_test/1,
    delete_during_open_with_deletion_marker_test/1,
    delete_during_open_with_storage_rename_test/1,

    delete_of_opened_file_moves_it_on_storage_test/1,
    name_of_deleted_opened_file_can_be_reused_test/1,
    release_before_deleted_file_is_moved_on_storage_test/1,
    release_after_deleted_file_is_moved_on_storage_test/1,

    rename_to_opened_file_test/1,
    create_file_existing_on_disk_test/1
]).

all() -> [
    concurrent_opens_create_one_storage_file_test,
    open_during_create_test,
    open_before_file_doc_is_saved_test,
    open_during_create_and_open_test,
    open_before_creation_times_are_reported_test,
    cancelled_create_leaves_no_storage_file_test,
    delete_during_open_with_deletion_marker_test,
    delete_during_open_with_storage_rename_test,

    delete_of_opened_file_moves_it_on_storage_test,
    name_of_deleted_opened_file_can_be_reused_test,
    release_before_deleted_file_is_moved_on_storage_test,
    release_after_deleted_file_is_moved_on_storage_test
    %%    rename_to_opened_file_test, % TODO VFS-5290
    %%    create_file_existing_on_disk_test % TODO VFS-5271
].

-define(SPACE_ID(Config), ?config(space_id, Config)).

% modules mocked by the test cases; unloaded after every one of them regardless
% of which ones it actually used
-define(MOCKED_MODULES, [
    file_meta, file_req, fslogic_delete, fslogic_event_emitter, sd_utils,
    storage_driver, times_api
]).

-define(TIMEOUT, timer:seconds(30)).

% Used inside the mocks to suspend the operation being tested until the test
% process lets it through. Every mock must first make sure the call concerns the
% file under test - the deployment is shared, so suspending calls indiscriminately
% would stall whatever else runs on the provider at that moment.
-define(SUSPEND_UNTIL_RESUMED(Master), begin
    Master ! {suspended, self()},
    ok = receive
        resume -> ok
    after ?TIMEOUT ->
        timeout
    end
end).

%%%====================================================================
%%% Test functions
%%%====================================================================

concurrent_opens_create_one_storage_file_test(Config) ->
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    % widen the window in which both opens see the storage file as missing
    mock(Node, sd_utils, generic_create_deferred, fun(UserCtx, FileCtx, IgnoreEexist) ->
        Ans = meck:passthrough([UserCtx, FileCtx, IgnoreEexist]),
        case file_ctx:get_space_id_const(FileCtx) of
            SpaceId -> timer:sleep(2000);
            _ -> ok
        end,
        Ans
    end),

    {FileGuid, _FilePath} = create_file(Node, SessId, SpaceId),

    [OpenResult1, OpenResult2] = open_concurrently(Node, SessId, FileGuid, 2),
    {ok, Handle1} = ?assertMatch({ok, _}, OpenResult1),
    {ok, Handle2} = ?assertMatch({ok, _}, OpenResult2),
    ?assertEqual(get_storage_file_id(Node, Handle1), get_storage_file_id(Node, Handle2)),

    ?assertEqual(1, count_space_files_on_storage(Node, SpaceId)).


open_during_create_test(Config) ->
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),
    Master = self(),

    FilePath = build_file_path(Node, SessId, SpaceId),

    % suspend the creation right after the file doc has been written
    mock_file_doc_creation(Node, filename:basename(FilePath), Master),

    CreateResult = create_asynchronously(Node, SessId, FilePath),

    SuspendedProc = await_suspension(),
    OpenResult = lfm_proxy:open(Node, SessId, {path, FilePath}, read),
    resume(SuspendedProc),
    ?assertMatch({ok, _}, CreateResult()),

    {ok, Handle1} = ?assertMatch({ok, _}, OpenResult),
    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {path, FilePath}, read)),
    ?assertEqual(get_storage_file_id(Node, Handle1), get_storage_file_id(Node, Handle2)),

    ?assertEqual(1, count_space_files_on_storage(Node, SpaceId)).


open_before_file_doc_is_saved_test(Config) ->
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),
    Master = self(),

    FilePath = build_file_path(Node, SessId, SpaceId),
    FileName = filename:basename(FilePath),

    % suspend the creation before the file doc is written, so that the file
    % cannot be found by the concurrent open
    mock(Node, file_meta, save, fun
        (#document{value = #file_meta{name = Name}} = FileDoc) when Name =:= FileName ->
            ?SUSPEND_UNTIL_RESUMED(Master),
            meck:passthrough([FileDoc]);
        (FileDoc) ->
            meck:passthrough([FileDoc])
    end),

    CreateResult = create_asynchronously(Node, SessId, FilePath),

    SuspendedProc = await_suspension(),
    OpenResult = lfm_proxy:open(Node, SessId, {path, FilePath}, read),
    resume(SuspendedProc),
    ?assertMatch({ok, _}, CreateResult()),

    ?assertMatch({error, ?ENOENT}, OpenResult),
    ?assertEqual(0, count_space_files_on_storage(Node, SpaceId)).


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
        mock(Node, times_api, report_file_created, fun(FileCtx) ->
            case file_ctx:get_space_id_const(FileCtx) of
                SpaceId -> ?SUSPEND_UNTIL_RESUMED(Master);
                _ -> ok
            end,
            meck:passthrough([FileCtx])
        end)
    end).


cancelled_create_leaves_no_storage_file_test(Config) ->
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    % emulate the file being deleted while it is still being created
    mock(Node, fslogic_event_emitter, emit_file_attr_changed, fun(FileCtx, FileAttr, ExcludedSessions) ->
        case file_ctx:get_space_id_const(FileCtx) of
            SpaceId -> {error, not_found};
            _ -> meck:passthrough([FileCtx, FileAttr, ExcludedSessions])
        end
    end),

    FilePath = build_file_path(Node, SessId, SpaceId),
    ?assertMatch({error, ecanceled}, lfm_proxy:create_and_open(Node, SessId, FilePath)),

    % TODO VFS-5274 - verify if all the documents (e.g. file_location) are cleared
    ?assertEqual(0, count_space_files_on_storage(Node, SpaceId)).


delete_during_open_with_deletion_marker_test(Config) ->
    Node = get_node(),
    SpaceId = ?SPACE_ID(Config),

    % with the deletion marker method the storage file is left under its
    % original path until the handle is released
    mock(Node, fslogic_delete, get_open_file_handling_method, fun(FileCtx) ->
        case file_ctx:get_space_id_const(FileCtx) of
            SpaceId -> {?SET_DELETION_MARKER, FileCtx};
            _ -> meck:passthrough([FileCtx])
        end
    end),

    delete_during_open_test_base(Config),

    ?assertEqual(1, count_space_files_on_storage(Node, SpaceId)),
    ?assertEqual(0, count_deleted_open_files_on_storage(Node, SpaceId)).


delete_during_open_with_storage_rename_test(Config) ->
    Node = get_node(),
    SpaceId = ?SPACE_ID(Config),

    % a POSIX storage defaults to the rename method, which moves the storage
    % file to a hidden directory the moment the file is deleted
    delete_during_open_test_base(Config),

    ?assertEqual(0, count_space_files_on_storage(Node, SpaceId)),
    ?assertEqual(1, count_deleted_open_files_on_storage(Node, SpaceId)).


%%%====================================================================
%%% Test functions concerning deletion of an opened file
%%%====================================================================


delete_of_opened_file_moves_it_on_storage_test(Config) ->
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    {FileGuid, FilePath} = create_and_open_file(Node, SessId, SpaceId),
    ?assertEqual([filename:basename(FilePath)], list_space_files_on_storage(Node, SpaceId)),

    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(FileGuid))),

    % the file is gone from the file tree, but as long as it is open its storage
    % file lives on, moved to a hidden directory in the root of the storage
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid))),
    ?assertEqual([], list_space_files_on_storage(Node, SpaceId)),
    ?assertEqual([FileGuid], list_deleted_open_files_on_storage(Node, SpaceId)),

    ExpStorageFileId = filename:join([?DELETED_OPENED_FILES_DIR, FileGuid]),
    ?assertMatch({ok, #file_location{file_id = ExpStorageFileId}},
        lfm_proxy:get_file_location(Node, SessId, ?FILE_REF(FileGuid))),

    % releasing the last handle finally removes the storage file
    ?assertEqual(ok, lfm_proxy:close_all(Node)),
    ?assertEqual([], list_deleted_open_files_on_storage(Node, SpaceId)).


name_of_deleted_opened_file_can_be_reused_test(Config) ->
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    {FileGuid, FilePath} = create_and_open_file(Node, SessId, SpaceId),
    FileName = filename:basename(FilePath),

    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(FileGuid))),
    ?assertEqual([], list_space_files_on_storage(Node, SpaceId)),
    ?assertEqual([FileGuid], list_deleted_open_files_on_storage(Node, SpaceId)),

    % the freed name can be taken over by a new file tree entry, whose storage
    % file takes the very path the deleted one used to occupy
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, FilePath)),
    ChildPath = filename:join([FilePath, generator:gen_name()]),
    {ok, {ChildGuid, _}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Node, SessId, ChildPath)),
    ?assertEqual([FileName], list_space_files_on_storage(Node, SpaceId)),

    ?assertEqual(ok, lfm_proxy:close_all(Node)),
    ?assertEqual([], list_deleted_open_files_on_storage(Node, SpaceId)),

    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(ChildGuid))),
    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(DirGuid))),
    ?assertEqual([], list_space_files_on_storage(Node, SpaceId)).


release_before_deleted_file_is_moved_on_storage_test(Config) ->
    release_during_deletion_of_opened_file_test_base(Config, before_move).


release_after_deleted_file_is_moved_on_storage_test(Config) ->
    release_during_deletion_of_opened_file_test_base(Config, after_move).


%%%====================================================================
%%% Test functions awaiting the product to catch up
%%%====================================================================

%% TODO VFS-5290 - moving a file onto an opened one must not remove the latter
%% from the storage while it is still open (see sd_utils:rename_storage_file)
rename_to_opened_file_test(Config) ->
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    % the target is left open, so its storage file must survive being overwritten
    {_, TargetPath} = create_and_open_file(Node, SessId, SpaceId),

    SourcePath = build_file_path(Node, SessId, SpaceId),
    {ok, {_, SourceHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, SourcePath
    )),
    ?assertEqual(ok, lfm_proxy:close(Node, SourceHandle)),

    ?assertMatch({ok, _}, lfm_proxy:mv(Node, SessId, {path, SourcePath}, TargetPath)),

    ?assertEqual(2, count_space_files_on_storage(Node, SpaceId)).


%% TODO VFS-5271 - creating a file whose storage file already exists must be
%% rejected (see sd_utils:handle_conflicting_directory)
create_file_existing_on_disk_test(Config) ->
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    FileName = generator:gen_name(),
    StoragePath = storage_test_utils:file_path(Node, SpaceId, FileName),
    {ok, FD} = ?assertMatch({ok, _}, rpc:call(Node, file, open, [StoragePath, [write]])),
    ok = rpc:call(Node, file, close, [FD]),

    FilePath = filename:join([build_space_path(Node, SessId, SpaceId), FileName]),
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
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    FilePath = build_file_path(Node, SessId, SpaceId),
    ok = InstallMockFun(Node, SpaceId, filename:basename(FilePath)),

    CreateResult = create_and_open_asynchronously(Node, SessId, FilePath),

    SuspendedProc = await_suspension(),
    {ok, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {path, FilePath}, rdwr)),
    resume(SuspendedProc),
    {ok, {_, CreationHandle}} = ?assertMatch({ok, _}, CreateResult()),

    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {path, FilePath}, rdwr)),
    ?assertEqual(get_storage_file_id(Node, Handle1), get_storage_file_id(Node, Handle2)),
    ?assertEqual(get_storage_file_id(Node, Handle1), get_storage_file_id(Node, CreationHandle)),

    ?assertEqual(1, count_space_files_on_storage(Node, SpaceId)),
    ok.


%% @private
%% @doc
%% Deletes a file while it is being opened - the open is suspended halfway
%% through, after the storage file has been opened - and checks that the open
%% still succeeds. What is left on the storage depends on the method of handling
%% deletion of an opened file and is asserted by the calling test case.
%% @end
-spec delete_during_open_test_base(test_config:config()) -> ok | no_return().
delete_during_open_test_base(Config) ->
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),
    Master = self(),

    % the provider opens files on storage only when the session does not use
    % direct IO; the space is discarded after the case, so there is nothing to restore
    ?assertEqual(ok, rpc:call(Node, session, set_direct_io, [SessId, SpaceId, false])),

    mock(Node, file_req, open_on_storage, fun(UserCtx, FileCtx, SessionId, Flag, HandleId) ->
        Ans = meck:passthrough([UserCtx, FileCtx, SessionId, Flag, HandleId]),
        case file_ctx:get_space_id_const(FileCtx) of
            SpaceId -> ?SUSPEND_UNTIL_RESUMED(Master);
            _ -> ok
        end,
        Ans
    end),

    {FileGuid, FilePath} = create_file(Node, SessId, SpaceId),
    [OpenResult] = open_asynchronously(Node, SessId, FileGuid, 1),

    SuspendedProc = await_suspension(),
    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, {path, FilePath})),
    resume(SuspendedProc),

    ?assertMatch({ok, _}, OpenResult()),
    ok.


%% @private
%% @doc
%% Releases the handle to a deleted file while its storage file is being moved
%% to the hidden directory for deleted open files - either just before or just
%% after the move itself. Whichever order the two end up in, nothing may be left
%% behind on the storage.
%% @end
-spec release_during_deletion_of_opened_file_test_base(
    test_config:config(), before_move | after_move
) ->
    ok | no_return().
release_during_deletion_of_opened_file_test_base(Config, When) ->
    Node = get_node(),
    SessId = get_session_id(),
    SpaceId = ?SPACE_ID(Config),
    Master = self(),

    {FileGuid, _FilePath} = create_and_open_file(Node, SessId, SpaceId),
    TargetFileId = filename:join([?DELETED_OPENED_FILES_DIR, FileGuid]),

    mock(Node, storage_driver, mv, case When of
        before_move ->
            fun(Handle, FileId) ->
                case FileId of
                    TargetFileId -> ?SUSPEND_UNTIL_RESUMED(Master);
                    _ -> ok
                end,
                meck:passthrough([Handle, FileId])
            end;
        after_move ->
            fun(Handle, FileId) ->
                Ans = meck:passthrough([Handle, FileId]),
                case FileId of
                    TargetFileId -> ?SUSPEND_UNTIL_RESUMED(Master);
                    _ -> ok
                end,
                Ans
            end
    end),

    UnlinkResult = run_asynchronously(fun() ->
        lfm_proxy:unlink(Node, SessId, ?FILE_REF(FileGuid))
    end),

    SuspendedProc = await_suspension(),
    ?assertEqual(ok, lfm_proxy:close_all(Node)),
    resume(SuspendedProc),

    ?assertEqual(ok, UnlinkResult()),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid))),

    % the hidden directory was created, so the move was indeed attempted, but the
    % file released in the meantime must not have been left in it
    ?assert(has_deleted_open_files_dir(Node, SpaceId)),
    ?assertEqual([], list_space_files_on_storage(Node, SpaceId)),
    ?assertEqual([], list_deleted_open_files_on_storage(Node, SpaceId)),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite([{?LOAD_MODULES, [?MODULE]} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            space_setup_utils:clean_up_after_previous_run(all(), [krakow]),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    ct:timetrap({minutes, 5}),
    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = Case,
        owner = user1,
        supports = [#support_spec{
            provider = krakow,
            storage_spec = create_posix_storage(),
            size = 1073741824
        }]
    }),
    lfm_proxy:init([{space_id, SpaceId} | Config]).


end_per_testcase(_Case, Config) ->
    Node = get_node(),
    % release the handles while the mocks are still in place - the release path
    % goes through some of the mocked modules
    ?assertEqual(ok, lfm_proxy:close_all(Node)),
    ok = test_utils:mock_unload(Node, ?MOCKED_MODULES),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_node() -> node().
get_node() ->
    oct_background:get_random_provider_node(krakow).


%% @private
-spec get_session_id() -> session:id().
get_session_id() ->
    oct_background:get_user_session_id(user1, krakow).


%% @private
%% @doc
%% Every test case gets a storage of its own, so that it always starts with an
%% empty one and whatever it happens to leave behind cannot affect any other case.
%% @end
-spec create_posix_storage() -> storage:id().
create_posix_storage() ->
    space_setup_utils:create_storage(krakow, #posix_storage_params{
        mount_point = <<"/mnt/st_", (generator:gen_name())/binary>>
    }).


%% @private
%% @doc
%% Mocks the creation of the file doc so that it is suspended right after the doc
%% has been written - the file is already visible to other operations, but its
%% creation has not finished yet.
%% @end
-spec mock_file_doc_creation(node(), file_meta:name(), pid()) -> ok.
mock_file_doc_creation(Node, FileName, Master) ->
    mock(Node, file_req, create_file_doc, fun(UserCtx, ParentFileCtx, Name, Mode) ->
        Ans = meck:passthrough([UserCtx, ParentFileCtx, Name, Mode]),
        case Name of
            FileName -> ?SUSPEND_UNTIL_RESUMED(Master);
            _ -> ok
        end,
        Ans
    end).


%% @private
-spec mock(node(), module(), atom(), function()) -> ok.
mock(Node, Module, Function, MockFun) ->
    ok = test_utils:mock_new(Node, Module, [passthrough]),
    ok = test_utils:mock_expect(Node, Module, Function, MockFun).


%% @private
-spec await_suspension() -> pid().
await_suspension() ->
    receive
        {suspended, Pid} -> Pid
    after ?TIMEOUT ->
        ct:fail("Timeout awaiting the operation under test to suspend")
    end.


%% @private
-spec resume(pid()) -> ok.
resume(SuspendedProc) ->
    SuspendedProc ! resume,
    ok.


%% @private
-spec build_space_path(node(), session:id(), od_space:id()) -> file_meta:path().
build_space_path(Node, SessId, SpaceId) ->
    {ok, SpacePath} = ?assertMatch({ok, _}, lfm_proxy:get_file_path(
        Node, SessId, space_dir:guid(SpaceId)
    )),
    SpacePath.


%% @private
-spec build_file_path(node(), session:id(), od_space:id()) -> file_meta:path().
build_file_path(Node, SessId, SpaceId) ->
    filename:join([build_space_path(Node, SessId, SpaceId), generator:gen_name()]).


%% @private
-spec create_file(node(), session:id(), od_space:id()) ->
    {file_id:file_guid(), file_meta:path()}.
create_file(Node, SessId, SpaceId) ->
    FilePath = build_file_path(Node, SessId, SpaceId),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath)),
    {FileGuid, FilePath}.


%% @private
-spec create_and_open_file(node(), session:id(), od_space:id()) ->
    {file_id:file_guid(), file_meta:path()}.
create_and_open_file(Node, SessId, SpaceId) ->
    FilePath = build_file_path(Node, SessId, SpaceId),
    {ok, {FileGuid, _}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Node, SessId, FilePath)),
    {FileGuid, FilePath}.


%% @private
%% @doc
%% Starts an operation in a separate process and returns a function that awaits
%% and returns its result.
%% @end
-spec run_asynchronously(function()) -> fun(() -> term()).
run_asynchronously(Fun) ->
    Master = self(),
    Ref = make_ref(),
    spawn(fun() -> Master ! {Ref, Fun()} end),
    fun() ->
        receive
            {Ref, Result} -> Result
        after ?TIMEOUT ->
            ct:fail("Timeout awaiting the asynchronous operation to finish")
        end
    end.


%% @private
-spec create_asynchronously(node(), session:id(), file_meta:path()) -> fun(() -> term()).
create_asynchronously(Node, SessId, FilePath) ->
    run_asynchronously(fun() -> lfm_proxy:create(Node, SessId, FilePath) end).


%% @private
-spec create_and_open_asynchronously(node(), session:id(), file_meta:path()) ->
    fun(() -> term()).
create_and_open_asynchronously(Node, SessId, FilePath) ->
    run_asynchronously(fun() -> lfm_proxy:create_and_open(Node, SessId, FilePath) end).


%% @private
-spec open_asynchronously(node(), session:id(), file_id:file_guid(), pos_integer()) ->
    [fun(() -> term())].
open_asynchronously(Node, SessId, FileGuid, Count) ->
    lists:map(fun(_) ->
        run_asynchronously(fun() -> lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), read) end)
    end, lists:seq(1, Count)).


%% @private
-spec open_concurrently(node(), session:id(), file_id:file_guid(), pos_integer()) -> [term()].
open_concurrently(Node, SessId, FileGuid, Count) ->
    lists:map(fun(AwaitResult) ->
        AwaitResult()
    end, open_asynchronously(Node, SessId, FileGuid, Count)).


%% @private
%% @doc
%% Id of the storage file behind an lfm handle, as seen by the provider.
%% @end
-spec get_storage_file_id(node(), term()) -> helpers:file_id().
get_storage_file_id(Node, TestHandle) ->
    lfm_context:get_file_id(rpc:call(Node, ets, lookup_element, [lfm_handles, TestHandle, 2])).


%% @private
-spec count_space_files_on_storage(node(), od_space:id()) -> non_neg_integer().
count_space_files_on_storage(Node, SpaceId) ->
    length(list_space_files_on_storage(Node, SpaceId)).


%% @private
-spec count_deleted_open_files_on_storage(node(), od_space:id()) -> non_neg_integer().
count_deleted_open_files_on_storage(Node, SpaceId) ->
    length(list_deleted_open_files_on_storage(Node, SpaceId)).


%% @private
%% @doc Names of the files the space has on its storage.
-spec list_space_files_on_storage(node(), od_space:id()) -> [binary()].
list_space_files_on_storage(Node, SpaceId) ->
    list_storage_dir(Node, storage_test_utils:space_path(Node, SpaceId)).


%% @private
%% @doc
%% Names of the files in the hidden storage directory, to which the files deleted
%% while being open are moved until their handles are released. The directory sits
%% in the root of the storage, shared by all the spaces it supports.
%% @end
-spec list_deleted_open_files_on_storage(node(), od_space:id()) -> [binary()].
list_deleted_open_files_on_storage(Node, SpaceId) ->
    list_storage_dir(Node, deleted_open_files_path(Node, SpaceId)).


%% @private
-spec has_deleted_open_files_dir(node(), od_space:id()) -> boolean().
has_deleted_open_files_dir(Node, SpaceId) ->
    storage_test_utils:list_dir(Node, deleted_open_files_path(Node, SpaceId)) =/= {error, ?ENOENT}.


%% @private
-spec deleted_open_files_path(node(), od_space:id()) -> binary().
deleted_open_files_path(Node, SpaceId) ->
    {ok, StorageId} = storage_test_utils:get_supporting_storage_id(Node, SpaceId),
    MountPoint = storage_test_utils:storage_mount_point(Node, StorageId),
    filename:join([MountPoint, ?DELETED_OPENED_FILES_DIR]).


%% @private
-spec list_storage_dir(node(), binary()) -> [binary()].
list_storage_dir(Node, DirPath) ->
    case storage_test_utils:list_dir(Node, DirPath) of
        {ok, Entries} -> lists:sort([list_to_binary(Entry) || Entry <- Entries]);
        {error, ?ENOENT} -> []
    end.
