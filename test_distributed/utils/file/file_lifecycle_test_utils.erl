%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helpers shared by the file_lifecycle_*_tests modules wired into
%%% file_lifecycle_races_test_SUITE. They come in three kinds:
%%%
%%% - resolving the node and the session the whole family works against, and
%%%   creating the storage each test case gets for itself;
%%% - running an operation asynchronously and suspending one inside the provider,
%%%   which is how the cases squeeze two operations into each other;
%%% - reading what the space has on its storage, which is what most cases assert
%%%   on in the end.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lifecycle_test_utils).
-author("Michal Wrzeszcz").

-include("env/space_setup_utils.hrl").
-include("file/file_lifecycle_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([get_node/0, get_session_id/0, create_posix_storage/0, create_s3_storage/0]).
-export([mock/4]).
-export([
    run_asynchronously/1,
    create_asynchronously/3, create_and_open_asynchronously/3,
    open_asynchronously/4, open_concurrently/4
]).
-export([await_suspension/0, resume/1]).
-export([
    build_space_path/3, build_file_path/3,
    create_file/3, create_and_open_file/3,
    create_file_with_storage_file/3, create_file_with_content/4
]).
-export([get_handle_storage_file_id/2, locate_on_storage/3]).
-export([
    list_space_files_on_storage/2, count_space_files_on_storage/2,
    list_deleted_open_files_on_storage/2, has_deleted_open_files_dir/2
]).


%%%===================================================================
%%% Environment
%%%===================================================================


-spec get_node() -> node().
get_node() ->
    oct_background:get_random_provider_node(?PROVIDER_SELECTOR).


-spec get_session_id() -> session:id().
get_session_id() ->
    oct_background:get_user_session_id(?USER_SELECTOR, ?PROVIDER_SELECTOR).


%%--------------------------------------------------------------------
%% @doc
%% Every test case gets a storage of its own, so that it always starts with an
%% empty one and whatever it happens to leave behind cannot affect any other case.
%% @end
%%--------------------------------------------------------------------
-spec create_posix_storage() -> storage:id().
create_posix_storage() ->
    space_setup_utils:create_storage(?PROVIDER_SELECTOR, #posix_storage_params{
        mount_point = <<"/mnt/st_", (generator:gen_name())/binary>>
    }).


%%--------------------------------------------------------------------
%% @doc
%% Counterpart of the above for the cases concerning an object storage, which
%% the Oneprovider treats differently in that it cannot rename a storage file
%% (see helper_spec:is_rename_supported/1). A bucket of its own per storage
%% keeps the guarantee the posix storages give - that a case starts with an
%% empty one.
%% @end
%%--------------------------------------------------------------------
-spec create_s3_storage() -> storage:id().
create_s3_storage() ->
    space_setup_utils:create_storage(?PROVIDER_SELECTOR, #s3_storage_params{
        storage_path_type = <<"flat">>,
        hostname = space_setup_utils:build_s3_hostname(?PROVIDER_SELECTOR),
        bucket_name = ?RAND_STR(15)
    }).


-spec mock(node(), module(), atom(), function()) -> ok.
mock(Node, Module, Function, MockFun) ->
    ok = test_utils:mock_new(Node, Module, [passthrough]),
    ok = test_utils:mock_expect(Node, Module, Function, MockFun).


%%%===================================================================
%%% Running operations asynchronously
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts an operation in a separate process and returns a function that awaits
%% and returns its result.
%% @end
%%--------------------------------------------------------------------
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


-spec create_asynchronously(node(), session:id(), file_meta:path()) -> fun(() -> term()).
create_asynchronously(Node, SessId, FilePath) ->
    run_asynchronously(fun() -> lfm_proxy:create(Node, SessId, FilePath) end).


-spec create_and_open_asynchronously(node(), session:id(), file_meta:path()) ->
    fun(() -> term()).
create_and_open_asynchronously(Node, SessId, FilePath) ->
    run_asynchronously(fun() -> lfm_proxy:create_and_open(Node, SessId, FilePath) end).


-spec open_asynchronously(node(), session:id(), file_id:file_guid(), pos_integer()) ->
    [fun(() -> term())].
open_asynchronously(Node, SessId, FileGuid, Count) ->
    lists:map(fun(_) ->
        run_asynchronously(fun() -> lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), read) end)
    end, lists:seq(1, Count)).


-spec open_concurrently(node(), session:id(), file_id:file_guid(), pos_integer()) -> [term()].
open_concurrently(Node, SessId, FileGuid, Count) ->
    lists:map(fun(AwaitResult) ->
        AwaitResult()
    end, open_asynchronously(Node, SessId, FileGuid, Count)).


%%%===================================================================
%%% Suspending an operation inside the provider
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Awaits the operation suspended by a mock using ?SUSPEND_UNTIL_RESUMED and
%% returns the process to hand back to resume/1 once the test has squeezed in
%% whatever it means to squeeze in.
%% @end
%%--------------------------------------------------------------------
-spec await_suspension() -> pid().
await_suspension() ->
    receive
        {suspended, Pid} -> Pid
    after ?TIMEOUT ->
        ct:fail("Timeout awaiting the operation under test to suspend")
    end.


-spec resume(pid()) -> ok.
resume(SuspendedProc) ->
    SuspendedProc ! resume,
    ok.


%%%===================================================================
%%% Files in the space
%%%===================================================================


-spec build_space_path(node(), session:id(), od_space:id()) -> file_meta:path().
build_space_path(Node, SessId, SpaceId) ->
    {ok, SpacePath} = ?assertMatch({ok, _}, lfm_proxy:get_file_path(
        Node, SessId, space_dir:guid(SpaceId)
    )),
    SpacePath.


-spec build_file_path(node(), session:id(), od_space:id()) -> file_meta:path().
build_file_path(Node, SessId, SpaceId) ->
    filename:join([build_space_path(Node, SessId, SpaceId), generator:gen_name()]).


-spec create_file(node(), session:id(), od_space:id()) ->
    {file_id:file_guid(), file_meta:path()}.
create_file(Node, SessId, SpaceId) ->
    FilePath = build_file_path(Node, SessId, SpaceId),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath)),
    {FileGuid, FilePath}.


-spec create_and_open_file(node(), session:id(), od_space:id()) ->
    {file_id:file_guid(), file_meta:path()}.
create_and_open_file(Node, SessId, SpaceId) ->
    FilePath = build_file_path(Node, SessId, SpaceId),
    {ok, {FileGuid, _}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Node, SessId, FilePath)),
    {FileGuid, FilePath}.


%%--------------------------------------------------------------------
%% @doc
%% Creates a file that already has a storage file behind it. Creating a file
%% does not create one - the provider defers that until the file is first opened
%% - so the file is opened and released right away.
%% @end
%%--------------------------------------------------------------------
-spec create_file_with_storage_file(node(), session:id(), od_space:id()) ->
    {file_id:file_guid(), file_meta:path()}.
create_file_with_storage_file(Node, SessId, SpaceId) ->
    FilePath = build_file_path(Node, SessId, SpaceId),
    {ok, {FileGuid, Handle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, FilePath
    )),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),
    {FileGuid, FilePath}.


%%--------------------------------------------------------------------
%% @doc
%% Counterpart of the above for the cases that must be able to tell a storage
%% file that went away from one that was never there. Opening a file is enough
%% to create it on a POSIX storage, but an object storage has no empty objects -
%% one appears only once something is written (which is why file_lfm_s3_test_SUITE
%% skips the case asserting that opening a file creates it on the storage).
%% @end
%%--------------------------------------------------------------------
-spec create_file_with_content(node(), session:id(), od_space:id(), binary()) ->
    {file_id:file_guid(), file_meta:path()}.
create_file_with_content(Node, SessId, SpaceId, Content) ->
    {FileGuid, FilePath} = create_file(Node, SessId, SpaceId),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle, 0, Content)),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),
    {FileGuid, FilePath}.


%%%===================================================================
%%% Files on the storage
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Id of the storage file behind an lfm handle, as seen by the provider - for
%% checking that two handles ended up pointing at the same storage file.
%% @end
%%--------------------------------------------------------------------
-spec get_handle_storage_file_id(node(), term()) -> helpers:file_id().
get_handle_storage_file_id(Node, TestHandle) ->
    lfm_context:get_file_id(rpc:call(Node, ets, lookup_element, [lfm_handles, TestHandle, 2])).


%%--------------------------------------------------------------------
%% @doc
%% Where the given file sits on the storage, in the form taken by
%% storage_file_tree_test_utils:stat/3 - which is how a case checks the presence
%% of a single storage file regardless of the storage type. Locate the file while
%% it still exists; the returned location outlives it, which is what lets a case
%% check what became of the storage file after the file itself was deleted.
%% @end
%%--------------------------------------------------------------------
-spec locate_on_storage(node(), od_space:id(), file_id:file_guid()) ->
    {storage:id(), helpers:file_id()}.
locate_on_storage(Node, SpaceId, FileGuid) ->
    {ok, StorageId} = storage_test_utils:get_supporting_storage_id(Node, SpaceId),
    {StorageId, storage_test_utils:get_storage_file_id(Node, FileGuid)}.


%%--------------------------------------------------------------------
%% @doc
%% Names of the files the space has on its storage.
%%
%% NOTE: this and the three functions below work on POSIX storages only. They
%% reach the storage through its mount point on the provider node, which an
%% object storage does not have; more fundamentally, an object storage keeps a
%% file under an id derived from its uuid rather than from its name or path (see
%% storage_file_id:raw_flat/2), so there is nothing there to list by name. Cases
%% running on an object storage assert on a single file at a time instead, via
%% locate_on_storage/3 above and storage_file_tree_test_utils:stat/3.
%% @end
%%--------------------------------------------------------------------
-spec list_space_files_on_storage(node(), od_space:id()) -> [binary()].
list_space_files_on_storage(Node, SpaceId) ->
    list_storage_dir(Node, storage_test_utils:space_path(Node, SpaceId)).


-spec count_space_files_on_storage(node(), od_space:id()) -> non_neg_integer().
count_space_files_on_storage(Node, SpaceId) ->
    length(list_space_files_on_storage(Node, SpaceId)).


%%--------------------------------------------------------------------
%% @doc
%% Names of the files in the hidden storage directory, to which the files deleted
%% while being open are moved until their handles are released. The directory sits
%% in the root of the storage, shared by all the spaces it supports.
%% @end
%%--------------------------------------------------------------------
-spec list_deleted_open_files_on_storage(node(), od_space:id()) -> [binary()].
list_deleted_open_files_on_storage(Node, SpaceId) ->
    list_storage_dir(Node, deleted_open_files_path(Node, SpaceId)).


-spec has_deleted_open_files_dir(node(), od_space:id()) -> boolean().
has_deleted_open_files_dir(Node, SpaceId) ->
    storage_test_utils:list_dir(Node, deleted_open_files_path(Node, SpaceId)) =/= {error, ?ENOENT}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
