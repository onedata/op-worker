%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions inspecting a provider's storage through the file system of
%%% the node the storage is mounted on (rpc calls to the 'file' module) - hence
%%% they only work for POSIX compatible storages, but in exchange they report
%%% what the operating system sees (#file_info{}), independently of the storage
%%% helper. Backs the assertion macros of storage_test.hrl.
%%%
%%% The two ensure_*_created_on_storage/2 functions are the odd ones out - they
%%% go through lfm, not the storage - but they are named after their purpose
%%% (materializing a file on the storage, as creating it logically does not
%%% suffice) and are used together with the assertions below, hence they live here.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_test_utils).
-author("Jakub Kudzia").

-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("kernel/include/file.hrl").

%% API
-export([
    assert_file_info/5,

    read_file/2, read_file_info/2, list_dir/2,
    space_path/2, file_path/3,
    get_supporting_storage_id/2, get_storage_file_id/2, storage_mount_point/2,

    ensure_file_created_on_storage/2,
    ensure_dir_created_on_storage/2,

    assert_file_owner_on_posix_storage/4,
    assert_file_attrs_on_posix_storage/5
]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec assert_file_info(#{atom() => term()}, node(), binary(), pos_integer(), non_neg_integer()) ->
    ok | no_return().
assert_file_info(ExpectedValues, Worker, FilePath, Line, Attempts) when Attempts >= 0 ->
    try
        {ok, FI} = read_file_info(Worker, FilePath),
        maps:foreach(fun(Field, ExpectedValue) ->
            assert_field(Field, ExpectedValue, FI)
        end, ExpectedValues)
    catch
        throw:(Error = {assertion_error, Field, ExpectedValue, Value}) when Attempts =:= 0 ->
            ct:pal(
                "Assertion for file ~tp failed.~n"
                "   Field: ~tp~n"
                "   Expected: ~tp~n"
                "   Got: ~tp~n"
                "   Module: ~tp~n"
                "   Line: ~tp",
                [FilePath, Field, ExpectedValue, Value, ?MODULE, Line]
            ),
            ct:fail(Error);
        Error:Reason when Attempts =:= 0 ->
            ct:pal(
                "Assertion for file ~tp failed.~n"
                "   Error: {~tp, ~tp}~n"
                "   Module: ~tp~n"
                "   Line: ~tp",
                [FilePath, Error, Reason, ?MODULE, Line]
            ),
            ct:fail({Error, Reason});
        _:_ ->
            timer:sleep(timer:seconds(1)),
            assert_file_info(ExpectedValues, Worker, FilePath, Line, Attempts - 1)
    end.

%% @private
-spec assert_field(atom(), term(), #file_info{}) -> ok | no_return().
assert_field(Field, ExpectedValue, Record) ->
    case get_record_field(Record, Field) of
        ExpectedValue ->
            ok;
        OtherValue ->
            throw({assertion_error, Field, ExpectedValue, OtherValue})
    end.

%% @private
-spec get_record_field(#file_info{}, atom()) -> term().
get_record_field(Record, Field) ->
    FieldsList = record_info(fields, file_info),
    Index = lists_utils:index_of(Field, FieldsList),
    element(Index + 1, Record).


-spec read_file(node(), binary()) -> {ok, binary()} | {error, term()}.
read_file(Worker, FilePath) ->
    rpc:call(Worker, file, read_file, [FilePath]).


-spec read_file_info(node(), binary()) -> {ok, #file_info{}} | {error, term()}.
read_file_info(Worker, FilePath) ->
    rpc:call(Worker, file, read_file_info, [FilePath]).


-spec list_dir(node(), binary()) -> {ok, [file:filename()]} | {error, term()}.
list_dir(Worker, DirPath) ->
    rpc:call(Worker, file, list_dir, [DirPath]).


%% @doc Path of the space dir on the storage supporting the space.
-spec space_path(node(), od_space:id()) -> binary().
space_path(Worker, SpaceId) ->
    file_path(Worker, SpaceId, <<"">>).


%% @doc Path of a file on the storage supporting the space, given its path
%% relative to the space dir.
-spec file_path(node(), od_space:id(), file_meta:path()) -> binary().
file_path(Worker, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, FilePath]).


%% @private
-spec get_space_mount_point(node(), od_space:id()) -> binary().
get_space_mount_point(Worker, SpaceId) ->
    {ok, StorageId} = get_supporting_storage_id(Worker, SpaceId),
    IsImportedStorage = rpc:call(Worker, storage, is_imported, [StorageId]),
    StorageMountPoint = storage_mount_point(Worker, StorageId),
    case IsImportedStorage of
        true -> StorageMountPoint;
        false -> filename:join([StorageMountPoint, SpaceId])
    end.


-spec get_supporting_storage_id(node(), od_space:id()) -> {ok, storage:id()} | {error, term()}.
get_supporting_storage_id(Worker, SpaceId) ->
    rpc:call(Worker, space_logic, get_local_supporting_storage, [SpaceId]).


%%--------------------------------------------------------------------
%% @doc
%% Id under which the given file is kept on the storage supporting its space.
%% Unlike the path of a file in a space, this depends on the storage: a canonical
%% one mirrors the path, while a flat one derives the id from the file uuid (see
%% storage_file_id). Resolve it while the file still exists - the id outlives the
%% file, which is what lets a test check what became of the storage file after
%% the file itself was deleted.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_file_id(node(), file_id:file_guid()) -> helpers:file_id().
get_storage_file_id(Worker, FileGuid) ->
    FileCtx = rpc:call(Worker, file_ctx, new_by_guid, [FileGuid]),
    {StorageFileId, _} = rpc:call(Worker, file_ctx, get_storage_file_id, [FileCtx]),
    StorageFileId.


%% @private
-spec get_helper(node(), storage:id()) -> helpers:helper().
get_helper(Worker, StorageId) ->
    rpc:call(Worker, storage, get_helper, [StorageId]).


-spec storage_mount_point(node(), storage:id()) -> binary().
storage_mount_point(Worker, StorageId) ->
    Helper = get_helper(Worker, StorageId),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).


%% @private
-spec is_posix_compatible_storage(node(), storage:id()) -> boolean().
is_posix_compatible_storage(Worker, StorageId) ->
    Helper = get_helper(Worker, StorageId),
    helper:is_posix_compatible(Helper).


-spec ensure_file_created_on_storage(node(), file_id:file_guid()) -> ok.
ensure_file_created_on_storage(Node, FileGuid) ->
    % Open and close file in dir to ensure it is created on storage.
    {ok, Handle} = lfm_proxy:open(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), write),
    ok = lfm_proxy:close(Node, Handle).


-spec ensure_dir_created_on_storage(node(), file_id:file_guid()) -> ok.
ensure_dir_created_on_storage(Node, DirGuid) ->
    % Create and open file in dir to ensure it is created on storage.
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, ?ROOT_SESS_ID, DirGuid, <<"__tmp_file">>, 8#777
    )),
    {ok, Handle} = lfm_proxy:open(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), write),
    ok = lfm_proxy:close(Node, Handle),

    % Remove file to ensure it will not disturb tests
    ok = lfm_proxy:unlink(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid)).


-spec assert_file_owner_on_posix_storage(node(), od_space:id(), file_meta:path(), session:id()) ->
    ok | no_return().
assert_file_owner_on_posix_storage(Node, SpaceId, LogicalFilePath, ExpOwnerSessionId) ->
    assert_file_attrs_on_posix_storage(Node, SpaceId, LogicalFilePath, ExpOwnerSessionId, #{}).


-spec assert_file_attrs_on_posix_storage(node(), od_space:id(), file_meta:path(), session:id(), map()) ->
    ok | no_return().
assert_file_attrs_on_posix_storage(Node, SpaceId, LogicalFilePath, ExpOwnerSessionId, ExpAttrs) ->
    {ok, StorageId} = get_supporting_storage_id(Node, SpaceId),

    case is_posix_compatible_storage(Node, StorageId) of
        true ->
            {ok, UserId} = rpc:call(Node, session, get_user_id, [ExpOwnerSessionId]),
            {ok, UidAndGidAttrs} = rpc:call(Node, luma, map_to_storage_credentials, [
                UserId, SpaceId, StorageId
            ]),
            ExpOwnerPosixAttrs = ExpAttrs#{
                uid => binary_to_integer(maps:get(<<"uid">>, UidAndGidAttrs)),
                gid => binary_to_integer(maps:get(<<"gid">>, UidAndGidAttrs))
            },

            StorageFilePath = get_storage_file_path(Node, SpaceId, LogicalFilePath),
            assert_file_info(ExpOwnerPosixAttrs, Node, StorageFilePath, ?LINE, 0);
        false ->
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_storage_file_path(node(), od_space:id(), file_meta:path()) -> binary().
get_storage_file_path(Node, SpaceId, LogicalPath) ->
    [_Sep, _SpaceName | PathTokens] = filepath_utils:split(LogicalPath),
    file_path(Node, SpaceId, filepath_utils:join(PathTokens)).
