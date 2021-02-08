%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions used in tests that operate on provider's
%%% storage via RPC.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_test_utils).
-author("Jakub Kudzia").

-include("storage_files_test_SUITE.hrl").

%% API
-export([
    assert_file_info/5,
    read_file/2, read_file_info/2, list_dir/2,
    space_path/2, file_path/3,
    get_space_mount_point/2, get_supporting_storage_id/2,
    storage_mount_point/2, get_helper/2,
    is_supporting_storage_posix_compatible/2, is_posix_compatible_storage/2
]).


%%%===================================================================
%%% API functions
%%%===================================================================

assert_file_info(ExpectedValues, Worker, FilePath, Line, Attempts) when Attempts >= 0 ->
    try
        {ok, FI} = storage_test_utils:read_file_info(Worker, FilePath),
        maps:map(fun(Field, ExpectedValue) ->
            assert_field(Field, ExpectedValue, FI)
        end, ExpectedValues)
    catch
        throw:(Error = {assertion_error, Field, ExpectedValue, Value}) when Attempts =:= 0 ->
            ct:pal(
                "Assertion for file ~p failed.~n"
                "   Field: ~p~n"
                "   Expected: ~p~n"
                "   Got: ~p~n"
                "   Module: ~p~n"
                "   Line: ~p",
                [FilePath, Field, ExpectedValue, Value, ?MODULE, Line]
            ),
            ct:fail(Error);
        Error:Reason when Attempts =:= 0 ->
            ct:pal(
                "Assertion for file ~p failed.~n"
                "   Error: {~p, ~p}~n"
                "   Module: ~p~n"
                "   Line: ~p",
                [FilePath, Error, Reason, ?MODULE, Line]
            ),
            ct:fail({Error, Reason});
        _:_ ->
            timer:sleep(timer:seconds(1)),
            assert_file_info(ExpectedValues, Worker, FilePath, Line, Attempts - 1)
    end.

%% @private
assert_field(Field, ExpectedValue, Record) ->
    case get_record_field(Record, Field) of
        ExpectedValue ->
            ok;
        OtherValue ->
            throw({assertion_error, Field, ExpectedValue, OtherValue})
    end.

%% @private
get_record_field(Record, Field) ->
    FieldsList = record_info(fields, file_info),
    Index = index(Field, FieldsList),
    element(Index + 1, Record).

%% @private
index(Key, List) ->
    case lists:keyfind(Key, 2, lists:zip(lists:seq(1, length(List)), List)) of
        false ->
            throw({wrong_assertion_key, Key, List});
        {Index, _} ->
            Index
    end.

read_file(Worker, FilePath) ->
    rpc:call(Worker, file, read_file, [FilePath]).

read_file_info(Worker, FilePath) ->
    rpc:call(Worker, file, read_file_info, [FilePath]).

list_dir(Worker, DirPath) ->
    rpc:call(Worker, file, list_dir, [DirPath]).

space_path(Worker, SpaceId) ->
    file_path(Worker, SpaceId, <<"">>).

file_path(Worker, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, FilePath]).

get_space_mount_point(Worker, SpaceId) ->
    {ok, StorageId} = get_supporting_storage_id(Worker, SpaceId),
    IsImportedStorage = rpc:call(Worker, storage, is_imported, [StorageId]),
    StorageMountPoint = storage_mount_point(Worker, StorageId),
    case IsImportedStorage of
        true -> StorageMountPoint;
        false -> filename:join([StorageMountPoint, SpaceId])
    end.

get_supporting_storage_id(Worker, SpaceId) ->
    rpc:call(Worker, space_logic, get_local_supporting_storage, [SpaceId]).

get_helper(Worker, StorageId) ->
    rpc:call(Worker, storage, get_helper, [StorageId]).

storage_mount_point(Worker, StorageId) ->
    Helper = get_helper(Worker, StorageId),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).

is_supporting_storage_posix_compatible(Worker, SpaceId) ->
    {ok, StorageId} = storage_test_utils:get_supporting_storage_id(Worker, SpaceId),
    is_posix_compatible_storage(Worker, StorageId).

is_posix_compatible_storage(Worker, StorageId) ->
    Helper = storage_test_utils:get_helper(Worker, StorageId),
    helper:is_posix_compatible(Helper).
