%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains util functions used in tests of transfers.
%%% @end
%%%-------------------------------------------------------------------
-module(transfers_test_utils).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include("transfers_test_mechanism.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    get_transfer/2, provider_id/1, ensure_transfers_removed/1,
    list_ended_transfers/2, list_waiting_transfers/2, list_ongoing_transfers/2,
    get_ongoing_transfers_for_file/2, get_ended_transfers_for_file/2,
    remove_transfers/1, unmock_replication_worker/1,
    root_name/2, root_name/3,
    mock_replica_synchronizer_failure/1,
    unmock_replica_synchronizer_failure/1, remove_all_views/2
]).
-export([assert_transfer_state/4]).


-define(RANDOM_NAMESPACE_SIZE, 1073741824). % 1024 ^ 3

%%%===================================================================
%%% API
%%%===================================================================

get_transfer(Node, TransferId) ->
    case rpc:call(Node, transfer, get, [TransferId]) of
        {ok, #document{value = Transfer}} ->
            Transfer;
        {error, not_found} ->
            throw(transfer_not_found)
    end.

provider_id(Node) ->
    rpc:call(Node, oneprovider, get_id, []).

ensure_transfers_removed(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        {ok, SpaceIds} = rpc:call(Worker, provider_logic, get_spaces, []),
        lists:foreach(fun(SpaceId) ->
            ?assertMatch([], list_ended_transfers(Worker, SpaceId), ?ATTEMPTS),
            ?assertMatch([], list_ongoing_transfers(Worker, SpaceId), ?ATTEMPTS),
            ?assertMatch([], list_waiting_transfers(Worker, SpaceId), ?ATTEMPTS)
        end, SpaceIds)
    end, Workers).

list_ended_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_ended_transfers, [SpaceId]),
    Transfers.

list_waiting_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_waiting_transfers, [SpaceId]),
    Transfers.

list_ongoing_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_ongoing_transfers, [SpaceId]),
    Transfers.

get_ongoing_transfers_for_file(_Worker, undefined) ->
    [];
get_ongoing_transfers_for_file(Worker, FileGuid) ->
    {ok, #{ongoing := Transfers}} = rpc:call(Worker, transferred_file, get_transfers, [FileGuid]),
    lists:sort(Transfers).

get_ended_transfers_for_file(_Worker, undefined) ->
    [];
get_ended_transfers_for_file(Worker, FileGuid) ->
    {ok, #{ended := Transfers}} = rpc:call(Worker, transferred_file, get_transfers, [FileGuid]),
    lists:sort(Transfers).

remove_transfers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        {ok, SpaceIds} = rpc:call(Worker, provider_logic, get_spaces, []),
        lists:foreach(fun(SpaceId) ->
            Ongoing = transfers_test_utils:list_ongoing_transfers(Worker, SpaceId),
            Past = transfers_test_utils:list_ended_transfers(Worker, SpaceId),
            Scheduled = transfers_test_utils:list_waiting_transfers(Worker, SpaceId),
            lists:foreach(fun(Tid) ->
                rpc:call(Worker, transfer, delete, [Tid])
            end, lists:umerge([Ongoing, Past, Scheduled]))
        end, SpaceIds)
    end, Workers).

unmock_replication_worker(Node) ->
    test_utils:mock_unload(Node, replication_worker).

root_name(FunctionName, Type) ->
    root_name(FunctionName, Type, <<"">>).

root_name(FunctionName, Type, FileKeyType) ->
    RandIntBin = str_utils:to_binary(rand:uniform(?RANDOM_NAMESPACE_SIZE)),
    root_name(FunctionName, Type, FileKeyType, RandIntBin).

root_name(FunctionName, Type, FileKeyType, RandomSuffix) ->
    TypeBin = str_utils:to_binary(Type),
    FileKeyTypeBin = str_utils:to_binary(FileKeyType),
    FunctionNameBin = str_utils:to_binary(FunctionName),
    SuffixBin = str_utils:to_binary(RandomSuffix),
    <<FunctionNameBin/binary, "_", TypeBin/binary, "_", FileKeyTypeBin/binary, "_", SuffixBin/binary>>.

mock_replica_synchronizer_failure(Node) ->
    ok = test_utils:mock_new(Node, replica_synchronizer),
    ok = test_utils:mock_expect(Node, replica_synchronizer, synchronize,
        fun(_, _, _, _, _, _, _) ->
            throw(test_error) end
    ).

unmock_replica_synchronizer_failure(Node) ->
    ok = test_utils:mock_unload(Node, replica_synchronizer).

remove_all_views(Nodes, SpaceId) ->
    lists:foreach(fun(Node) ->
        {ok, ViewNames} = rpc:call(Node, index, list, [SpaceId]),
        lists:foreach(fun(ViewName) ->
            ok = rpc:call(Node, index, delete, [SpaceId, ViewName])
        end, ViewNames)
    end, Nodes).

assert_transfer_state(Node, TransferId, ExpectedTransfer, Attempts) ->
    try
        Transfer = get_transfer(Node, TransferId),
        assert_transfer_state(ExpectedTransfer, Transfer)
    catch
        throw:transfer_not_found ->
            case Attempts == 0 of
                false ->
                    timer:sleep(timer:seconds(1)),
                    assert_transfer_state(Node, TransferId, ExpectedTransfer, Attempts - 1);
                true ->
                    ct:pal("Transfer: ~tp not found.", [TransferId]),
                    ct:fail("Transfer: ~tp not found.", [TransferId])
            end;
        throw:{assertion_error, Field, Expected, Value} ->
            case Attempts == 0 of
                false ->
                    timer:sleep(timer:seconds(1)),
                    assert_transfer_state(Node, TransferId, ExpectedTransfer, Attempts - 1);
                true ->
                    {Format, Args} = transfer_fields_description(Node, TransferId),
                    ct:pal(
                        "Assertion of field \"~tp\" in transfer ~tp failed.~n"
                        "    Expected: ~tp~n"
                        "    Value: ~tp~n" ++ Format, [Field, TransferId, Expected, Value | Args]),
                    ct:fail("assertion failed")
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

assert_transfer_state(ExpectedTransfer, Transfer) ->
    maps:fold(fun(FieldName, ExpectedValueOrPredicate, _AccIn) ->
        assert_transfer_field(ExpectedValueOrPredicate, Transfer, FieldName)
    end, undefined, ExpectedTransfer).

assert_transfer_field(ExpectedValueOrPredicate, Transfer, FieldName) ->
    Value = get_transfer_value(Transfer, FieldName),
    try
        case is_function(ExpectedValueOrPredicate, 1) of
            true ->
                case ExpectedValueOrPredicate(Value) of
                    true ->
                        ok;
                    false ->
                        throw({assertion_error, FieldName, <<"<pred>">>, Value})
                end;
            false ->
                case Value of
                    ExpectedValueOrPredicate ->
                        ok;
                    _ ->
                        throw({assertion_error, FieldName, ExpectedValueOrPredicate, Value})
                end
        end
    catch error:{assertMatch_failed, _} ->
        throw({assertion_error, FieldName, ExpectedValueOrPredicate, Value})
    end.

get_transfer_value(Transfer, FieldName) ->
    FieldsList = record_info(fields, transfer),

    case lists_utils:index_of(FieldName, FieldsList) of
        undefined ->
            throw({wrong_assertion_key, FieldName, FieldsList});
        Index ->
            element(Index + 1, Transfer)
    end.

transfer_fields_description(Node, TransferId) ->
    FieldsList = record_info(fields, transfer),
    Transfer = transfers_test_utils:get_transfer(Node, TransferId),
    lists:foldl(fun(FieldName, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~tp = ~tp~n", AccArgs ++ [FieldName, get_transfer_value(Transfer, FieldName)]}
    end, {"~nTransfer ~tp fields values:~n", [TransferId]}, FieldsList).
