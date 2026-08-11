%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Helpers for the (envup based) transfer test suites: reading and removing
%%% transfer documents, asserting the state of a #transfer{} record and
%%% mocking the replica synchronizer.
%%% @end
%%%-------------------------------------------------------------------
-module(transfers_test_utils).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include("transfers/transfers_test_mechanism.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    provider_id/1, ensure_transfers_removed/1,
    list_ended_transfers/2, list_waiting_transfers/2, list_ongoing_transfers/2,
    get_ongoing_transfers_for_file/2, get_ended_transfers_for_file/2,
    remove_transfers/1, unmock_replication_worker/1,
    root_name/2, root_name/3,
    mock_replica_synchronizer_failure/1,
    unmock_replica_synchronizer_failure/1, remove_all_views/2
]).
-export([assert_transfer_state/4]).


-define(RANDOM_NAMESPACE_SIZE, 1073741824). % 1024 ^ 3

%% Expected values (or predicates) of #transfer{} record fields, keyed by field name.
-type transfer_expectation() :: #{atom() => term() | fun((term()) -> boolean())}.
-export_type([transfer_expectation/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_transfer(node(), transfer:id()) -> transfer:transfer() | no_return().
get_transfer(Node, TransferId) ->
    case rpc:call(Node, transfer, get, [TransferId]) of
        {ok, #document{value = Transfer}} ->
            Transfer;
        {error, not_found} ->
            throw(transfer_not_found)
    end.

-spec provider_id(node()) -> od_provider:id().
provider_id(Node) ->
    rpc:call(Node, oneprovider, get_id, []).

-spec ensure_transfers_removed(test_config:config()) -> ok | no_return().
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

-spec list_ended_transfers(node(), od_space:id()) -> [transfer:id()].
list_ended_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_ended_transfers, [SpaceId]),
    Transfers.

-spec list_waiting_transfers(node(), od_space:id()) -> [transfer:id()].
list_waiting_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_waiting_transfers, [SpaceId]),
    Transfers.

-spec list_ongoing_transfers(node(), od_space:id()) -> [transfer:id()].
list_ongoing_transfers(Worker, SpaceId) ->
    {ok, Transfers} = rpc:call(Worker, transfer, list_ongoing_transfers, [SpaceId]),
    Transfers.

-spec get_ongoing_transfers_for_file(node(), undefined | file_id:file_guid()) -> [transfer:id()].
get_ongoing_transfers_for_file(_Worker, undefined) ->
    [];
get_ongoing_transfers_for_file(Worker, FileGuid) ->
    {ok, #{ongoing := Transfers}} = rpc:call(Worker, transferred_file, get_transfers, [FileGuid]),
    lists:sort(Transfers).

-spec get_ended_transfers_for_file(node(), undefined | file_id:file_guid()) -> [transfer:id()].
get_ended_transfers_for_file(_Worker, undefined) ->
    [];
get_ended_transfers_for_file(Worker, FileGuid) ->
    {ok, #{ended := Transfers}} = rpc:call(Worker, transferred_file, get_transfers, [FileGuid]),
    lists:sort(Transfers).

-spec remove_transfers(test_config:config()) -> ok.
remove_transfers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        {ok, SpaceIds} = rpc:call(Worker, provider_logic, get_spaces, []),
        lists:foreach(fun(SpaceId) ->
            Ongoing = list_ongoing_transfers(Worker, SpaceId),
            Past = list_ended_transfers(Worker, SpaceId),
            Scheduled = list_waiting_transfers(Worker, SpaceId),
            lists:foreach(fun(Tid) ->
                rpc:call(Worker, transfer, delete, [Tid])
            end, lists:umerge([Ongoing, Past, Scheduled]))
        end, SpaceIds)
    end, Workers).

-spec unmock_replication_worker(node() | [node()]) -> ok.
unmock_replication_worker(Node) ->
    test_utils:mock_unload(Node, replication_worker).

-spec root_name(FunctionName :: atom() | binary(), Type :: atom() | binary()) -> binary().
root_name(FunctionName, Type) ->
    root_name(FunctionName, Type, <<"">>).

-spec root_name(
    FunctionName :: atom() | binary(), Type :: atom() | binary(), FileKeyType :: atom() | binary()
) ->
    binary().
root_name(FunctionName, Type, FileKeyType) ->
    RandIntBin = str_utils:to_binary(rand:uniform(?RANDOM_NAMESPACE_SIZE)),
    root_name(FunctionName, Type, FileKeyType, RandIntBin).

%% @private
-spec root_name(
    FunctionName :: atom() | binary(), Type :: atom() | binary(),
    FileKeyType :: atom() | binary(), RandomSuffix :: binary()
) ->
    binary().
root_name(FunctionName, Type, FileKeyType, RandomSuffix) ->
    TypeBin = str_utils:to_binary(Type),
    FileKeyTypeBin = str_utils:to_binary(FileKeyType),
    FunctionNameBin = str_utils:to_binary(FunctionName),
    SuffixBin = str_utils:to_binary(RandomSuffix),
    <<FunctionNameBin/binary, "_", TypeBin/binary, "_", FileKeyTypeBin/binary, "_", SuffixBin/binary>>.

-spec mock_replica_synchronizer_failure(node() | [node()]) -> ok.
mock_replica_synchronizer_failure(Node) ->
    ok = test_utils:mock_new(Node, replica_synchronizer),
    ok = test_utils:mock_expect(Node, replica_synchronizer, synchronize,
        fun(_, _, _, _, _, _, _) ->
            throw(test_error) end
    ).

-spec unmock_replica_synchronizer_failure(node() | [node()]) -> ok.
unmock_replica_synchronizer_failure(Node) ->
    ok = test_utils:mock_unload(Node, replica_synchronizer).

-spec remove_all_views([node()], od_space:id()) -> ok.
remove_all_views(Nodes, SpaceId) ->
    lists:foreach(fun(Node) ->
        {ok, ViewNames} = rpc:call(Node, index, list, [SpaceId]),
        lists:foreach(fun(ViewName) ->
            ok = rpc:call(Node, index, delete, [SpaceId, ViewName])
        end, ViewNames)
    end, Nodes).

-spec assert_transfer_state(
    node(), transfer:id(), transfer_expectation(), Attempts :: non_neg_integer()
) ->
    ok | no_return().
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

%% @private
-spec assert_transfer_state(transfer_expectation(), transfer:transfer()) -> ok | no_return().
assert_transfer_state(ExpectedTransfer, Transfer) ->
    maps:fold(fun(FieldName, ExpectedValueOrPredicate, _AccIn) ->
        assert_transfer_field(ExpectedValueOrPredicate, Transfer, FieldName)
    end, undefined, ExpectedTransfer).

%% @private
-spec assert_transfer_field(
    term() | fun((term()) -> boolean()), transfer:transfer(), FieldName :: atom()
) ->
    ok | no_return().
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

%% @private
-spec get_transfer_value(transfer:transfer(), FieldName :: atom()) -> term() | no_return().
get_transfer_value(Transfer, FieldName) ->
    FieldsList = record_info(fields, transfer),

    case lists_utils:index_of(FieldName, FieldsList) of
        undefined ->
            throw({wrong_assertion_key, FieldName, FieldsList});
        Index ->
            element(Index + 1, Transfer)
    end.

%% @private
-spec transfer_fields_description(node(), transfer:id()) ->
    {Format :: string(), Args :: [term()]}.
transfer_fields_description(Node, TransferId) ->
    FieldsList = record_info(fields, transfer),
    Transfer = get_transfer(Node, TransferId),
    lists:foldl(fun(FieldName, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~tp = ~tp~n", AccArgs ++ [FieldName, get_transfer_value(Transfer, FieldName)]}
    end, {"~nTransfer ~tp fields values:~n", [TransferId]}, FieldsList).
