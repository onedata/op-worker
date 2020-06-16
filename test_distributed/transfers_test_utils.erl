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
-include("transfers_test_mechanism.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([get_transfer/2, provider_id/1, ensure_transfers_removed/1,
    list_ended_transfers/2, list_waiting_transfers/2, list_ongoing_transfers/2,
    get_ongoing_transfers_for_file/2, get_ended_transfers_for_file/2,
    remove_transfers/1, get_space_support/2,
    mock_space_occupancy/3, unmock_space_occupancy/2, unmock_replication_worker/1,
    root_name/2, root_name/3,
    mock_prolonged_replication/3, mock_replica_synchronizer_failure/1,
    mock_prolonged_replica_eviction/3, unmock_prolonged_replica_eviction/1,
    mock_replica_eviction_failure/1, unmock_replica_eviction_failure/1,
    unmock_replica_synchronizer_failure/1, remove_all_views/2, random_job_name/1,
    test_map_function/1, test_reduce_function/1, test_map_function/2,
    create_view/7, create_view/6, random_view_name/1, unmock_prolonged_replication/1]).

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

provider_id(?MISSING_PROVIDER_NODE) ->
    % overridden for test reason
    ?MISSING_PROVIDER_ID;
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

get_space_support(Node, SpaceId) ->
    {ok, SupportSize} = rpc:call(Node, provider_logic, get_support_size, [SpaceId]),
    SupportSize.

mock_space_occupancy(Node, SpaceId, MockedSize) ->
    CurrentSize = rpc:call(Node, space_quota, current_size, [SpaceId]),
    rpc:call(Node, space_quota, apply_size_change, [SpaceId, MockedSize - CurrentSize]).

unmock_space_occupancy(Node, SpaceId) ->
    CurrentSize = rpc:call(Node, space_quota, current_size, [SpaceId]),
    rpc:call(Node, space_quota, apply_size_change, [SpaceId, -CurrentSize]).

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

%%-------------------------------------------------------------------
%% @doc
%% Prolongs call to replication_worker:transfer_regular_file/2 function for
%% ProlongationTime seconds with probability ProlongationProbability.
%% This function should be used to prolong duration time of transfers
%% @end
%%-------------------------------------------------------------------
-spec mock_prolonged_replication(node(), non_neg_integer(), non_neg_integer()) -> ok.
mock_prolonged_replication(Worker, ProlongationProbability, ProlongationTime) ->
    ok = test_utils:mock_new(Worker, replication_worker),
    ok = test_utils:mock_expect(Worker, replication_worker, transfer_regular_file,
        fun(FileCtx, TransferParams) ->
            Result = meck:passthrough([FileCtx, TransferParams]),
            case rand:uniform() < ProlongationProbability of
                true -> timer:sleep(timer:seconds(ProlongationTime));
                false -> ok
            end,
            Result
        end).

unmock_prolonged_replication(Worker) ->
    ok = test_utils:mock_unload(Worker, replication_worker).

%%-------------------------------------------------------------------
%% @doc
%% Prolongs call to replica_eviction_worker:transfer_regular_file/2 function for
%% ProlongationTime seconds with probability ProlongationProbability.
%% This function should be used to prolong duration time of replica eviction transfers.
%% @end
%%-------------------------------------------------------------------
-spec mock_prolonged_replica_eviction(node(), non_neg_integer(), non_neg_integer()) -> ok.
mock_prolonged_replica_eviction(Worker, ProlongationProbability, ProlongationTime) ->
    ok = test_utils:mock_new(Worker, replica_eviction_worker),
    ok = test_utils:mock_expect(Worker, replica_eviction_worker, transfer_regular_file,
        fun
            (FileCtx, TransferParams) ->
                Result = meck:passthrough([FileCtx, TransferParams]),
                case rand:uniform() < ProlongationProbability of
                    true -> timer:sleep(timer:seconds(ProlongationTime));
                    false -> ok
                end,
                Result
        end
    ).

unmock_prolonged_replica_eviction(Worker) ->
    ok = test_utils:mock_unload(Worker, replica_eviction_worker).

mock_replica_synchronizer_failure(Node) ->
    ok = test_utils:mock_new(Node, replica_synchronizer),
    ok = test_utils:mock_expect(Node, replica_synchronizer, synchronize,
        fun(_, _, _, _, _, _) ->
            throw(test_error) end
    ).

unmock_replica_synchronizer_failure(Node) ->
    ok = test_utils:mock_unload(Node, replica_synchronizer).

mock_replica_eviction_failure(Node) ->
    test_utils:mock_new(Node, replica_deletion_req),
    test_utils:mock_expect(Node, replica_deletion_req, delete_blocks,
        fun(_, _, _) -> {error, test_error} end
    ).

unmock_replica_eviction_failure(Node) ->
    ok = test_utils:mock_unload(Node, replica_deletion_req).

remove_all_views(Nodes, SpaceId) ->
    lists:foreach(fun(Node) ->
        {ok, ViewNames} = rpc:call(Node, index, list, [SpaceId]),
        lists:foreach(fun(ViewName) ->
            ok = rpc:call(Node, index, delete, [SpaceId, ViewName])
        end, ViewNames)
    end, Nodes).

random_job_name(FunctionName) ->
    FunctionNameBin = str_utils:to_binary(FunctionName),
    RandomIntBin = str_utils:to_binary(rand:uniform(?RANDOM_NAMESPACE_SIZE)),
    <<"job_", FunctionNameBin/binary, "_", RandomIntBin/binary>>.

random_view_name(FunctionName) ->
    FunctionNameBin = str_utils:to_binary(FunctionName),
    RandomIntBin = str_utils:to_binary(rand:uniform(?RANDOM_NAMESPACE_SIZE)),
    <<"view_", FunctionNameBin/binary, "_", RandomIntBin/binary>>.

test_map_function(XattrName) ->
    <<"function (id, type, meta, ctx) {
        if(type == 'custom_metadata' && meta['", XattrName/binary, "']) {
            return [meta['", XattrName/binary, "'], id];
        }
        return null;
    }">>.

test_map_function(XattrName, XattrName2) ->
    <<"function (id, type, meta, ctx) {
        if(type == 'custom_metadata' && meta['", XattrName/binary, "']) {
            return [meta['", XattrName/binary, "'], [id, meta['", XattrName2/binary, "']]];
        }
        return null;
    }">>.

test_reduce_function(XattrValue) ->
    XattrValueBin = str_utils:to_binary(XattrValue),
    <<"function (key, values, rereduce) {
        var filtered = [];
        for(i = 0; i < values.length; i++)
            if(values[i][1] == ", XattrValueBin/binary, ")
                filtered.push(values[i][0]);
        return filtered;
    }">>.

create_view(Worker, SpaceId, ViewName, MapFunction, Options, Providers) ->
    create_view(Worker, SpaceId, ViewName, MapFunction, undefined, Options, Providers).

create_view(Worker, SpaceId, ViewName, MapFunction, ReduceFunction, Options, Providers) ->
    ok = rpc:call(Worker, index, save, [SpaceId, ViewName, MapFunction, ReduceFunction,
        Options, false, Providers]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

