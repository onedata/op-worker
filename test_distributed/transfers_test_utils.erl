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
-include("countdown_server.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("proto/common/credentials.hrl").
%% API
-export([get_transfer/2, provider_id/1, ensure_transfers_removed/1,
    list_ended_transfers/2, list_waiting_transfers/2, list_ongoing_transfers/2,
    get_ongoing_transfers_for_file/2, get_ended_transfers_for_file/2,
    remove_transfers/1, get_space_support/2,
    mock_space_occupancy/3, unmock_space_occupancy/2, unmock_sync_req/1,
    root_name/2, root_name/3,
    mock_prolonged_replication/3, mock_replica_synchronizer_failure/1,
    unmock_replica_synchronizer_failure/1, remove_all_indexes/2, random_job_name/0,
    test_map_function/1, test_reduce_function/1, test_map_function/2,
    create_index/7, create_index/6]).

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
    ProviderId = transfers_test_utils:provider_id(Node),
    {ok, Supports} = rpc:call(Node, space_logic, get_providers_supports, [<<"0">>, SpaceId]),
    maps:get(ProviderId, Supports, 0).

mock_space_occupancy(Node, SpaceId, MockedSize) ->
    CurrentSize = rpc:call(Node, space_quota, current_size, [SpaceId]),
    rpc:call(Node, space_quota, apply_size_change, [SpaceId, MockedSize - CurrentSize]).

unmock_space_occupancy(Node, SpaceId) ->
    CurrentSize = rpc:call(Node, space_quota, current_size, [SpaceId]),
    rpc:call(Node, space_quota, apply_size_change, [SpaceId, -CurrentSize]).

unmock_sync_req(Node) ->
    test_utils:mock_unload(Node, sync_req).

root_name(FunctionName, Type) ->
    root_name(FunctionName, Type, <<"">>).

root_name(FunctionName, Type, FileKeyType) ->
    RandIntBin = str_utils:to_binary(rand:uniform(1024)),
    root_name(FunctionName, Type, FileKeyType, RandIntBin).

root_name(FunctionName, Type, FileKeyType, RandomSufix) ->
    TypeBin = str_utils:to_binary(Type),
    FileKeyTypeBin = str_utils:to_binary(FileKeyType),
    FunctionNameBin = str_utils:to_binary(FunctionName),
    SuffixBin = str_utils:to_binary(RandomSufix),
    <<FunctionNameBin/binary, "_", TypeBin/binary, "_" , FileKeyTypeBin/binary, "_", SuffixBin/binary>>.

%%-------------------------------------------------------------------
%% @doc
%% Prolongs call to sync_req:replicate_file/4 function for
%% ProlongationTime seconds with probability ProlongationProbability.
%% This function should be used to prolong duration time of transfers
%% @end
%%-------------------------------------------------------------------
-spec mock_prolonged_replication(node(), non_neg_integer(), non_neg_integer()) -> ok.
mock_prolonged_replication(Worker, ProlongationProbability, ProlongationTime) ->
    ok = test_utils:mock_new(Worker, sync_req),
    ok = test_utils:mock_expect(Worker, sync_req, replicate_file,
        fun(UserCtx, FileCtx, Block, TransferId, IndexName, QueryViewParams) ->
            case rand:uniform() < ProlongationProbability of
                true ->
                    timer:sleep(timer:seconds(ProlongationTime));
                false ->
                    ok
            end,
            meck:passthrough([UserCtx, FileCtx, Block, TransferId, IndexName, QueryViewParams])
        end).

mock_replica_synchronizer_failure(Node) ->
    test_utils:mock_new(Node, replica_synchronizer),
    test_utils:mock_expect(Node, replica_synchronizer, synchronize,
        fun(_, _, _, _, _, _) -> throw(test_error) end
    ).

unmock_replica_synchronizer_failure(Node) ->
    ok = test_utils:mock_unload(Node, replica_synchronizer).

remove_all_indexes(Nodes, SpaceId) ->
    lists:foreach(fun(Node) ->
        {ok, IndexNames} = rpc:call(Node, index, list, [SpaceId]),
        lists:foreach(fun(IndexName) ->
            ok = rpc:call(Node, index, delete, [SpaceId, IndexName])
        end, IndexNames)
    end, Nodes).

random_job_name() ->
    RandomIntBin = str_utils:to_binary(rand:uniform(1024)),
    <<"job_", RandomIntBin/binary>>.

test_map_function(XattrName) ->
    <<"function (id, meta) {
        if(meta['", XattrName/binary,"']) {
            return [meta['", XattrName/binary,"'], id];
        }
        return null;
    }">>.

test_map_function(XattrName, XattrName2) ->
    <<"function (id, meta) {
        if(meta['", XattrName/binary,"']) {
            return [meta['", XattrName/binary,"'], [id, meta['", XattrName2/binary, "']]];
        }
        return null;
    }">>.

test_reduce_function(XattrValue) ->
    XattrValueBin = str_utils:to_binary(XattrValue),
    <<"function (key, values, rereduce) {
        var filtered = [];
        for(i = 0; i < values.length; i++)
            if(values[i][1] == ", XattrValueBin/binary ,")
                filtered.push(values[i][0]);
        return filtered;
    }">>.

create_index(Worker, SpaceId, IndexName, MapFunction, Options, Providers) ->
    create_index(Worker, SpaceId, IndexName, MapFunction, undefined, Options, Providers).

create_index(Worker, SpaceId, IndexName, MapFunction, ReduceFunction, Options, Providers) ->
    ok = rpc:call(Worker, index, save, [SpaceId, IndexName, MapFunction, ReduceFunction,
        Options, false, Providers]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

