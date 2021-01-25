%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utils used to analyze dbsync state.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_test_utils).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([are_all_seqs_equal/2, are_all_seqs_and_timestamps_equal/3]).
%% Exported for RPC (internal usage)
-export([get_seq_and_timestamp_or_error/2]).

%%%===================================================================
%%% API
%%%===================================================================

are_all_seqs_equal(Workers, SpaceId) ->
    are_all_seqs_and_timestamps_equal(Workers, SpaceId, undefined).

are_all_seqs_and_timestamps_equal(Workers, SpaceId, Timestamp0) ->
    ProvIds = lists:foldl(fun(W, Acc) ->
        sets:add_element(rpc:call(W, oneprovider, get_id, []), Acc)
    end, sets:new(), Workers),

    % Get list of states of all dbsync streams (for tested space) from all workers (all providers), e.g.:
    % WorkersDbsyncStates = [StateOnWorker1, StateOnWorker2 ... StateOnWorkerN],
    % StateOnWorker = [ProgressOfSynWithProv1, ProgressOfSynWithProv2 ... ProgressOfSynWithProvM]
    WorkersDbsyncStates = lists:foldl(fun(W, Acc) ->
        WorkerState = lists:foldl(fun(ProvID, Acc2) ->
            case get_seq_and_timestamp_or_error(W, SpaceId, ProvID) of
                {error, not_found} ->
                    % provider `ProvID` does not support space so there is no synchronization progress data
                    % on this worker
                    Acc2;
                {_, Timestamp} = Ans when Timestamp0 =/= undefined ->
                    case Timestamp >= Timestamp0 of
                        true -> ok;
                        false -> throw(too_small_timestamp)
                    end,
                    [Ans | Acc2];
                {Seq, _} ->
                    [Seq | Acc2]
            end
        end, [], sets:to_list(ProvIds)),

        case WorkerState of
            [] -> Acc; % this worker belongs to provider that does not support this space
            _ -> [WorkerState | Acc]
        end
    end, [], Workers),

    % States of all workers (that belong to providers that support tested space) should be equal
    % create set from list to remove duplicates
    WorkersDbsyncStatesSet = sets:from_list(WorkersDbsyncStates),
    case sets:size(WorkersDbsyncStatesSet) of
        1 -> true; % States of all workers are equal
        _ -> {false, WorkersDbsyncStates}
    end.

%%%===================================================================
%%% Helper functions
%%%===================================================================

get_seq_and_timestamp_or_error(Worker, SpaceId, ProviderId) ->
    rpc:call(Worker, ?MODULE, get_seq_and_timestamp_or_error, [SpaceId, ProviderId]).

get_seq_and_timestamp_or_error(SpaceId, ProviderId) ->
    case datastore_model:get(#{model => dbsync_state}, SpaceId) of
        {ok, #document{value = #dbsync_state{sync_progress = SyncProgress}}} ->
            maps:get(ProviderId, SyncProgress, {error, not_found});
        Error ->
            Error
    end.