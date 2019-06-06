%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%---- ---------------------------------------------------------------
%%% @doc
%%% Helper module for processing results of harvesting returned from
%%% Onezone.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_result).
-author("Jakub Kudzia").

-include("graph_sync/provider_graph_sync.hrl").

%% API
-export([process/3, get_summary/1, get_max_successful_seq/1]).

% record used to store processed result
-record(result, {
    % maximal sequence for which harvesting succeeded
    % if batch is empty, max_successful_seq is undefined
    max_successful_seq :: couchbase_changes:seq() | undefined,
    % map that stores association between results of harvesting to Destination
    % for which harvesting has finished with the same result
    summary = #{} :: summary()
}).

% accumulator record, used when processing the results
-record(acc, {
    failure_map :: failure_map(),
    last_batch_seq :: couchbase_changes:seq() | undefined,
    result :: result()
}).

-type failure_map() :: #{od_harvester:id() => #{od_harvester:index() => couchbase_changes:seq()}}.
-type result() :: #result{}.
% possible keys in the summary() map
%   * couchbase_changes:seq() - last sequence for which harvesting succeeded for
%     Destination associated with given sequence
%   * gs_protocol:error() - error returned from Graph Sync for
%     Destination associated with given sequence
%   * undefined - if sent batch was empty, and the call was successful
%     whole Destination is associated with undefined key
-type summary_key() :: couchbase_changes:seq() | gs_protocol:error() | undefined.
-type summary() :: #{summary_key() => harvesting_destination:destination()} | gs_protocol:error().
-type acc() :: #acc{}.

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Processes harvesting result.
%% This function returns #result{} structure that is more convenient for
%% interpreting the results than raw result returned from
%% space_logic:harvest_metadata function.
%% @end
%%--------------------------------------------------------------------
-spec process({ok, failure_map()} | gs_protocol:error(), harvesting_destination:destination(),
    harvesting_batch:batch()) -> result().
process(Error = {error, _}, _, Batch) ->
    Result = init_result(Batch),
    Result#result{summary = Error};
process({ok, Result}, Destination, Batch) when is_map(Result) ->
    Acc = init_acc(Result, Batch),
    #acc{result = ProcessedResult} =
        harvesting_destination:fold(fun process_internal/3, Acc, Destination),
    ProcessedResult.

-spec get_summary(result()) -> summary().
get_summary(#result{summary = Summary}) ->
    Summary.

-spec get_max_successful_seq(result()) -> couchbase_changes:seq() | undefined.
get_max_successful_seq(#result{max_successful_seq = MaxSuccessfulSeq}) ->
    MaxSuccessfulSeq.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_acc(failure_map(), harvesting_batch:batch()) -> acc().
init_acc(Result, Batch) ->
    #acc{
        failure_map = Result,
        last_batch_seq = harvesting_batch:get_last_seq(Batch),
        result = init_result(Batch)
    }.

-spec init_result(harvesting_batch:batch()) -> result().
init_result(Batch) ->
    case harvesting_batch:is_empty(Batch) of
        true ->
            #result{max_successful_seq = undefined};
        false ->
            #result{max_successful_seq = harvesting_batch:get_first_seq(Batch) - 1}
    end.

-spec process_internal(od_harvester:id(), [od_harvester:index()], acc()) -> acc().
process_internal(HarvesterId, Indices, Acc = #acc{
    failure_map = RawResult,
    result = ResultIn = #result{summary = SummaryIn},
    last_batch_seq = LastBatchSeq
}) ->
    Result = case maps:get(HarvesterId, RawResult, undefined) of
        undefined ->
            % all Harvester's indices are missing in the Result which means
            % that harvesting succeeded for them
            % we can add them to LastBatchSeq destination
            Summary = maps:update_with(LastBatchSeq, fun(Destination) ->
                harvesting_destination:add(HarvesterId, Indices, Destination)
            end, harvesting_destination:init(HarvesterId, Indices), SummaryIn),
            ResultIn#result{
                summary = Summary,
                max_successful_seq = LastBatchSeq
            };
        #{<<"error">> := ErrorBinary} ->
            Error = gs_protocol_errors:json_to_error(?GS_PROTOCOL_VERSION, ErrorBinary),
            process_error(HarvesterId, Indices, Error, ResultIn);
        Error = {error, _} ->
            process_error(HarvesterId, Indices, Error, ResultIn);
        FailedIndices when is_map(FailedIndices) ->
            lists:foldl(fun(IndexId, ResultIn2 = #result{
                summary = SummaryIn2,
                max_successful_seq = MaxSuccessfulSeqIn2
            }) ->
                LastSuccessfulSeq = case maps:get(IndexId, FailedIndices, undefined) of
                    undefined ->
                        % if IndexId is missing in FailedIndices it means that
                        % harvesting has succeeded for it and we can add it to
                        % LastBatchSeq destination
                        LastBatchSeq;
                    FailedSeq ->
                        FailedSeq - 1
                end,
                Summary = maps:update_with(LastSuccessfulSeq, fun(Destination) ->
                    harvesting_destination:add(HarvesterId, IndexId, Destination)
                end, harvesting_destination:init(HarvesterId, IndexId), SummaryIn2),
                ResultIn2#result{
                    summary = Summary,
                    max_successful_seq =  max(MaxSuccessfulSeqIn2, LastSuccessfulSeq)
                }
            end, ResultIn, Indices)
    end,
    Acc#acc{result = Result}.

-spec process_error(od_harvester:id(), [od_harvester:index()],
    gs_protocol:error(), result()) -> result().
process_error(HarvesterId, Indices, Error, ResultIn = #result{summary = SummaryIn}) ->
    % harvesting failed with Error for all indices of Harvester
    Summary = maps:update_with(Error, fun(Destination) ->
        harvesting_destination:add(HarvesterId, Indices, Destination)
    end, harvesting_destination:init(HarvesterId, Indices), SummaryIn),
    ResultIn#result{summary = Summary}.