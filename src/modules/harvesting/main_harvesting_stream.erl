%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements harvesting_stream behaviour.
%%% main_harvesting_stream is responsible for harvesting metadata changes
%%% in the given space. Metadata changes are pushed per specific Destination.
%%% If new harvesters or indices are added to Destination, main_harvesting_stream
%%% starts aux_harvesting_streams. Each of them is responsible for catching up
%%% harvesting for a pair {HarvesterId, IndexId} with main_harvesting_stream.
%%% main_harvesting_stream updates its destination and starts
%%% aux_harvesting_streams when handling
%%% ?REVISE_HARVESTER and ?REVISE_SPACE_HARVESTERS messages.
%%% These messages are sent from od_space and od_harvesters posthooks.
%%% @end
%%%-------------------------------------------------------------------
-module(main_harvesting_stream).
-author("Jakub Kudzia").

-behaviour(harvesting_stream).

-include("global_definitions.hrl").
-include("modules/harvesting/harvesting.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([propose_takeover/2, revise_harvester/3,
    revise_space_harvesters/2, space_removed/1, space_unsupported/1, revise_all_spaces/0]).

%% harvesting_stream callbacks
-export([init/1, name/1, handle_call/3, handle_cast/2, custom_error_handling/2,
    on_end_of_stream/1, on_harvesting_doc_not_found/1]).

%% RPC
-export([call_internal/2, space_removed_internal/1, space_unsupported_internal/1]).

% Revising a harvester means comparing current list of its indices with
% previous list. Indices that are missing in the current list are
% removed from stream destination and aux_harvesting_streams are stopped.
% For indices that are new on the current list, aux_harvesting_streams are
% started.
-define(REVISE_HARVESTER(HarvesterId, Indices),
    {revise_harvester, HarvesterId, Indices}).

% Revising space harvesters means comparing current list of space's harvesters
% with previous list. Harvesters that are missing in the current list are
% removed from stream destination and aux_harvesting_streams are stopped.
% For harvesters that are new on the current list, ?REVISE_HARVESTER is
% scheduled.
-define(REVISE_SPACE_HARVESTERS(Harvesters),
    {revise_space_harvesters, Harvesters}).

%%%===================================================================
%%% API
%%%===================================================================

-spec propose_takeover(harvesting_stream:name(), couchbase_changes:seq()) -> ok.
propose_takeover(Name = ?AUX_HARVESTING_STREAM(SpaceId, _, _), Seq) ->
    multicall_internal(SpaceId, ?TAKEOVER_PROPOSAL(Name, Seq)).

-spec revise_harvester(od_space:id(), od_harvester:id(), [od_harvester:index()]) -> ok.
revise_harvester(SpaceId, HarvesterId, Indices) ->
    multicall_internal(SpaceId, ?REVISE_HARVESTER(HarvesterId, Indices)).

-spec revise_space_harvesters(od_space:id(), [od_harvester:id()]) -> ok.
revise_space_harvesters(SpaceId, Harvesters) ->
    multicall_internal(SpaceId, ?REVISE_SPACE_HARVESTERS(Harvesters)).

-spec space_removed(od_space:id()) -> ok.
space_removed(SpaceId) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, ?MODULE, space_removed_internal, [SpaceId]).

-spec space_unsupported(od_space:id()) -> ok.
space_unsupported(SpaceId) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, ?MODULE, space_unsupported_internal, [SpaceId]).

-spec revise_all_spaces() -> ok.
revise_all_spaces() ->
    case provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:foreach(fun(SpaceId) ->
                case space_logic:get_harvesters(SpaceId) of
                    {ok, HarvesterIds} ->
                        revise_space_harvesters(SpaceId, HarvesterIds);
                    Error ->
                        ?error("Couldn't fetch list of space ~p harvesters due to ~w", [SpaceId, Error])
                end
            end, SpaceIds);
        Error2 ->
            ?error("Couldn't fetch list of provider spaces due to ~w", [Error2])
    end.

%%%===================================================================
%%% harvesting_stream callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init([term()]) -> {ok, harvesting_stream:state()} | {stop, term()}.
init([SpaceId]) ->
    case space_logic:get_harvesters(SpaceId) of
        {ok, HarvesterIds} ->
            harvesting_state:ensure_created(SpaceId),
            {ok, HDoc} = harvesting_state:get(SpaceId),
            DestinationsPerSeqs = build_destinations_per_seqs(HarvesterIds, HDoc),
            case maps:size(DestinationsPerSeqs) =:= 0 of
                true ->
                    {stop, normal};
                false ->
                    MainSeq = lists:max(maps:keys(DestinationsPerSeqs)),
                    {MainDestination, AuxDestinationsPerSeqs} =
                        maps:take(MainSeq, DestinationsPerSeqs),
                    AuxDestination =
                        harvesting_destination:merge(maps:values(AuxDestinationsPerSeqs)),
                    schedule_start_aux_streams(AuxDestination, MainSeq),
                    {ok, #hs_state{
                        name = ?MAIN_HARVESTING_STREAM(SpaceId),
                        space_id = SpaceId,
                        until = infinity,
                        destination = MainDestination,
                        last_seen_seq = MainSeq
                    }}
            end;
        _ ->
            {stop, normal}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback name/1.
%% @end
%%--------------------------------------------------------------------
-spec name([term()]) -> harvesting_stream:name().
name([SpaceId | _]) ->
    ?MAIN_HARVESTING_STREAM(SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback handle_call/3.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    harvesting_stream:state()) -> harvesting_stream:handling_result().
handle_call(Request, From, State) ->
    gen_server2:reply(From, ok),
    handle_call_async(Request, State).

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback handle_cast/2.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: harvesting_stream:state()) ->
    harvesting_stream:handling_result().
handle_cast(?SPACE_REMOVED, State = #hs_state{space_id = SpaceId}) ->
    broadcast_space_removed_message(State),
    harvesting_state:delete(SpaceId),
    {stop, normal, State};
handle_cast(?SPACE_UNSUPPORTED, State) ->
    broadcast_space_unsupported_message(State),
    {stop, normal, State};
handle_cast(?START_AUX_STREAMS(AuxDestination, Until), State = #hs_state{space_id = SpaceId}) ->
    start_aux_streams(SpaceId, AuxDestination, Until),
    {noreply, State#hs_state{
        aux_destination = AuxDestination
    }};
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback custom_error_handling/2.
%% @end
%%--------------------------------------------------------------------
-spec custom_error_handling(harversting_stream:state(), harvesting_result:result()) ->
    harvesting_stream:handling_result().
custom_error_handling(State = #hs_state{
    space_id = SpaceId,
    batch = Batch,
    last_seen_seq = LastSeenSeq
}, Result) ->
    MaxSuccessfulSeq = harvesting_result:get_max_successful_seq(Result),
    FirstBatchSeq = harvesting_batch:get_first_seq(Batch),
    LastBatchSeq = harvesting_batch:get_last_seq(Batch),

    case {LastBatchSeq =:= MaxSuccessfulSeq, FirstBatchSeq =< MaxSuccessfulSeq} of
        {true, _} ->
            % harvesting of whole batch succeeded for at least one index
            MaxSeenSeq = max(LastSeenSeq, MaxSuccessfulSeq),
            {MainDestination2, AuxDestination2} = start_aux_streams_according_to_summary(State, Result),
            case harvesting_state:set_seen_seq(SpaceId, MainDestination2, MaxSeenSeq) of
                ok ->
                    {noreply, harvesting_stream:enter_streaming_mode(State#hs_state{
                        destination = MainDestination2,
                        aux_destination = AuxDestination2,
                        last_seen_seq = MaxSeenSeq,
                        last_persisted_seq = MaxSeenSeq,
                        last_harvest_timestamp = time_utils:system_time_seconds()
                    })};
                ?ERROR_NOT_FOUND ->
                    harvesting_stream:throw_harvesting_not_found_exception(State)
            end;
        {false, true} ->
            % harvesting of whole batch didn't succeed for any of the indices
            % but it succeeded partially for at least one index
            % we should increase seen_seq
            {MainDestination2, AuxDestination2} = start_aux_streams_according_to_summary(State, Result),
            case harvesting_state:set_seen_seq(SpaceId, MainDestination2, MaxSuccessfulSeq) of
                ok ->
                    ErrorLog = str_utils:format_bin(
                        "Unexpected errors occurred when applying batch of changes. "
                        "Last successful sequence number was: ~p", [MaxSuccessfulSeq]
                    ),
                    {noreply, harvesting_stream:enter_retrying_mode(State#hs_state{
                        error_log = ErrorLog,
                        log_level = error,
                        destination = MainDestination2,
                        aux_destination = AuxDestination2,
                        last_persisted_seq = MaxSuccessfulSeq,
                        batch = harvesting_batch:strip(Batch, MaxSuccessfulSeq)
                    })};
                ?ERROR_NOT_FOUND ->
                    harvesting_stream:throw_harvesting_not_found_exception(State)
            end;
        {false, false} ->
            % none of the changes in the batch where successfully
            % applied for any of the indices
            ErrorLog = <<"Unexpected errors occurred when applying batch of changes. "
            "None of sequences were successfully applied.">>,
            {noreply, harvesting_stream:enter_retrying_mode(State#hs_state{
                error_log = ErrorLog,
                log_level = error
            })}
    end.

-spec start_aux_streams_according_to_summary(harvesting_stream:state(), harvesting_result:result()) ->
    {harvesting_destination:destination(), harvesting_destination:destination()}.
start_aux_streams_according_to_summary(State = #hs_state{
    space_id = SpaceId,
    aux_destination = AuxDestination
}, Result) ->
    MaxSuccessfulSeq = harvesting_result:get_max_successful_seq(Result),
    Summary = harvesting_result:get_summary(Result),
    maps:fold(fun
        (?ERROR_NOT_FOUND, _ErrorDest, AccIn) ->
            % Harvesters in _ErrorDest were deleted
            % we can ignore this error
            AccIn;
        (?ERROR_FORBIDDEN, _ErrorDest, AccIn) ->
            % Harvesters in _ErrorDest were deleted from space
            % we can ignore this error
            AccIn;

        (?ERROR_TEMPORARY_FAILURE, ErrorDest, {DestIn, AuxDestIn}) ->
            harvesting_destination:foreach(fun(HarvesterId, Indices) ->
                ?warning("Harvester ~p is temporarily unavailable."
                "Starting aux_harvesting_streams", [HarvesterId]),
                lists:foreach(fun(IndexId) ->
                    harvesting_stream_sup:start_aux_stream(SpaceId,
                        HarvesterId, IndexId, MaxSuccessfulSeq)
                end, Indices)
            end, ErrorDest),
            {DestIn, harvesting_destination:merge(AuxDestIn, ErrorDest)};

        (Error = {error, _}, ErrorDest, {DestIn, AuxDestIn}) ->
            harvesting_destination:foreach(fun(HarvesterId, Indices) ->
                ?error("Unexpected error ~p occurred for harvester ~p. "
                "Starting aux_harvesting_stream", [Error, HarvesterId]),
                lists:foreach(fun(IndexId) ->
                    harvesting_stream_sup:start_aux_stream(SpaceId,
                        HarvesterId, IndexId, MaxSuccessfulSeq)
                end, Indices)
            end, ErrorDest),
            {DestIn, harvesting_destination:merge(AuxDestIn, ErrorDest)};

        (Seq, Dest, {_DestIn, AuxDestIn}) when Seq =:= MaxSuccessfulSeq ->
            {Dest, AuxDestIn};

        (Seq, Dest, {DestIn, AuxDestIn}) ->
            harvesting_destination:foreach(fun(HarvesterId, Indices) ->
                case harvesting_state:set_aux_seen_seq(SpaceId, HarvesterId, Indices, Seq) of
                    ok ->
                        lists:foreach(fun(IndexId) ->
                            harvesting_stream_sup:start_aux_stream(SpaceId,
                                HarvesterId, IndexId, MaxSuccessfulSeq)
                        end, Indices);
                    ?ERROR_NOT_FOUND ->
                        harvesting_stream:throw_harvesting_not_found_exception(State)
                end
            end, Dest),
            {DestIn, harvesting_destination:merge(AuxDestIn, Dest)}

    end, {harvesting_destination:init(), AuxDestination}, Summary).

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback on_end_of_stream/1.
%% @end
%%--------------------------------------------------------------------
-spec on_end_of_stream(harvesting_stream:state()) -> ok.
on_end_of_stream(_State) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback on_harvesting_doc_not_found/1.
%% @end
%%--------------------------------------------------------------------
-spec on_harvesting_doc_not_found(harvesting_stream:state()) ->
    {stop, normal, harvesting_stream:state()}.
on_harvesting_doc_not_found(State) ->
    broadcast_space_removed_message(State),
    {stop, normal, State}.

%%%===================================================================
%%% Internal API
%%%===================================================================

-spec multicall_internal(od_space:id(), term()) -> ok.
multicall_internal(SpaceId, Request) ->
    Nodes = consistent_hashing:get_all_nodes(),
    rpc:multicall(Nodes, ?MODULE, call_internal, [SpaceId, Request]),
    ok.

-spec call_internal(od_space:id(), term()) -> term().
call_internal(SpaceId, Request) ->
    case consistent_hashing:get_node(SpaceId) =:= node() of
        true ->
            Name = ?MAIN_HARVESTING_STREAM(SpaceId),
            try
                ok = gen_server2:call({global, Name}, Request, infinity)
            catch
                _:{noproc, _} ->
                    ?debug("Stream ~p noproc, retrying with a new one", [Name]),
                    case harvesting_stream_sup:start_main_stream(SpaceId) of
                        ok ->
                            call_internal(SpaceId, Request);
                        {error, normal} ->
                            ok
                    end;
                Error:Reason ->
                    ?error_stacktrace("
                    Unexpected error in main_harvesting_stream:call_internal: ~p",
                        [{Error, Reason}])
            end;
        false ->
            ok
    end.

-spec space_removed_internal(od_space:id()) -> ok.
space_removed_internal(SpaceId) ->
    gen_server2:cast({global, ?MAIN_HARVESTING_STREAM(SpaceId)}, ?SPACE_REMOVED).

-spec space_unsupported_internal(od_space:id()) -> ok.
space_unsupported_internal(SpaceId) ->
    gen_server2:cast({global, ?MAIN_HARVESTING_STREAM(SpaceId)}, ?SPACE_UNSUPPORTED).

-spec schedule_start_aux_streams(harvesting_destination:destination(),
    couchbase_changes:seq()) -> ok.
schedule_start_aux_streams(AuxDestination, Until) ->
    gen_server2:cast(self(), ?START_AUX_STREAMS(AuxDestination, Until)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec handle_call_async(Request :: term(), State :: harvesting_stream:state()) ->
    harvesting_stream:handling_result().
handle_call_async(?TAKEOVER_PROPOSAL(Name, Seq), State = #hs_state{
    last_seen_seq = SeenSeq,
    destination = Destination,
    aux_destination = AuxDestination
}) when Seq =:= SeenSeq ->
    ?AUX_HARVESTING_STREAM(_SpaceId, HarvesterId, IndexId) = Name,
    harvesting_stream_sup:terminate_aux_stream(Name),
    {noreply, harvesting_stream:enter_streaming_mode(State#hs_state{
        destination = harvesting_destination:add(HarvesterId, IndexId, Destination),
        aux_destination = harvesting_destination:delete(HarvesterId, IndexId, AuxDestination)
    })};
handle_call_async(?TAKEOVER_PROPOSAL(Name, _Seq), State = #hs_state{last_seen_seq = SeenSeq}) ->
    aux_harvesting_stream:reject_takeover(Name, SeenSeq),
    {noreply, State};
handle_call_async(?REVISE_HARVESTER(HarvesterId, Indices), State) ->
    State2 = revise_harvester_internal(HarvesterId, Indices, State, true),
    stop_on_empty_destination(State2);
handle_call_async(?REVISE_SPACE_HARVESTERS(Harvesters), State) ->
    State2 = revise_space_harvesters_internal(Harvesters, State),
    stop_on_empty_destination(State2).


-spec build_destinations_per_seqs([od_harvester:id()], harvesting_state:doc()) ->
    #{couchbase_changes:seq() => harvesting_state:destination()}.
build_destinations_per_seqs(CurrentHarvesterIds, HDoc) ->
    lists:foldl(fun(HarvesterId, DestinationsPerSeqsIn) ->
        build_destinations_per_seqs(HarvesterId, HDoc, DestinationsPerSeqsIn)
    end, #{}, CurrentHarvesterIds).

-spec build_destinations_per_seqs(od_harvester:id(), harvesting_state:doc(),
    #{couchbase_changes:seq() => harvesting_state:destination()}) ->
    #{couchbase_changes:seq() => harvesting_state:destination()}.
build_destinations_per_seqs(HarvesterId, HDoc, DestinationsPerSeqsIn) ->
    case harvester_logic:get_indices(HarvesterId) of
        {ok, Indices} ->
            lists:foldl(fun(IndexId, DestinationsPerSeqsInIn) ->
                {ok, Seq} = harvesting_state:get_seen_seq(HDoc, HarvesterId, IndexId),
                maps:update_with(Seq, fun(SeqDestination) ->
                    harvesting_destination:add(HarvesterId, IndexId, SeqDestination)
                end, harvesting_destination:init(HarvesterId, IndexId), DestinationsPerSeqsInIn)
            end, DestinationsPerSeqsIn, Indices);
        {error, _} ->
            DestinationsPerSeqsIn
    end.

-spec start_aux_streams(od_space:id(), harvesting_destination:destination(),
    couchbase_changes:until()) -> ok.
start_aux_streams(SpaceId, AuxDestination, Until) ->
    harvesting_destination:foreach_index(fun(HarvesterId, IndexId) ->
        harvesting_stream_sup:start_aux_stream(SpaceId, HarvesterId, IndexId, Until)
    end, AuxDestination).

-spec broadcast_space_removed_message(harvesting_stream:state()) -> ok.
broadcast_space_removed_message(State) ->
    broadcast_message(State, ?SPACE_REMOVED).

-spec broadcast_space_unsupported_message(harvesting_stream:state()) -> ok.
broadcast_space_unsupported_message(State) ->
    broadcast_message(State, ?SPACE_UNSUPPORTED).

-spec broadcast_message(harvesting_stream:state(), term()) -> ok.
broadcast_message(#hs_state{
    aux_destination = AuxDestination,
    space_id = SpaceId
}, Message) ->
    harvesting_destination:foreach_index(fun(HarvesterId, IndexId) ->
        AuxStreamName = ?AUX_HARVESTING_STREAM(SpaceId, HarvesterId, IndexId),
        case Message of
            ?SPACE_REMOVED ->
                aux_harvesting_stream:space_removed(AuxStreamName);
            ?SPACE_UNSUPPORTED ->
                aux_harvesting_stream:space_unsupported(AuxStreamName)
        end
    end, AuxDestination).

-spec revise_harvester_internal(od_harvester:id(), [od_harvester:index()],
    harvesting_stream:state(), boolean()) -> harvesting_stream:state().
revise_harvester_internal(HarvesterId, CurrentIndices, State = #hs_state{
    destination = Destination,
    aux_destination = AuxDestination,
    last_seen_seq = MainSeq,
    space_id = SpaceId
}, CleanupSeenSeqs) ->

    {ok, HDoc} = harvesting_state:get(SpaceId),
    IndicesInMainStream = harvesting_destination:get(HarvesterId, Destination),
    IndicesInAuxStreams = harvesting_destination:get(HarvesterId, AuxDestination),
    IndicesInAllStreams = IndicesInMainStream ++ IndicesInAuxStreams,
    IndicesToStart = CurrentIndices -- IndicesInAllStreams,
    IndicesToStopInMainStream = IndicesInMainStream -- CurrentIndices,
    IndicesToStopInAuxStream = IndicesInAuxStreams -- CurrentIndices,
    IndicesToStop = IndicesToStopInMainStream ++ IndicesToStopInAuxStream,
    {NewIndicesInMainStream, NewIndicesInAuxStreams} = lists:foldl(
        fun(IndexId, {AccMainIndices, AccAuxIndices}) ->
            {ok, IndexSeq} = harvesting_state:get_seen_seq(HDoc, HarvesterId, IndexId),
            case IndexSeq < MainSeq of
                true ->
                    harvesting_stream_sup:start_aux_stream(SpaceId,
                        HarvesterId, IndexId, MainSeq),
                    {AccMainIndices, [IndexId | AccAuxIndices]};
                false ->
                    {[IndexId | AccMainIndices], AccAuxIndices}
            end
        end, {[], []}, IndicesToStart),

    lists:foreach(fun(IndexId) ->
        harvesting_stream_sup:terminate_aux_stream(SpaceId, HarvesterId, IndexId)
    end, IndicesToStopInAuxStream),

    case CleanupSeenSeqs of
        true ->
            harvesting_state:delete_progress_entries(SpaceId, HarvesterId, IndicesToStop);
        false ->
            ok
    end,

    Destination2 = harvesting_destination:add(HarvesterId,
        NewIndicesInMainStream, Destination),
    Destination3 = harvesting_destination:delete(HarvesterId,
        IndicesToStopInMainStream, Destination2),
    AuxDestination2 = harvesting_destination:add(HarvesterId,
        NewIndicesInAuxStreams, AuxDestination),
    AuxDestination3 = harvesting_destination:delete(HarvesterId,
        IndicesToStopInAuxStream, AuxDestination2),

    State#hs_state{
        destination = Destination3,
        aux_destination = AuxDestination3
    }.

-spec revise_space_harvesters_internal([od_harvester:id()],
    harvesting_stream:state()) -> harvesting_stream:state().
revise_space_harvesters_internal(CurrentHarvesters, State = #hs_state{
    destination = Destination,
    aux_destination = AuxDestination
}) ->
    CurrentHarvestersSet = sets:from_list(CurrentHarvesters),
    HarvestersInMainStream = harvesting_destination:get_harvesters(Destination),
    HarvestersInAuxStreams = harvesting_destination:get_harvesters(AuxDestination),
    HarvestersInMainStreamSet = sets:from_list(HarvestersInMainStream),
    HarvestersInAuxStreamsSet = sets:from_list(HarvestersInAuxStreams),
    HarvestersInAllStreamsSet = sets:union(HarvestersInMainStreamSet,
        HarvestersInAuxStreamsSet),
    HarvestersToStopSet = sets:subtract(HarvestersInAllStreamsSet,
        CurrentHarvestersSet),

    State2 = lists:foldl(fun(HarvesterId, StateIn) ->
        remove_harvester(HarvesterId, StateIn)
    end, State, sets:to_list(HarvestersToStopSet)),

    lists:foldl(fun(HarvesterId, StateIn) ->
        case harvester_logic:get_indices(HarvesterId) of
            {ok, Indices} ->
                revise_harvester_internal(HarvesterId, Indices, StateIn, true);
            _ ->
                StateIn
        end
    end, State2, CurrentHarvesters).

-spec remove_harvester(od_harvester:id(), harvesting_stream:state()) ->
    harvesting_stream:state().
remove_harvester(HarvesterId, State) ->
    CleanupSeenSeqs = case harvester_logic:get(HarvesterId) of
        {ok, _} ->
            % harvester doesn't have space handled by this stream
            false;
        ?ERROR_FORBIDDEN ->
            % harvester doesn't have spaces supported by this provider
            false;
        ?ERROR_NOT_FOUND ->
            % harvester was permanently deleted in onezone
            true
    end,
    revise_harvester_internal(HarvesterId, [], State, CleanupSeenSeqs).

-spec stop_on_empty_destination(harvesting_stream:state()) ->
    harvesting_stream:handling_result().
stop_on_empty_destination(State = #hs_state{destination = Destination}) ->
    case harvesting_destination:is_empty(Destination) of
        true ->
            {stop, normal, State};
        false ->
            {noreply, State}
    end.