%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This worker is responsible for managing harvest streams.
%%% It tracks changes in harvesters list for each supported space
%%% and orders harvest_stream_sup to start/stop specific harvest streams.
%%% It uses consistent_hashing to decide whether stream for given pair
%%% {HarvesterId, SpaceId} should be started on current node.
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_manager).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/harvest/harvest.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([start_link/0, check_eff_harvesters_streams/1, check_harvester_streams/3]).

%% exported for RPC
-export([check_eff_harvesters_streams_internal/1, check_harvester_streams_internal/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

% exported for tests
-export([get_harvester/1]).

% requests
-define(CHECK_HARVESTER_STREAMS, check_harvester_streams).
-define(CHECK_HARVESTER_STREAMS(HarvesterId, Spaces, Indices),
    {?CHECK_HARVESTER_STREAMS, HarvesterId, Spaces, Indices}).
-define(CHECK_EFF_HARVESTERS_STREAMS, check_eff_harvesters_streams).
-define(CHECK_EFF_HARVESTERS_STREAMS(EffHarvesters),
    {?CHECK_EFF_HARVESTERS_STREAMS, EffHarvesters}).

-type state() :: #{od_harvester:id() => #{od_space:id() => sets:set(od_harvester:index())}}.

% helper record that identifies single harvest_stream
-record(harvest_stream, {
    harvester_id :: od_harvester:id(),
    space_id :: od_space:id(),
    index_id :: od_harvester:index()
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?HARVEST_MANAGER}, ?MODULE, [], []).

-spec check_eff_harvesters_streams([od_harvester:id()]) -> ok.
check_eff_harvesters_streams(EffHarvesters) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    rpc:multicall(Nodes, ?MODULE, check_eff_harvesters_streams_internal, [EffHarvesters]),
    ok.

-spec check_harvester_streams(od_harvester:id(), [od_space:id()],
    [od_harvester:index_id()]) -> ok.
check_harvester_streams(HarvesterId, Spaces, Indices) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    rpc:multicall(Nodes, ?MODULE, check_harvester_streams_internal, [HarvesterId, Spaces, Indices]),
    ok.

%%%===================================================================
%%% RPC
%%%===================================================================

-spec check_eff_harvesters_streams_internal([od_harvester:id()]) -> ok.
check_eff_harvesters_streams_internal(EffHarvesters) ->
    gen_server:cast(?HARVEST_MANAGER, ?CHECK_EFF_HARVESTERS_STREAMS(EffHarvesters)).

-spec check_harvester_streams_internal(od_harvester:id(), [od_space:id()],
    [od_harvester:index_id()]) -> ok.
check_harvester_streams_internal(HarvesterId, Spaces, Indices) ->
    gen_server:cast(?HARVEST_MANAGER, ?CHECK_HARVESTER_STREAMS(HarvesterId, Spaces, Indices)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes the worker.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, State :: state()}.
init([]) ->
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) -> {noreply, NewState :: state()}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()}.
handle_cast(?CHECK_EFF_HARVESTERS_STREAMS(EffHarvesters), State) ->
    {noreply, check_eff_harvesters_streams(EffHarvesters, State)};
handle_cast(?CHECK_HARVESTER_STREAMS(HarvesterId, Spaces, Indices), State) ->
    State2 = check_harvester_streams(HarvesterId, Spaces, Indices, State),
    {noreply, State2};
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()}.
handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term().
terminate(Reason, State) ->
    ?log_terminate(Reason, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function checks currently started harvest_streams according to
%% passed CurrentEffHarvesters list. It compares the CurrentEffHarvesters
%% list to PreviousHarvesters. For each harvester missing in current list
%% its streams are stopped.
%% For each current effective harvester, it's document is fetched
%% to trigger call to od_harvester:save and posthook that module.
%% That posthook is responsible for notifying harvest_manager about
%% changes in harvesters' records.
%% @end
%%-------------------------------------------------------------------
-spec check_eff_harvesters_streams([od_harvester:id()], state()) -> state().
check_eff_harvesters_streams(CurrentEffHarvesters, State) ->
    PreviousHarvesters = maps:keys(State),
    DeletedHarvesters = PreviousHarvesters -- CurrentEffHarvesters,

    State2 = lists:foldl(fun(HarvesterId, StateIn) ->
        stop_harvester_streams(HarvesterId, StateIn)
    end, State, DeletedHarvesters),

    lists:foreach(fun(HarvesterId) ->
        % trigger call to od_harvester:save function
        ?MODULE:get_harvester(HarvesterId)
    end, CurrentEffHarvesters),
    State2.


-spec get_harvester(od_harvester:id()) -> {ok, od_harvester:doc()} | {error, term()}.
get_harvester(HarvesterId) ->
    harvester_logic:get(HarvesterId).

%%-------------------------------------------------------------------
%% @private
%% @equiv check_harvester_streams(HarvesterId, [], [], State).
%% @end
%%-------------------------------------------------------------------
-spec stop_harvester_streams(od_harvester:id(), state()) -> state().
stop_harvester_streams(HarvesterId, State) ->
    check_harvester_streams(HarvesterId, [], [], State).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function checks currently started harvest_streams for given
%% HarvesterId, according to passed CurrentSpaces and CurrentIndices parameters.
%% It checks which streams should be handled on given node and
%% compares set of PreviousStreams with set of LocalCurrentStreams.
%% Streams missing in the LocalCurrentStreams are stopped and
%% streams that appeared in the CurrentStreams are started.
%% @end
%%-------------------------------------------------------------------
-spec check_harvester_streams(od_harvester:id(), [od_space:id()],
    [od_harvester:index()], state()) -> state().
check_harvester_streams(HarvesterId, CurrentSpaces, CurrentIndices, State) ->
    PreviousStreams = maps:get(HarvesterId, State, sets:new()),
    CurrentStreamsList = generate_streams(HarvesterId, CurrentSpaces, CurrentIndices),
    LocalCurrentStreams = sets:from_list(filter_streams_to_be_handled_locally(CurrentStreamsList)),
    StreamsToStop = sets:subtract(PreviousStreams, LocalCurrentStreams),
    StreamsToStart = sets:subtract(LocalCurrentStreams, PreviousStreams),

    lists:foreach(fun(HS) -> stop_stream(HS) end, sets:to_list(StreamsToStop)),
    lists:foreach(fun(HS) -> start_stream(HS) end, sets:to_list(StreamsToStart)),
    case sets:size(LocalCurrentStreams) =:= 0 of
        true -> maps:remove(HarvesterId, State);
        _ -> State#{HarvesterId  => LocalCurrentStreams}
    end.

-spec filter_streams_to_be_handled_locally([#harvest_stream{}]) -> [#harvest_stream{}].
filter_streams_to_be_handled_locally(HarvestStreams) ->
    lists:filter(fun(HS) -> should_be_handled_locally(HS) end, HarvestStreams).

-spec should_be_handled_locally(#harvest_stream{}) -> boolean().
should_be_handled_locally(#harvest_stream{
    harvester_id = HarvesterId,
    space_id = SpaceId,
    index_id = IndexId
}) ->
    consistent_hashing:get_node({HarvesterId, SpaceId, IndexId}) =:= node().

-spec stop_stream(#harvest_stream{}) -> ok.
stop_stream(#harvest_stream{
    harvester_id = HarvesterId,
    space_id = SpaceId,
    index_id = IndexId
}) ->
    harvest_stream_sup:terminate_child(HarvesterId, SpaceId, IndexId).

-spec start_stream(#harvest_stream{}) -> ok.
start_stream(#harvest_stream{
    harvester_id = HarvesterId,
    space_id = SpaceId,
    index_id = IndexId
}) ->
    harvest_stream_sup:start_child(HarvesterId, SpaceId, IndexId).

-spec harvest_stream(od_harvester:id(), od_space:id(), od_harvester:index()) ->
    #harvest_stream{}.
harvest_stream(HarvesterId, SpaceId, IndexId) ->
    #harvest_stream{
        harvester_id = HarvesterId,
        space_id = SpaceId,
        index_id = IndexId
    }.

-spec generate_streams(od_harvester:id(), [od_space:id()], [od_harvester:index()]) ->
    [#harvest_stream{}].
generate_streams(HarvesterId, Spaces, Indices) ->
    lists:flatmap(fun(SpaceId) ->
        [harvest_stream(HarvesterId, SpaceId, IndexId) || IndexId <- Indices]
    end, Spaces).
