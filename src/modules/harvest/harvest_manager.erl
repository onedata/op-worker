%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This worker is responsible for managing harvest streams.
%%% It tracks changes in harvesters list for each supported space
%%% and orders harvest_stream_sup to start/stop specific harvest streams.
%%% Streams are revised by manager when changes from onezone trigger posthooks
%%% in od_harvester and od_provider models.
%%% Currently, one harvest stream is started per triple
%%% {HarvesterId, SpaceId, IndexId}. Harvest stream is responsible
%%% for tracking and pushing metadata changes to oz.
%%% It uses consistent_hashing to decide whether stream for given triple
%%% {HarvesterId, SpaceId, IndexId} should be started on current node.
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
-export([start_link/0, revise_all_streams/0, revise_all_streams/1,
    revise_streams_of_harvester/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

% exported for tests
-export([]).

% requests
-define(REVISE_STREAMS_OF_HARVESTER(HarvesterId, Spaces, Indices),
    {revise_streams_of_harvester, HarvesterId, Spaces, Indices}).
-define(REVISE_ALL_STREAMS(Harvesters),
    {revise_all_streams, Harvesters}).

% helper record that identifies single harvest_stream
-record(harvest_stream, {
    harvester_id :: od_harvester:id(),
    space_id :: od_space:id(),
    index_id :: od_harvester:index()
}).

-type state() :: #{od_harvester:id() => #{od_space:id() => sets:set(#harvest_stream{})}}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | {error, Reason :: term()}.
start_link() ->
    gen_server2:start_link({local, ?HARVEST_MANAGER}, ?MODULE, [], []).

-spec revise_all_streams() -> ok.
revise_all_streams() ->
    {ok, Harvesters} = provider_logic:get_eff_harvesters(),
    revise_all_streams(Harvesters).

-spec revise_all_streams([od_harvester:id()]) -> ok.
revise_all_streams(Harvesters) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    gen_server2:abcast(Nodes, ?HARVEST_MANAGER,
        ?REVISE_ALL_STREAMS(Harvesters)),
    ok.

-spec revise_streams_of_harvester(od_harvester:id(), [od_space:id()],
    [od_harvester:index_id()]) -> ok.
revise_streams_of_harvester(HarvesterId, Spaces, Indices) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    gen_server2:abcast(Nodes, ?HARVEST_MANAGER,
        ?REVISE_STREAMS_OF_HARVESTER(HarvesterId, Spaces, Indices)),
    ok.

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
handle_cast(?REVISE_ALL_STREAMS(Harvesters), State) ->
    {noreply, revise_all_streams(Harvesters, State)};
handle_cast(?REVISE_STREAMS_OF_HARVESTER(HarvesterId, Spaces, Indices), State) ->
    State2 = revise_streams_of_harvester(HarvesterId, Spaces, Indices, State),
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
%% This function revises currently started harvest_streams according to
%% passed CurrentHarvesters list. It compares the CurrentHarvesters
%% list to PreviousHarvesters. For each harvester missing in current list
%% its streams are stopped.
%% For each current harvester, it's document is fetched to revise
%% its streams.
%% @end
%%-------------------------------------------------------------------
-spec revise_all_streams([od_harvester:id()], state()) -> state().
revise_all_streams(CurrentHarvesters, State) ->
    PreviousHarvesters = maps:keys(State),
    DeletedHarvesters = PreviousHarvesters -- CurrentHarvesters,
    NewHarvesters = CurrentHarvesters -- PreviousHarvesters,

    State2 = lists:foldl(fun(HarvesterId, StateIn) ->
        stop_streams_of_harvester(HarvesterId, StateIn)
    end, State, DeletedHarvesters),

    lists:foreach(fun(HarvesterId) ->
        {ok, HarvesterDoc} = harvester_logic:get(HarvesterId),
        {ok, Indices} = harvester_logic:get_indices(HarvesterDoc),
        {ok, Spaces} = harvester_logic:get_spaces(HarvesterDoc),
        revise_streams_of_harvester(HarvesterId, Spaces, Indices)
    end, NewHarvesters),
    State2.


%%-------------------------------------------------------------------
%% @private
%% @equiv revise_streams_of_harvester(HarvesterId, [], [], State).
%% @end
%%-------------------------------------------------------------------
-spec stop_streams_of_harvester(od_harvester:id(), state()) -> state().
stop_streams_of_harvester(HarvesterId, State) ->
    revise_streams_of_harvester(HarvesterId, [], [], State).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function revises currently started harvest_streams for given
%% HarvesterId, according to passed CurrentSpaces and CurrentIndices parameters.
%% It checks which streams should be handled on given node and
%% compares set of PreviousStreams with set of LocalCurrentStreams.
%% Streams missing in the LocalCurrentStreams are stopped and
%% streams that appeared in the CurrentStreams are started.
%% @end
%%-------------------------------------------------------------------
-spec revise_streams_of_harvester(od_harvester:id(), [od_space:id()],
    [od_harvester:index()], state()) -> state().
revise_streams_of_harvester(HarvesterId, CurrentSpaces, CurrentIndices, State) ->
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
    lists:filter(fun should_be_handled_locally/1, HarvestStreams).

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

-spec generate_streams(od_harvester:id(), [od_space:id()], [od_harvester:index()]) ->
    [#harvest_stream{}].
generate_streams(HarvesterId, Spaces, Indices) ->
    lists:flatmap(fun(SpaceId) ->
        lists:map(fun(IndexId) ->
            #harvest_stream{
                harvester_id = HarvesterId,
                space_id = SpaceId,
                index_id = IndexId
            }
        end, Indices)
    end, Spaces).
