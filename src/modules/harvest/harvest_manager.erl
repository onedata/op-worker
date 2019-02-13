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
-export([start_link/0, update_space_harvest_streams_on_all_nodes/2,
    delete_space_harvest_streams_on_all_nodes/1]).

%% exported for RPC
-export([update_space_harvest_streams/2, delete_space_harvest_streams/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

% requests
-define(CHECK_ALL_SPACES, check_all_spaces).
-define(UPDATE, update).
-define(UPDATE(SpaceId, Harvesters), {?UPDATE, SpaceId, Harvesters}).
-define(DELETE, delete).
-define(DELETE(SpaceId), {?DELETE, SpaceId}).

-define(CONNECTION_TO_OZ_TIMEOUT, timer:seconds(5)).

-type state() :: #{od_space:id() => sets:set(od_harvester:id())}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?HARVEST_MANAGER}, ?MODULE, [], []).

-spec update_space_harvest_streams_on_all_nodes(od_space:id(), [od_harvester:id()]) -> ok.
update_space_harvest_streams_on_all_nodes(SpaceId, Harvesters) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    rpc:multicall(Nodes, ?MODULE, update_space_harvest_streams, [SpaceId, Harvesters]),
    ok.

-spec delete_space_harvest_streams_on_all_nodes(od_space:id()) -> ok.
delete_space_harvest_streams_on_all_nodes(SpaceId) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    rpc:multicall(Nodes, ?MODULE, delete_space_harvest_streams, [SpaceId]),
    ok.

%%%===================================================================
%%% RPC
%%%===================================================================

-spec update_space_harvest_streams(od_space:id(), [od_harvester:id()]) -> ok.
update_space_harvest_streams(SpaceId, Harvesters) ->
    gen_server:cast(?HARVEST_MANAGER, ?UPDATE(SpaceId, Harvesters)).

-spec delete_space_harvest_streams(od_space:id()) -> ok.
delete_space_harvest_streams(SpaceId) ->
    gen_server:cast(?HARVEST_MANAGER, ?DELETE(SpaceId)).

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
    schedule_check_all_spaces(),
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
handle_cast(?DELETE(SpaceId), State) ->
    {noreply, delete_streams(SpaceId, State)};
handle_cast(?UPDATE(SpaceId, Harvesters), State) ->
    {noreply, update_streams_per_space(SpaceId, Harvesters, State)};
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
handle_info(?CHECK_ALL_SPACES, State) ->
    check_all_spaces(State);
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

-spec check_all_spaces(state()) -> {noreply, state()} | {stop, term(), state()}.
check_all_spaces(State) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            State2 = lists:foldl(fun(SpaceId, StateIn) ->
                update_streams_per_space(SpaceId, StateIn)
            end, State, SpaceIds),
            {noreply, State2};
        ?ERROR_UNREGISTERED_PROVIDER ->
            ?debug("harvest_manager was unable to check_all_spaces due to unregistered provider"),
            schedule_check_all_spaces(),
            {noreply, State};
        ?ERROR_NO_CONNECTION_TO_OZ ->
            ?debug("harvest_manager was unable to check_all_spaces due to no connection to oz"),
            schedule_check_all_spaces(),
            {noreply, State};
        Error ->
            ?error("harvest_manager was unable to check_all_spaces due to: ~p", [Error]),
            {stop, Error, State}
    catch
        Error2:Reason2 ->
            ?error_stacktrace("harvest_manager was unable to check_all_spaces due to: ~p", [{Error2, Reason2}]),
            {stop, {Error2, Reason2}, State}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Stops all streams for given SpaceId by passing an empty list
%% as a list of current harvesters.
%% @end
%%-------------------------------------------------------------------
-spec delete_streams(od_space:id(), state()) -> state().
delete_streams(SpaceId, State) ->
    update_streams_per_space(SpaceId, [], State).

-spec update_streams_per_space(od_space:id(), state()) -> state().
update_streams_per_space(SpaceId, State) ->
    {ok, Harvesters} = space_logic:get_harvesters(?ROOT_SESS_ID, SpaceId),
    update_streams_per_space(SpaceId, Harvesters, State).

%%-------------------------------------------------------------------
%% @doc
%% This function updates currently started harvest_streams per given
%% SpaceId. according to passed CurrentHarvesters parameters.
%% It checks which streams should be handled on giver node and
%% compares set of OldStreams with set of CurrentStreams.
%% Streams missing in the CurrentStreams are stopped and
%% streams that appeared in the CurrentStreams are started.
%% @end
%%-------------------------------------------------------------------
-spec update_streams_per_space(od_space:id(), [od_harvester:id()], state()) -> state().
update_streams_per_space(SpaceId, CurrentHarvesters, State) ->
    OldLocalNodeHarvesters = maps:get(SpaceId, State, sets:new()),
    Node = node(),
    LocalNodeHarvesters = sets:from_list(lists:filtermap(fun(HarvesterId) ->
        case Node =:= consistent_hashing:get_node({HarvesterId, SpaceId}) of
            true ->
                {true, HarvesterId};
            false ->
                false
        end
    end, CurrentHarvesters)),

    StreamsToStart = sets:subtract(LocalNodeHarvesters, OldLocalNodeHarvesters),
    StreamsToStop = sets:subtract(OldLocalNodeHarvesters, LocalNodeHarvesters),

    lists:foreach(fun(HarvesterId) ->
        ok = harvest_stream_sup:terminate_child(HarvesterId, SpaceId)
    end, sets:to_list(StreamsToStop)),

    lists:foreach(fun(HarvesterId) ->
        ok = harvest_stream_sup:start_child(HarvesterId, SpaceId)
    end, sets:to_list(StreamsToStart)),

    case CurrentHarvesters =:= [] of
        true -> maps:remove(SpaceId, State);
        _ -> State#{SpaceId => LocalNodeHarvesters}
    end.


-spec schedule_check_all_spaces() -> ok.
schedule_check_all_spaces() ->
    erlang:send_after(?CONNECTION_TO_OZ_TIMEOUT, ?HARVEST_MANAGER, ?CHECK_ALL_SPACES),
    ok.
