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
    update_all_spaces_harvest_streams_on_all_nodes/0]).

%% exported for rpc
-export([update_space_harvest_streams/2, update_all_spaces_harvest_streams/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

% requests
-define(INITIALISE, initialise).
-define(UPDATE, update).
-define(UPDATE(SpaceId, Harvesters), {?UPDATE, SpaceId, Harvesters}).
-define(UPDATE_ALL, update_all).

-define(INITIALISATION_TIMEOUT, timer:seconds(5)).

-type state() :: #{od_space:id() => sets:set(harvest_stream:id())}.

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

-spec update_all_spaces_harvest_streams_on_all_nodes() -> ok.
update_all_spaces_harvest_streams_on_all_nodes() ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    rpc:multicall(Nodes, ?MODULE, update_all_spaces_harvest_streams, []),
    ok.

%%%===================================================================
%%% RPC
%%%===================================================================

-spec update_space_harvest_streams(od_space:id(), [od_harvester:id()]) -> ok.
update_space_harvest_streams(SpaceId, Harvesters) ->
    gen_server:cast(?HARVEST_MANAGER, ?UPDATE(SpaceId, Harvesters)).

-spec update_all_spaces_harvest_streams() -> ok.
update_all_spaces_harvest_streams() ->
    gen_server:cast(?HARVEST_MANAGER, ?UPDATE_ALL).

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
    schedule_initialisation(),
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
handle_cast(?INITIALISE, State) ->
    {noreply, initialise(State)};
handle_cast(?UPDATE_ALL, State) ->
    {noreply, update_streams_for_all_supported_spaces(State)};
handle_cast({?UPDATE, SpaceId, Harvesters}, State) ->
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

-spec initialise(state()) -> ok.
initialise(State) ->
    try provider_logic:get_spaces(oneprovider:get_id_or_undefined()) of
        {ok, SpaceIds} ->
            lists:foldl(fun(SpaceId, StateIn) ->
                update_streams_per_space(SpaceId, StateIn)
            end, State, SpaceIds);
        ?ERROR_UNREGISTERED_PROVIDER ->
            timer:sleep(timer:seconds(?INITIALISATION_TIMEOUT)),
            schedule_initialisation(),
            State;
        ?ERROR_NO_CONNECTION_TO_OZ ->
            timer:sleep(timer:seconds(?INITIALISATION_TIMEOUT)),
            schedule_initialisation(),
            State;
        Error = {error, _} ->
            ?error("Unable to initialise harvest_manager due to: ~p", [Error]),
            State
    catch
        Error2:Reason ->
            ?error_stacktrace("Unable to initialise harvest_manager due to: ~p", [{Error2, Reason}]),
            State
    end.

-spec update_streams_for_all_supported_spaces(state()) -> state().
update_streams_for_all_supported_spaces(State) ->
    {ok, SpaceIds} = provider_logic:get_spaces(oneprovider:get_id()),
    lists:foldl(fun(SpaceId, StateIn) ->
        update_streams_per_space(SpaceId, StateIn)
    end, State, SpaceIds).

-spec update_streams_per_space(od_space:id(), state()) -> state().
update_streams_per_space(SpaceId, State) ->
    {ok, Harvesters} = space_logic:get_harvesters(?ROOT_SESS_ID, SpaceId),
    update_streams_per_space(SpaceId, Harvesters, State).

-spec update_streams_per_space(od_space:id(), [od_harvester:id()], state()) -> ok.
update_streams_per_space(SpaceId, CurrentHarvesters, State) ->
    OldStreams = maps:get(SpaceId, State, sets:new()),
    Node = node(),
    CurrentStreams = sets:from_list(lists:filtermap(fun(Harvester) ->
        Id = harvest_stream_state:id(Harvester, SpaceId),
        case Node =:= consistent_hashing:get_node(Id) of
            true ->
                {true, Id};
            false ->
                false
        end
    end, CurrentHarvesters)),

    StreamsToStart = sets:subtract(CurrentStreams, OldStreams),
    StreamsToStop = sets:subtract(OldStreams, CurrentStreams),

    lists:foreach(fun(HarvesterId) ->
        harvest_stream_sup:terminate_child(HarvesterId, SpaceId)
    end, sets:to_list(StreamsToStop)),

    lists:foreach(fun(HarvesterId) ->
        harvest_stream_sup:start_child(HarvesterId, SpaceId)
    end, sets:to_list(StreamsToStart)),

    State#{SpaceId => CurrentStreams}.


-spec schedule_initialisation() -> ok.
schedule_initialisation() ->
    gen_server:cast(self(), ?INITIALISE).