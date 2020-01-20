%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This gen_server is responsible for aggregating requests for
%%% checking autocleaning in given space.
%%% One such gen_server is started per Oneprovider cluster.
%%% The gen_server checks autocleaning when shutting down.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_checker).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start/1, check/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER(SpaceId), {global, {?MODULE, SpaceId}}).

-define(SCHEDULE_CHECK, schedule_autocleaning_check).
-define(CHECK_AUTOCLEANING_DELAY, timer:seconds(1)).

-record(state, {space_id :: od_space:id()}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec check(od_space:id()) -> ok.
check(SpaceId) ->
    try
        gen_server2:call(?SERVER(SpaceId), ?SCHEDULE_CHECK, infinity)
    catch
        exit:{noproc, _} ->
            start(SpaceId);
        exit:{normal, _} ->
            start(SpaceId);
        exit:{{shutdown, timeout}, _} ->
            start(SpaceId)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: term()) -> {ok, state(), timeout()}.
init([SpaceId]) ->
    {ok, #state{space_id = SpaceId}, ?CHECK_AUTOCLEANING_DELAY}.

-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), state()} |
    {reply, Reply :: term(), state(), timeout()}.
handle_call(?SCHEDULE_CHECK, _From, State) ->
    {reply, ok, State, ?CHECK_AUTOCLEANING_DELAY};
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, {error, bad_request}, State}.

-spec handle_cast(Request :: term(), state()) -> {noreply, state()}.
handle_cast(Request, State = #state{}) ->
    ?log_bad_request(Request),
    {noreply, State}.

-spec handle_info(Info :: timeout() | term(), state()) -> {stop, Reason :: term(), state()}.
handle_info(timeout, State = #state{}) ->
    {stop, normal, State}.

-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), state()) -> term().
terminate(normal, _State = #state{space_id = SpaceId}) ->
    autocleaning_api:check(SpaceId);
terminate(Reason, State) ->
    ?log_terminate(Reason, State).

-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, state()} | {error, Reason :: term()}.
code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal callbacks
%%%===================================================================

-spec start(od_space:id()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start(SpaceId) ->
    Node = datastore_key:responsible_node(SpaceId),
    rpc:call(Node, gen_server2, start, [?SERVER(SpaceId), ?MODULE, [SpaceId], []]).