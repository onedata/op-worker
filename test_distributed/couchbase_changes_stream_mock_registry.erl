%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Registry for couchbase_changes_stream_mocks used in tests of
%%% harvesting. Mocks register in this server during intialization.
%%% It is possible to get mock's pid associated with given
%%% harvesting_steam pid.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes_stream_mock_registry).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0, stop/0, register/1, deregister/1, get/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, {global, ?MODULE}).

-define(REGISTER(HarvestingStreamPid, ChangesStreamPid),
    {register, HarvestingStreamPid, ChangesStreamPid}).
-define(DEREGISTER(HarvestingStreamPid),
    {deregister, HarvestingStreamPid}).
-define(GET(HarvestingStreamPid),
    {get, HarvestingStreamPid}).

-type state() :: map().

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link(?SERVER, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

register(HarvestingStreamPid) ->
    gen_server:call(?SERVER, ?REGISTER(HarvestingStreamPid, self())).

deregister(HarvestingStreamPid) ->
    gen_server:call(?SERVER, ?DEREGISTER(HarvestingStreamPid)).

get(HarvestingStreamPid) ->
    gen_server:call(?SERVER, ?GET(HarvestingStreamPid)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    {ok, #{}}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()}).
handle_call(?REGISTER(HarvestingStreamPid, ChangesStreamPid), _From, State) ->
    {reply, ok, State#{HarvestingStreamPid => ChangesStreamPid}};
handle_call(?DEREGISTER(HarvestingStreamPid), _From, State) ->
    {reply, ok, maps:remove(HarvestingStreamPid, State)};
handle_call(?GET(HarvestingStreamPid), _From, State) ->
    {reply, maps:get(HarvestingStreamPid, State, undefined), State}.


-spec(handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()}).
handle_cast(_Request, State) ->
    {noreply, State}.

-spec(handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()}).
handle_info(_Info, State) ->
    {noreply, State}.

-spec(terminate(Reason :: normal, State :: state()) -> term()).
terminate(_Reason, _State) ->
    ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
