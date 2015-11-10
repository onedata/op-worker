%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module hosts all oneprovider modules (fslogic, cluster_rengin etc.).
%%% It makes it easy to manage modules and provides some basic functionality
%%% for its plug-ins (oneprovider modules) e.g. requests management.
%%% @end
%%%-------------------------------------------------------------------
-module(worker_host).
-author("Michal Wrzeszcz").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("cluster/worker/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").

%% This record is used by worker_host (it contains its state). It describes
%% plugin that is used and state of this plugin. It contains also
%% information about time of requests processing (used by ccm during
%% load balancing). The two list of Loads are accessible, When second list
%% becomes full, it overrides first list . Loads storing works similarly to
%% round robin databases
-record(host_state, {
    plugin = non :: module(),
    load_info :: {
        Loads1 :: list(),
        Loads2 :: list(),
        NumberOfLoads :: integer(),
        LoadMemorySize :: integer()
    }}).

%% API
-export([start_link/3, stop/1, proc_request/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([state_get/2, state_put/3, state_update/3, state_delete/2, state_to_map/1]).

-type plugin_state() :: #{term() => term()}.
-export_type([plugin_state/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts host with apropriate plug-in
%% @end
%%--------------------------------------------------------------------
-spec start_link(Plugin, PluginArgs, LoadMemorySize) -> Result when
    Plugin :: atom(),
    PluginArgs :: term(),
    LoadMemorySize :: integer(),
    Result :: {ok, Pid}
    | ignore
    | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
start_link(Plugin, PluginArgs, LoadMemorySize) ->
    gen_server:start_link({local, Plugin}, ?MODULE, [Plugin, PluginArgs, LoadMemorySize], []).

%%--------------------------------------------------------------------
%% @doc
%% Stops the server
%% @end
%%--------------------------------------------------------------------
-spec stop(Plugin) -> ok when
    Plugin :: atom().
stop(Plugin) ->
    gen_server:cast(Plugin, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State}
    | {ok, State, Timeout}
    | {ok, State, hibernate}
    | {stop, Reason :: term()}
    | ignore,
    State :: term(),
    Timeout :: non_neg_integer() | infinity.
init([Plugin, PluginArgs, LoadMemorySize]) ->
    process_flag(trap_exit, true),
    ets:new(state_table_name(Plugin), [named_table, public, set, {read_concurrency, true}]),
    {ok, InitAns} = Plugin:init(PluginArgs),
    [ets:insert(state_table_name(Plugin), Entry) || Entry <- maps:to_list(InitAns)],
    ?debug("Plugin ~p initialized with args ~p and result ~p", [Plugin, PluginArgs, InitAns]),
    {ok, #host_state{plugin = Plugin, load_info = {[], [], 0, LoadMemorySize}}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
    Result :: {reply, Reply, NewState}
    | {reply, Reply, NewState, Timeout}
    | {reply, Reply, NewState, hibernate}
    | {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, Reply, NewState}
    | {stop, Reason, NewState},
    Reply :: term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().
handle_call({update_plugin_state, Key, UpdateFun}, _From, State = #host_state{plugin = Plugin}) when is_function(UpdateFun) ->
    OldValue = state_get(Plugin, Key),
    NewValue = UpdateFun(OldValue),
    {reply, state_put(Plugin, Key, NewValue), State};

handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% Spawning migrated to worker proxy
%% handle_cast(#worker_request{} = Req, State = #host_state{plugin = Plugin}) ->
%%     spawn(fun() -> proc_request(Plugin, Req) end),
%%     {noreply, State};

handle_cast({progress_report, Report}, State) ->
    NewLoadInfo = save_progress(Report, State#host_state.load_info),
    {noreply, State#host_state{load_info = NewLoadInfo}};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
handle_info({'EXIT', Pid, Reason}, State) ->
    Plugin = State#host_state.plugin,
    gen_server:cast(Plugin, #worker_request{req = {'EXIT', Pid, Reason}}),
    {noreply, State};

handle_info({timer, Msg}, State) ->
    gen_server:cast(self(), Msg),
    {noreply, State};

handle_info(Msg, State) ->
    gen_server:cast(self(), Msg),
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
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
terminate(_Reason, #host_state{plugin = Plugin}) ->
    Plugin:cleanup(),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% Returns Value for given Key form plugin's KV state or 'undefined' if the is no such entry.
%% @end
%%--------------------------------------------------------------------
-spec state_get(Plugin :: atom(), Key :: term()) -> Value :: term() | undefined.
state_get(Plugin, Key) ->
    case ets:lookup(state_table_name(Plugin), Key) of
        [{_, Value}] -> Value;
        []           -> undefined
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates Value for given Key in plugin's KV state using given function. The function gets as argument old Value and
%% shall return new Value. The function runs in worker_host's process.
%% @end
%%--------------------------------------------------------------------
-spec state_update(Plugin :: atom(), Key :: term(), UpdateFun :: fun((OldValue :: term()) -> NewValue :: term())) ->
    ok | no_return().
state_update(Plugin, Key, UpdateFun) when is_function(UpdateFun) ->
    ok = gen_server:call(Plugin, {update_plugin_state, Key, UpdateFun}).


%%--------------------------------------------------------------------
%% @doc
%% Puts Value for given Key into plugin's KV state.
%% @end
%%--------------------------------------------------------------------
-spec state_put(Plugin :: atom(), Key :: term(), Value :: term()) -> ok | no_return().
state_put(Plugin, Key, Value) ->
    true = ets:insert(state_table_name(Plugin), {Key, Value}),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Removes entry for given Key in plugin's KV state.
%% @end
%%--------------------------------------------------------------------
-spec state_delete(Plugin :: atom(), Key :: term()) -> ok | no_return().
state_delete(Plugin, Key) ->
    true = ets:delete(state_table_name(Plugin), Key),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns whole plugin's KV state as Erlang's map.
%% @end
%%--------------------------------------------------------------------
-spec state_to_map(Plugin :: atom()) -> plugin_state().
state_to_map(Plugin) ->
    maps:from_list(ets:tab2list(state_table_name(Plugin))).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes client request using Plugin:handle function. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
%% @end
%%--------------------------------------------------------------------
-spec proc_request(Plugin :: atom(), Request :: #worker_request{}) -> Result when
    Result :: atom().
proc_request(Plugin, Request = #worker_request{req = Msg}) ->
    BeforeProcessingRequest = os:timestamp(),
    Response =
        try
            Plugin:handle(Msg)
        catch
            Type:Error ->
                ?error_stacktrace("Worker plug-in ~p error: ~p:~p, on request: ~p", [Plugin, Type, Error, Request]),
                worker_plugin_error
        end,
    send_response(Plugin, BeforeProcessingRequest, Request, Response).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends responce to client
%% @end
%%--------------------------------------------------------------------
-spec send_response(Plugin :: atom(), BeforeProcessingRequest :: term(), Request :: #worker_request{}, Response :: term()) -> atom().
send_response(Plugin, BeforeProcessingRequest, #worker_request{id = MsgId, reply_to = ReplyTo}, Response) ->
    case ReplyTo of
        undefined -> ok;
        {gen_serv, Serv} ->
            case MsgId of
                undefined -> gen_server:cast(Serv, Response);
                Id ->
                    gen_server:cast(Serv, #worker_answer{id = Id, response = Response})
            end;
        {proc, Pid} ->
            case MsgId of
                undefined -> Pid ! Response;
                Id -> Pid ! #worker_answer{id = Id, response = Response}
            end
    end,

    AfterProcessingRequest = os:timestamp(),
    Time = timer:now_diff(AfterProcessingRequest, BeforeProcessingRequest),
    gen_server:cast(Plugin, {progress_report, {BeforeProcessingRequest, Time}}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds information about ended request to host memory (ccm uses
%% it to control cluster load).
%% @end
%%--------------------------------------------------------------------
-spec save_progress(Report :: term(), LoadInfo :: term()) -> NewLoadInfo when
    NewLoadInfo :: term().
save_progress(Report, {New, Old, NewListSize, Max}) ->
    case NewListSize + 1 of
        Max ->
            {[], [Report | New], 0, Max};
        S ->
            {[Report | New], Old, S, Max}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the name of ETS table holding plugin's state.
%% @end
%%--------------------------------------------------------------------
-spec state_table_name(Plugin :: atom()) -> atom().
state_table_name(Plugin) ->
    Plugin.