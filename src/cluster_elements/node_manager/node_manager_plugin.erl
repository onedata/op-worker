%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Plugin which extends node manager for op_worker
%%% @end
%%%-------------------------------------------------------------------
-module(node_manager_plugin).
-author("Michal Wrzeszcz").

-behaviour(node_manager_plugin_behaviour).

-include("global_definitions.hrl").
-include("cluster/worker/elements/node_manager/node_manager.hrl").
-include("cluster/worker/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% node_manager_plugin_behaviour callbacks
-export([on_init/1, on_terminate/2, on_code_change/3,
  handle_call_extension/3, handle_cast_extension/2, handle_info_extension/2,
  modules/0, modules_with_args/0, listeners/0]).

%%%===================================================================
%%% node_manager_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% {@link node_manager_plugin_behaviour}  callback on_init/0.
%% @end
%%--------------------------------------------------------------------
-spec on_init(Args :: term()) -> Result when
  Result :: {ok, State}
  | {ok, State, Timeout}
  | {ok, State, hibernate}
  | {stop, Reason :: term()}
  | ignore,
  State :: term(),
  Timeout :: non_neg_integer() | infinity.
on_init([]) ->
  try
    lists:foreach(fun(Module) -> erlang:apply(Module, start_listener, []) end, node_manager:listeners()),
    ?info("All listeners started"),

    next_mem_check(),
    next_task_check(),

    %% Load NIFs
    ok = helpers_nif:init(),
    ok
  catch
    _:Error ->
      ?error_stacktrace("Cannot start node_manager plugin: ~p", [Error]),
      {error, cannot_start_node_manager_plugin}
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call_extension(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
  Result :: {reply, Reply, NewState}
  | {reply, Reply, NewState, Timeout}
  | {reply, Reply, NewState, hibernate}
  | {noreply, NewState}
  | {noreply, NewState, Timeout}
  | {noreply, NewState, hibernate}
  | {stop, Reason, Reply, NewState}
  | {stop, Reason, NewState},
  Reply :: nagios_handler:healthcheck_response() | term(),
  NewState :: term(),
  Timeout :: non_neg_integer() | infinity,
  Reason :: term().

% only for tests
handle_call_extension(check_mem_synch, _From, State) ->
  Ans = case monitoring:get_memory_stats() of
          [{<<"mem">>, MemUsage}] ->
            case caches_controller:should_clear_cache(MemUsage) of
              true ->
                free_memory(MemUsage);
              _ ->
                ok
            end;
          _ ->
            cannot_check_mem_usage
        end,
  {reply, Ans, State};

% only for tests
handle_call_extension(clear_mem_synch, _From, State) ->
  caches_controller:delete_old_keys(locally_cached, 0),
  caches_controller:delete_old_keys(globally_cached, 0),
  {reply, ok, State};

handle_call_extension(disable_cache_control, _From, State) ->
  {reply, ok, State#state{cache_control = false}};

handle_call_extension(enable_cache_control, _From, State) ->
  {reply, ok, State#state{cache_control = true}};

handle_call_extension(_Request, _From, State) ->
  ?log_bad_request(_Request),
  {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast_extension(Request :: term(), State :: term()) -> Result when
  Result :: {noreply, NewState}
  | {noreply, NewState, Timeout}
  | {noreply, NewState, hibernate}
  | {stop, Reason :: term(), NewState},
  NewState :: term(),
  Timeout :: non_neg_integer() | infinity.


handle_cast_extension(check_mem, #state{monitoring_state = MonState, cache_control = CacheControl,
  last_cache_cleaning = Last} = State) when CacheControl =:= true ->
  MemUsage = monitoring:mem_usage(MonState),
  % Check if memory cleaning of oldest docs should be started
  % even when memory utilization is low (e.g. once a day)
  NewState = case caches_controller:should_clear_cache(MemUsage) of
               true ->
                 spawn(fun() -> free_memory(MemUsage) end),
                 State#state{last_cache_cleaning = os:timestamp()};
               _ ->
                 Now = os:timestamp(),
                 {ok, CleaningPeriod} = application:get_env(?APP_NAME, clear_cache_max_period_ms),
                 case timer:now_diff(Now, Last) >= 1000000 * CleaningPeriod of
                   true ->
                     spawn(fun() -> free_memory() end),
                     State#state{last_cache_cleaning = Now};
                   _ ->
                     State
                 end
             end,
  next_mem_check(),
  {noreply, NewState};

handle_cast_extension(check_mem, State) ->
  next_mem_check(),
  {noreply, State};

handle_cast_extension(check_tasks, State) ->
  spawn(task_manager, check_and_rerun_all, []),
  next_task_check(),
  {noreply, State};

handle_cast_extension(_Request, State) ->
  ?log_bad_request(_Request),
  {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info_extension(Info :: timeout | term(), State :: term()) -> Result when
  Result :: {noreply, NewState}
  | {noreply, NewState, Timeout}
  | {noreply, NewState, hibernate}
  | {stop, Reason :: term(), NewState},
  NewState :: term(),
  Timeout :: non_neg_integer() | infinity.

handle_info_extension(_Request, State) ->
  ?log_bad_request(_Request),
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
-spec on_terminate(Reason, State :: term()) -> Any :: term() when
  Reason :: normal
  | shutdown
  | {shutdown, term()}
  | term().
on_terminate(_Reason, _State) ->
  lists:foreach(fun(Module) -> erlang:apply(Module, stop_listener, []) end, node_manager:listeners()),
  ?info("All listeners stopped").

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec on_code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
  Result :: {ok, NewState :: term()} | {error, Reason :: term()},
  OldVsn :: Vsn | {down, Vsn},
  Vsn :: term().
on_code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears memory caches.
%% @end
%%--------------------------------------------------------------------
-spec free_memory(NodeMem :: number()) -> ok | mem_usage_too_high | cannot_check_mem_usage | {error, term()}.
free_memory(NodeMem) ->
  try
    AvgMem = gen_server:call({global, ?CCM}, get_avg_mem_usage),
    ClearingOrder = case NodeMem >= AvgMem of
                      true ->
                        [{false, locally_cached}, {false, globally_cached}, {true, locally_cached}, {true, globally_cached}];
                      _ ->
                        [{false, globally_cached}, {false, locally_cached}, {true, globally_cached}, {true, locally_cached}]
                    end,
    ?info("Clearing memory in order: ~p", [ClearingOrder]),
    lists:foldl(fun
      ({_Aggressive, _StoreType}, ok) ->
        ok;
      ({Aggressive, StoreType}, _) ->
        Ans = caches_controller:clear_cache(NodeMem, Aggressive, StoreType),
        case Ans of
          mem_usage_too_high ->
            ?warning("Not able to free enough memory clearing cache ~p with param ~p", [StoreType, Aggressive]);
          _ ->
            ok
        end,
        Ans
    end, start, ClearingOrder)
  catch
    E1:E2 ->
      ?error_stacktrace("Error during caches cleaning ~p:~p", [E1, E2]),
      {error, E2}
  end.

free_memory() ->
  try
    ClearingOrder = [{false, globally_cached}, {false, locally_cached}],
    lists:foreach(fun
      ({Aggressive, StoreType}) ->
        caches_controller:clear_cache(100, Aggressive, StoreType)
    end, ClearingOrder)
  catch
    E1:E2 ->
      ?error_stacktrace("Error during caches cleaning ~p:~p", [E1, E2]),
      {error, E2}
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Plans next memory checking.
%% @end
%%--------------------------------------------------------------------
-spec next_mem_check() -> TimerRef :: reference().
next_mem_check() ->
  {ok, IntervalMin} = application:get_env(?APP_NAME, check_mem_interval_minutes),
  Interval = timer:minutes(IntervalMin),
  % random to reduce probability that two nodes clear memory simultanosly
  erlang:send_after(crypto:rand_uniform(round(0.8 * Interval), round(1.2 * Interval)), self(), {timer, check_mem}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Plans next tasks list checking.
%% @end
%%--------------------------------------------------------------------
-spec next_task_check() -> TimerRef :: reference().
next_task_check() ->
  {ok, IntervalMin} = application:get_env(?APP_NAME, task_checking_period_minutes),
  Interval = timer:minutes(IntervalMin),
  erlang:send_after(Interval, self(), {timer, check_tasks}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% {@link node_manager_plugin_behaviour} callback modules/0.
%% @end
%%--------------------------------------------------------------------
-spec modules() -> Models :: [atom()].
modules() -> [
  datastore_worker,
  dns_worker,
  session_manager_worker,
  http_worker,
  fslogic_worker
].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% {@link node_manager_plugin_behaviour} callback listeners/0.
%% @end
%%--------------------------------------------------------------------
-spec listeners() -> Listeners :: [atom()].
listeners() -> [
  start_dns_listener,
  start_gui_listener,
  start_protocol_listener,
  start_redirector_listener,
  start_rest_listener
].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% {@link node_manager_plugin_behaviour} callback modules_with_args/0.
%% @end
%%--------------------------------------------------------------------
-spec modules_with_args() -> Models :: [{atom(), [any()]}].
modules_with_args() -> [
  {datastore_worker, []},
  {dns_worker, []},
  {session_manager_worker, [
    {supervisor_spec, session_manager_worker:supervisor_spec()},
    {supervisor_child_spec, session_manager_worker:supervisor_child_spec()}
  ]},
  {http_worker, []},
  {fslogic_worker, []}
].

