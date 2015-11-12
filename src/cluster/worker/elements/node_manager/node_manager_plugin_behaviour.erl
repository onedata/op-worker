%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is the behaviour of each node_manager plugin.
%%% @end
%%%-------------------------------------------------------------------
-module(node_manager_plugin_behaviour).
-author("Michal Zmuda").

%%--------------------------------------------------------------------
%% @doc
%% Callback executed on node manager init callback execution. At time
%% of invocation node_manager is not set init'ed yet. Use to inject
%% custom initialisation.
%% @end
%%--------------------------------------------------------------------
-callback on_init(Args :: term()) -> ok | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Extension to default node manager callback. Matched as last.
%% @end
%%--------------------------------------------------------------------
-callback handle_call_extension(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) ->
  {reply, Reply :: term(), NewState :: term()} |
  {reply, Reply :: term(), NewState :: term(), timeout() | hibernate} |
  {noreply, NewState :: term()} |
  {noreply, NewState :: term(), timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
  {stop, Reason :: term(), NewState :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Extension to default node manager callback. Matched as last.
%% @end
%%--------------------------------------------------------------------
-callback handle_cast_extension(Request :: term(), State :: term()) ->
  {noreply, NewState :: term()} |
  {noreply, NewState :: term(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Extension to default node manager callback. Matched as last.
%% @end
%%--------------------------------------------------------------------
-callback handle_info_extension(Info :: timeout | term(), State :: term()) ->
  {noreply, NewState :: term()} |
  {noreply, NewState :: term(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Callback executed on node manager terminate callback execution.
%% Invoked after default callback actions. Invoked with original
%% arguments. Returns final result of termination.
%% @end
%%--------------------------------------------------------------------
-callback on_terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), State :: term()) ->
  term().

%%--------------------------------------------------------------------
%% @doc
%% Callback executed on node manager code_change callback execution.
%% Invoked before default callback actions.
%% @end
%%--------------------------------------------------------------------
-callback on_code_change(OldVsn :: (term() | {down, term()}), State :: term(), Extra :: term()) ->
  {ok, NewState :: term()} | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% List of modules to be loaded. Consistient with modules_with_args
%% as in the original implementation.
%% @end
%%--------------------------------------------------------------------
-callback modules() -> Models :: [atom()].

%%--------------------------------------------------------------------
%% @doc
%% List of listeners to be loaded by node_manager.
%% @end
%%--------------------------------------------------------------------
-callback listeners() -> Listeners :: [atom()].

%%--------------------------------------------------------------------
%% @doc
%% List of modules (accompanied by their configs) to be loaded by node_manager.
%% @end
%%--------------------------------------------------------------------
-callback modules_with_args() -> Models :: [{atom(), [any()]}].

