%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module coordinates tasks that needs special supervision.
%%% @end
%%% TODO - atomic update at persistent driver needed
%%%-------------------------------------------------------------------
-module(task_manager).
-author("Michal Wrzeszcz").

-include("cluster_elements/node_manager/task_manager.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-type task() :: fun(() -> term()) | {fun((list()) -> term()), Args :: list()} | {M :: atom(), F :: atom, Args :: list()}.
-type level() :: ?NON_LEVEL | ?NODE_LEVEL | ?CLUSTER_LEVEL | ?PERSISTENT_LEVEL.
-export_type([task/0, level/0]).

%% API
-export([]).

-define(TASK_SAVE_TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% API
%%%===================================================================

% zapisac klucz zeby potem go usunac
start_task(Task, Level) ->
  Pid = spawn(fun() ->
    receive
      start ->
        start_task(Task)
    after
      ?TASK_SAVE_TIMEOUT ->
        timeout
    end
  end),
  {ok, _} = save_pid(Task, Pid, Level),
  Pid ! start.

task_ended(Task) ->
  ok.

check_and_rerun_all() ->
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

save_pid(Task, Pid, Level) ->
  task_pool:create(Level, #task_pool{task = Task, owner = Pid}).

start_task(Fun) when is_function(Fun) ->
  Fun();

start_task({Fun, Args}) when is_function(Fun) ->
  Fun(Args);

start_task({M, F, Args}) ->
  apply(M, F, Args);

start_task(Task) ->
  ?error_stacktrace("Not a task ~p", [Task]),
  not_a_task.