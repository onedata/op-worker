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
-include_lib("ctool/include/global_definitions.hrl").

-type task() :: fun(() -> term()) | {fun((list()) -> term()), Args :: list()} | {M :: atom(), F :: atom, Args :: list()}.
-type level() :: ?NON_LEVEL | ?NODE_LEVEL | ?CLUSTER_LEVEL | ?PERSISTENT_LEVEL.
-export_type([task/0, level/0]).

%% API
-export([start_task/2, start_task/3, check_and_rerun_all/0]).
-export([save_pid/3, update_pid/3]).

-define(TASK_SAVE_TIMEOUT, timer:seconds(10)).
-define(TASK_REPEATS, 20).

%%%===================================================================
%%% API
%%%===================================================================

start_task(Task, Level) ->
  start_task(Task, Level, save_pid).

start_task(Task, Level, PersistFun) ->
  Pid = spawn(fun() ->
    receive
      {start, Uuid} ->
        case do_task(Task, ?TASK_REPEATS) of
          ok ->
            delete_task(Uuid, Task, Level);
          _ ->
            ?error_stacktrace("~p fails of a task ~p", [?TASK_REPEATS, Task])
        end
    after
      ?TASK_SAVE_TIMEOUT ->
        ?error_stacktrace("Timeput for task ~p", [Task]),
        timeout
    end
  end),
  {ok, Uuid} = apply(?MODULE, PersistFun, [Task, Pid, Level]),
  Pid ! {start, Uuid}.

check_and_rerun_all() ->
  check_and_rerun_all(?NODE_LEVEL),
  check_and_rerun_all(?CLUSTER_LEVEL),
  check_and_rerun_all(?PERSISTENT_LEVEL).

save_pid(Task, Pid, Level) ->
  task_pool:create(Level, #task_pool{task = Task, owner = Pid}).

update_pid(Task, Pid, Level) ->
  task_pool:update(Level, Task#document.key, #{owner => Pid}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

delete_task(Uuid, Task, Level) ->
  case task_pool:delete(Level, Uuid) of
    ok ->
      ok;
    E ->
      ?error_stacktrace("Error ~p while deleting task ~p", [E, Task])
  end.

do_task(Fun) when is_function(Fun) ->
  Fun();

do_task({Fun, Args}) when is_function(Fun) ->
  Fun(Args);

do_task({M, F, Args}) ->
  apply(M, F, Args);

do_task(Task) ->
  ?error_stacktrace("Not a task ~p", [Task]),
  ok.

do_task(_Task, 0) ->
  task_failed;

do_task(Task, Num) ->
  try
    ok = do_task(Task)
  catch
    E1:E2 ->
      ?error_stacktrace("Task ~p error: ~p:~p", [Task, E1, E2]),
      do_task(Task, Num - 1)
  end.

check_and_rerun_all(?NODE_LEVEL) ->
  {ok, Tasks} = task_pool:list(?NODE_LEVEL),
  lists:foreach(fun(Task) ->
    start_task(Task, ?NODE_LEVEL, update_pid)
  end, Tasks);

check_and_rerun_all(Level) ->
  {ok, Tasks} = task_pool:list(Level),
  {ok, Nodes} = gen_server:call({global, ?CCM}, get_nodes),
  lists:foreach(fun({Task, Node}) ->
    rpc:call(Node, ?MODULE, start_task, [Task, Level, update_pid])
  end, zip(Tasks, Nodes)).

zip([], _L12, [], _L22) ->
  [];
zip([], L12, L12, L22) ->
  zip(L12, [], L12, L22);
zip(L11, L12, [], L22) ->
  zip(L11,L12, L22, []);
zip([H1 | L11], L12, [H2 | L21], L22) ->
  [{H1, H2} | zip(L11, [H1 | L12], L21, [H2 | L22])].