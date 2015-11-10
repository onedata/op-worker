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

-include("global_definitions.hrl").
-include("cluster/worker/elements/task_manager/task_manager.hrl").
-include("cluster/worker/modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").

-type task() :: fun(() -> term()) | {fun((list()) -> term()), Args :: list()}
| {M :: atom(), F :: atom, Args :: list()} | atom(). % atom() for tests
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

%%--------------------------------------------------------------------
%% @doc
%% Starts task.
%% @end
%%--------------------------------------------------------------------
-spec start_task(Task :: task() | #document{value :: #task_pool{}}, Level :: level()) -> ok.
start_task(Task, Level) ->
    start_task(Task, Level, save_pid).

%%--------------------------------------------------------------------
%% @doc
%% Starts task.
%% @end
%%--------------------------------------------------------------------
-spec start_task(Task :: task() | #document{value :: #task_pool{}}, Level :: level(), PersistFun :: save_pid | update_pid) -> ok.
start_task(Task, Level, PersistFun) ->
    Pid = spawn(fun() ->
        receive
            {start, Uuid} ->
                case do_task(Task, ?TASK_REPEATS) of
                    ok ->
                        ok = delete_task(Uuid, Task, Level);
                    _ ->
                        ?error_stacktrace("~p fails of a task ~p", [?TASK_REPEATS, Task])
                end
        after
            ?TASK_SAVE_TIMEOUT ->
                ?error_stacktrace("Timeout for task ~p", [Task]),
                timeout
        end
    end),
    {ok, Uuid} = apply(?MODULE, PersistFun, [Task, Pid, Level]),
    Pid ! {start, Uuid},
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Checks all tasks and reruns failed.
%% @end
%%--------------------------------------------------------------------
-spec check_and_rerun_all() -> ok.
check_and_rerun_all() ->
    check_and_rerun_all(?NODE_LEVEL),
    check_and_rerun_all(?CLUSTER_LEVEL).
% TODO - list at persistent driver needed
%%     check_and_rerun_all(?PERSISTENT_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% Saves information about the task.
%% @end
%%--------------------------------------------------------------------
-spec save_pid(Task :: task(), Pid :: pid(), Level :: level()) ->
    {ok, datastore:key()} | datastore:create_error().
save_pid(Task, Pid, Level) ->
    task_pool:create(Level, #document{value = #task_pool{task = Task, owner = Pid, node = node()}}).

%%--------------------------------------------------------------------
%% @doc
%% Updates information about the task.
%% @end
%%--------------------------------------------------------------------
-spec update_pid(Task :: #document{value :: #task_pool{}}, Pid :: pid(), Level :: level()) ->
    {ok, datastore:key()} | datastore:create_error().
update_pid(Task, Pid, Level) ->
    task_pool:update(Level, Task#document.key, #{owner => Pid}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes information about the task.
%% @end
%%--------------------------------------------------------------------
-spec delete_task(Uuid :: datastore:key(), Task :: task() | #document{value :: #task_pool{}},
    Level :: level()) -> ok.
delete_task(Uuid, Task, Level) ->
    case task_pool:delete(Level, Uuid) of
        ok ->
            ok;
        E ->
            ?error_stacktrace("Error ~p while deleting task ~p", [E, Task]),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes task.
%% @end
%%--------------------------------------------------------------------
-spec do_task(Task :: task()) -> term().
do_task(Fun) when is_function(Fun) ->
    Fun();

do_task({Fun, Args}) when is_function(Fun) ->
    Fun(Args);

do_task({M, F, Args}) ->
    apply(M, F, Args);

do_task(Task) ->
    ?error_stacktrace("Not a task ~p", [Task]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Executes task.
%% @end
%%--------------------------------------------------------------------
-spec do_task(Task :: task(), Repeats :: integer()) -> term().
do_task(Task, Num) when is_record(Task, document) ->
    V = Task#document.value,
    do_task(V#task_pool.task, Num);

do_task(_Task, 0) ->
    task_failed;

do_task(Task, Num) ->
    try
        ok = do_task(Task)
    catch
        E1:E2 ->
            ?error_stacktrace("Task ~p error: ~p:~p", [Task, E1, E2]),
            {ok, Interval} = application:get_env(?APP_NAME, task_fail_sleep_time_ms),
            timer:sleep(Interval),
            do_task(Task, Num - 1)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks tasks and reruns failed.
%% @end
%%--------------------------------------------------------------------
-spec check_and_rerun_all(Level :: level()) -> ok.
check_and_rerun_all(Level) ->
    {ok, Tasks} = task_pool:list_failed(Level),
    lists:foreach(fun(Task) ->
        start_task(Task, Level, update_pid)
    end, Tasks).