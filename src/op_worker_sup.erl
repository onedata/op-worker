%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc It is the main supervisor. It starts (as it child) node manager
%%% which initializes node.
%%% @end
%%%--------------------------------------------------------------------
-module(op_worker_sup).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    ensure_audit_log_dirs(),

    {ok, {#{strategy => one_for_one, intensity => 10, period => 3600}, []}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_audit_log_dirs() -> ok.
ensure_audit_log_dirs() ->
    lists:foreach(fun(LogRootDir) ->
        ok = filelib:ensure_dir(op_worker:get_env(LogRootDir))
    end, [
        storage_import_audit_log_root_dir,
        dbsync_changes_audit_log_root_dir,
        dbsync_out_stream_audit_log_root_dir
    ]).
