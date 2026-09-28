%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Deletes the entries storages left behind in their stale LUMA DB namespaces -
%%% see luma_db for what a namespace is and when one goes stale.
%%%
%%% A stale namespace backs no lookup, so nothing here is needed for
%%% correctness; it reclaims space. That is precisely why the work is done here
%%% and not on the storage update path, where it used to hold the storage's
%%% critical section for as long as it took to walk five tables, and where a
%%% single failure meant the entries were never revisited. The namespaces to
%%% clean are read off the storage config rather than guessed, so a run that
%%% fails simply leaves them there and the next one retries.
%%%
%%% Runs as an internal service, i.e. on one node at a time, with another node
%%% taking over on failure. Running it on every node would have each of them
%%% sweeping the same forests in parallel.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_db_garbage_collector).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([setup_internal_service/0, terminate_internal_service/0]).
-export([run/0]).

%% Internal Service callbacks
-export([start_service/0, stop_service/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-type state() :: undefined.


-define(SERVICE_NAME, <<"luma-db-garbage-collector-service">>).
-define(SERVER, {global, ?MODULE}).

-define(RUN_INTERVAL_SECONDS, op_worker:get_env(
    luma_db_garbage_collector_run_interval_sec, 3600  %% 1 hour
)).


%%%===================================================================
%%% API
%%%===================================================================


-spec setup_internal_service() -> ok.
setup_internal_service() ->
    ok = internal_services_manager:start_service(?MODULE, ?SERVICE_NAME, ?SERVICE_NAME, #{
        start_function => start_service,
        stop_function => stop_service
    }).


-spec terminate_internal_service() -> ok.
terminate_internal_service() ->
    case node() =:= internal_services_manager:get_processing_node(?SERVICE_NAME) of
        true ->
            try
                ok = internal_services_manager:stop_service(?MODULE, ?SERVICE_NAME, ?SERVICE_NAME)
            catch Class:Reason:Stacktrace ->
                ?error_exception(Class, Reason, Stacktrace)
            end;
        false ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Runs a collection immediately and waits for it to finish, rather than waiting
%% for the next tick. Intended for tests and manual intervention.
%% @end
%%--------------------------------------------------------------------
-spec run() -> ok.
run() ->
    gen_server:call(?SERVER, collect_garbage, infinity).


%%%===================================================================
%%% Internal services API
%%%===================================================================


-spec start_service() -> ok | abort.
start_service() ->
    ChildSpec = #{
        id => ?MODULE,
        start => {gen_server, start_link, [?SERVER, ?MODULE, [], []]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [?MODULE]
    },
    case catch supervisor:start_child(?FSLOGIC_WORKER_SUP, ChildSpec) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        Error ->
            ?critical("Failed to start ~tp due to: ~tp", [?MODULE, Error]),
            abort
    end.


-spec stop_service() -> ok.
stop_service() ->
    ok = supervisor:terminate_child(?FSLOGIC_WORKER_SUP, ?MODULE),
    ok = supervisor:delete_child(?FSLOGIC_WORKER_SUP, ?MODULE).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


-spec init(Args :: term()) -> {ok, state(), non_neg_integer()}.
init(_) ->
    process_flag(trap_exit, true),
    {ok, undefined, timer:seconds(?RUN_INTERVAL_SECONDS)}.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), non_neg_integer()}.
handle_call(collect_garbage, _From, State) ->
    collect_garbage(),
    {reply, ok, State, timer:seconds(?RUN_INTERVAL_SECONDS)};

handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, {error, wrong_request}, State}.


-spec handle_cast(Request :: term(), state()) -> {noreply, NewState :: state()}.
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_info(Info :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), non_neg_integer()}.
handle_info(timeout, State) ->
    collect_garbage(),
    {noreply, State, timer:seconds(?RUN_INTERVAL_SECONDS)};

handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.


-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), state()) -> term().
terminate(_Reason, _State) ->
    ok.


-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, NewState :: state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec collect_garbage() -> ok.
collect_garbage() ->
    ?debug("Starting stale LUMA DB namespaces collecting procedure..."),

    case storage_config:list_all() of
        {ok, StorageConfigDocs} ->
            lists:foreach(fun collect_storage_garbage/1, StorageConfigDocs),
            ?debug("Stale LUMA DB namespaces collecting procedure finished successfully.");
        {error, _} = Error ->
            ?warning(
                "Skipping stale LUMA DB namespaces collecting procedure due to: ~tp",
                [Error]
            )
    end.


%% @private
-spec collect_storage_garbage(storage_config:doc()) -> ok.
collect_storage_garbage(#document{value = #storage_config{stale_luma_db_namespaces = []}}) ->
    ok;

collect_storage_garbage(#document{
    key = StorageId,
    value = #storage_config{stale_luma_db_namespaces = StaleNamespaces}
} = StorageConfigDoc) ->
    ?info("LUMA DB gc: clearing ~B stale namespace(s) of storage '~ts'", [
        length(StaleNamespaces), StorageId
    ]),
    lists:foreach(fun(Namespace) ->
        clear_stale_namespace(StorageConfigDoc, StorageId, Namespace)
    end, StaleNamespaces).


%% @private
-spec clear_stale_namespace(storage_config:doc(), storage:id(), undefined | luma_db:namespace()) ->
    ok.
clear_stale_namespace(StorageConfigDoc, StorageId, Namespace) ->
    try
        luma_crud_api:clear_stale_db_namespace(StorageConfigDoc, Namespace),

        case storage_config:forget_stale_luma_db_namespace(StorageId, Namespace) of
            ok ->
                ok;
            {error, not_found} ->
                % the storage has been deleted in the meantime - nothing left to
                % keep the list on
                ok;
            {error, _} = Error ->
                throw(Error)
        end
    catch Class:Reason:Stacktrace ->
        % only a warning - the namespace stays on the list and the next run retries it
        ?warning(
            "LUMA DB gc: failed to clear stale namespace ~tp of storage '~ts', will retry",
            [Namespace, StorageId]
        ),
        ?examine_exception(Class, Reason, Stacktrace)
    end.
