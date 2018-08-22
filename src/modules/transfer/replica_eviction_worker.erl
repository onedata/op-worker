%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Implementation of worker for replica_eviction_workers_pool.
%%% Worker is responsible for replica eviction of one file
%%% (regular or directory).
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_worker).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, process_replica_deletion_result/3]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(MAX_REPLICA_EVICTION_RETRIES,
    application:get_env(?APP_NAME, max_eviction_retries_per_file_replica, 5)).

-record(state, {}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

%%-------------------------------------------------------------------
%% @doc
%% Posthook executed by replica_deletion_worker after deleting file
%% replica.
%% @end
%%-------------------------------------------------------------------
-spec process_replica_deletion_result(replica_deletion:result(), file_meta:uuid(),
    transfer:id()) -> ok.
process_replica_deletion_result({ok, ReleasedBytes}, FileUuid, TransferId) ->
    ?debug("Replica eviction of file ~p in transfer ~p released ~p bytes.",
        [FileUuid, TransferId, ReleasedBytes]),
    {ok, _} = transfer:increment_files_evicted_and_processed_counters(TransferId),
    ok;
process_replica_deletion_result(Error, FileUuid, TransferId) ->
    ?error("Error ~p occured during replica eviction of file ~p in procedure ~p",
        [Error, FileUuid, TransferId]),
    {ok, _} = transfer:increment_files_processed_counter(TransferId),
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}).
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}).
handle_cast({start_replica_eviction, UserCtx, FileCtx,  SupportingProviderId,
    TransferId, RetriesLeft, NextRetryTimestamp}, State
) ->
    RetriesLeft2 = utils:ensure_defined(RetriesLeft, undefined,
        ?MAX_REPLICA_EVICTION_RETRIES),
    case should_start(NextRetryTimestamp) of
        true ->
            case evict_replica(UserCtx, FileCtx, SupportingProviderId,
                TransferId, RetriesLeft2) 
            of
                ok ->
                    ok;
                {error, not_found} ->
                    % todo VFS-4218 currently we ignore this case
                    {ok, _} = transfer:increment_files_processed_counter(TransferId);
                {error, _Reason} ->
                    {ok, _} = transfer:increment_files_failed_and_processed_counters(TransferId)
            end;
        _ ->
            replica_eviction_req:enqueue_replica_eviction(UserCtx, FileCtx, SupportingProviderId,
                TransferId, RetriesLeft2, NextRetryTimestamp)
    end,
    {noreply, State, hibernate}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}).
handle_info(_Info, State) ->
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
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Evicts file replica
%% @end
%%-------------------------------------------------------------------
-spec evict_replica(user:ctx(), file_ctx:ctx(), sync_req:provider_id(),
    sync_req:transfer_id(), non_neg_integer()) -> ok | {error, term()}.
evict_replica(UserCtx, FileCtx, SupportingProviderId, TransferId, RetriesLeft) ->
    try replica_eviction_req:evict_file_replica(UserCtx, FileCtx,
        SupportingProviderId, TransferId)
    of
        #provider_response{status = #status{code = ?OK}}  ->
            ok;
        Error = {error, not_found} ->
            maybe_retry(UserCtx, FileCtx, SupportingProviderId, TransferId,
                RetriesLeft, Error)
    catch
        Error:Reason ->
            maybe_retry(UserCtx, FileCtx, SupportingProviderId, TransferId,
                RetriesLeft,
                {Error, Reason})
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether file replica eviction can be retried and repeats
%% replica eviction if it's possible.
%% @end
%%-------------------------------------------------------------------
-spec maybe_retry(user:ctx(), file_ctx:ctx(), sync_req:provider_id(),
    sync_req:transfer_id(), non_neg_integer(), term()) -> ok | {error, term()}.
maybe_retry(_UserCtx, _FileCtx, _SupportingProviderId, TransferId, 0, Error = {error, not_found}) ->
    ?error(
        "Replica eviction in scope of transfer ~p failed due to ~p~n"
        "No retries left", [TransferId, Error]),
    Error;
maybe_retry(UserCtx, FileCtx, SupportingProviderId, TransferId, RetriesLeft,
    Error = {error, not_found}
) ->
    ?warning_stacktrace(
        "Replica eviction in scope of transfer ~p failed due to ~p~n"
        "File replica eviction will be retried (attempts left: ~p)",
        [TransferId, Error, RetriesLeft - 1]),
    replica_eviction_req:enqueue_replica_eviction(UserCtx, FileCtx, SupportingProviderId, TransferId,
        RetriesLeft - 1, next_retry(RetriesLeft));
maybe_retry(_UserCtx, FileCtx, _SupportingProviderId, TransferId, 0, Error) ->
    {Path, _FileCtx2} = file_ctx:get_canonical_path(FileCtx),
    ?error(
        "Replica eviction of file ~p in scope of transfer ~p failed due to ~p~n"
        "No retries left", [Path, TransferId, Error]),
    {error, retries_per_file_transfer_exceeded};
maybe_retry(UserCtx, FileCtx, SupportingProviderId, TransferId, Retries, Error) ->
    {Path, FileCtx2} = file_ctx:get_canonical_path(FileCtx),
    ?warning_stacktrace(
        "Replica eviction of file ~p in scope of transfer ~p failed due to ~p~n"
        "File Replica eviction will be retried (attempts left: ~p)",
        [Path, TransferId, Error, Retries - 1]),
    replica_eviction_req:enqueue_replica_eviction(UserCtx, FileCtx2, SupportingProviderId, TransferId,
        Retries - 1, next_retry(Retries)).


%%TODO VFS-4624 move below functions to utils module, because they're duplicated in transfer_worker !

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This functions check whether eviction can be started.
%% if NextRetryTimestamp is:
%%  * undefined - replica eviction can be started
%%  * greater than current timestamp - replica eviction cannot be started
%%  * otherwise replica eviction can be started
%% @end
%%-------------------------------------------------------------------
-spec should_start(undefined | non_neg_integer()) -> boolean().
should_start(undefined) ->
    true;
should_start(NextRetryTimestamp) ->
    time_utils:cluster_time_seconds() >= NextRetryTimestamp.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function returns minimal timestamp for next retry.
%% Interval is generated basing on exponential backoff algorithm.
%% @end
%%-------------------------------------------------------------------
-spec next_retry(non_neg_integer()) -> non_neg_integer().
next_retry(RetriesLeft) ->
    RetryNum = ?MAX_REPLICA_EVICTION_RETRIES - RetriesLeft,
    MinSecsToWait = backoff(RetryNum, ?MAX_REPLICA_EVICTION_RETRIES),
    time_utils:cluster_time_seconds() + MinSecsToWait.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Exponential backoff for transfer retries.
%% Returns random number from range [1, 2^(min(RetryNum, MaxRetries)]
%% where RetryNum is number of retry
%% @end
%%-------------------------------------------------------------------
-spec backoff(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
backoff(RetryNum, MaxRetries) ->
    rand:uniform(round(math:pow(2, min(RetryNum, MaxRetries)))).
