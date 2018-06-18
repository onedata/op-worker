%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(invalidation_worker).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, process_eviction_result/3]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(MAX_FILE_INVALIDATION_RETRIES,
    application:get_env(?APP_NAME, max_file_invalidation_retries_per_file, 5)).

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
%% Posthook executed by replica_eviction_worker after evicting file.
%% @end
%%-------------------------------------------------------------------
-spec process_eviction_result(replica_eviction:result(), file_meta:uuid(),
    transfer:id()) -> ok.
process_eviction_result({ok, ReleasedBytes}, FileUuid, TransferId) ->
    ?debug("Invalidation of file ~p in transfer ~p released ~p bytes.",
        [FileUuid, TransferId, ReleasedBytes]),
    {ok, _} = transfer:increase_files_invalidated_and_processed_counter(TransferId),
    ok;
process_eviction_result(Error, FileUuid, TransferId) ->
    ?error("Error ~p occured during invalidation of file ~p in procedure ~p",
        [Error, FileUuid, TransferId]),
    {ok, _} = transfer:increase_files_processed_counter(TransferId),
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
handle_cast({start_file_invalidation, UserCtx, FileCtx,  SupportingProviderId,
    TransferId, RetriesLeft, NextRetryTimestamp}, State
) ->
    RetriesLeft2 = utils:ensure_defined(RetriesLeft, undefined,
        ?MAX_FILE_INVALIDATION_RETRIES),
    case should_start(NextRetryTimestamp) of
        true ->
            case invalidate_file(UserCtx, FileCtx, SupportingProviderId, 
                TransferId, RetriesLeft2) 
            of
                ok ->
                    ok;
                {error, not_found} ->
                    % todo VFS-4218 currently we ignore this case
                    {ok, _} = transfer:increase_files_processed_counter(TransferId);
                {error, _Reason} ->
                    {ok, _} = transfer:mark_failed_file_processing(TransferId)
            end;
        _ ->
            invalidation_req:enqueue_file_invalidation(UserCtx, FileCtx, SupportingProviderId,
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
%% Replicates files
%% @end
%%-------------------------------------------------------------------
-spec invalidate_file(user:ctx(), file_ctx:ctx(), sync_req:provider_id(),
    sync_req:transfer_id(), non_neg_integer()) -> ok | {error, term()}.
invalidate_file(UserCtx, FileCtx, SupportingProviderId, TransferId, RetriesLeft) ->
    try invalidation_req:invalidate_file_replica(UserCtx, FileCtx,
        SupportingProviderId, TransferId)
    of
        #provider_response{status = #status{code = ?OK}}  ->
            ok;
        Error = {error, not_found} ->
            maybe_retry(UserCtx, FileCtx, SupportingProviderId, TransferId,
                RetriesLeft, Error);
        Error = {error, invalidation_timeout} ->
            maybe_retry(UserCtx, FileCtx, SupportingProviderId, TransferId,
                RetriesLeft, Error)
    catch
        throw:{transfer_cancelled, TransferId} ->
            {error, transfer_cancelled};
        Error:Reason ->
            maybe_retry(UserCtx, FileCtx, SupportingProviderId, TransferId,
                RetriesLeft,
                {Error, Reason})
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether file invalidation can be retried and repeats
%% invalidation if it's possible.
%% @end
%%-------------------------------------------------------------------
-spec maybe_retry(user:ctx(), file_ctx:ctx(), sync_req:provider_id(),
    sync_req:transfer_id(), non_neg_integer(), term()) -> ok | {error, term()}.
maybe_retry(_UserCtx, _FileCtx, _SupportingProviderId, TransferId, 0, Error = {error, not_found}) ->
    ?error(
        "Invalidation in scope of transfer ~p failed due to ~p~n"
        "No retries left", [TransferId, Error]),
    Error;
maybe_retry(UserCtx, FileCtx, SupportingProviderId, TransferId, RetriesLeft,
    Error = {error, not_found}
) ->
    ?warning_stacktrace(
        "Invalidation in scope of transfer ~p failed due to ~p~n"
        "File Invalidation will be retried (attempts left: ~p)",
        [TransferId, Error, RetriesLeft - 1]),
    invalidation_req:enqueue_file_invalidation(UserCtx, FileCtx, SupportingProviderId, TransferId,
        RetriesLeft - 1, next_retry(RetriesLeft));
maybe_retry(_UserCtx, FileCtx, _SupportingProviderId, TransferId, 0, Error) ->
    {Path, _FileCtx2} = file_ctx:get_canonical_path(FileCtx),
    ?error(
        "Invalidation of file ~p in scope of transfer ~p failed due to ~p~n"
        "No retries left", [Path, TransferId, Error]),
    {error, retries_per_file_transfer_exceeded};
maybe_retry(UserCtx, FileCtx, SupportingProviderId, TransferId, Retries, Error) ->
    {Path, FileCtx2} = file_ctx:get_canonical_path(FileCtx),
    ?warning_stacktrace(
        "Invalidation of file ~p in scope of transfer ~p failed due to ~p~n"
        "File Invalidation will be retried (attempts left: ~p)",
        [Path, TransferId, Error, Retries - 1]),
    invalidation_req:enqueue_file_invalidation(UserCtx, FileCtx2, SupportingProviderId, TransferId,
        Retries - 1, next_retry(Retries)).


%%TODO move below functions to utils module, because they're duplicated in transfer_worker !!!

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This functions check whether invalidation can be started.
%% if NextRetryTimestamp is:
%%  * undefined - invalidation can be started
%%  * greater than current timestamp - invalidation cannot be started
%%  * otherwise invalidation can be started
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
    RetryNum = ?MAX_FILE_INVALIDATION_RETRIES - RetriesLeft,
    MinSecsToWait = backoff(RetryNum, ?MAX_FILE_INVALIDATION_RETRIES),
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
