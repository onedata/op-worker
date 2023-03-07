%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This behaviour specifies an API for transfer worker - a module that
%%% supervises/participates in transfer of data.
%%% It implements most of required generic functionality for such worker
%%% including walking down fs subtree or querying specified view and
%%% scheduling transfer of individual files received.
%%% But leaves implementation of, specific to given transfer type,
%%% functions like transfer of regular file to callback modules.
%%% @end
%%%--------------------------------------------------------------------
-module(gen_transfer_worker).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

% There are 2 different types of transfers jobs:
% - traverse - iterates over data source (view, directory or even single file)
%              listing regular files and scheduling their transfer jobs.
%              This is the first scheduled job for every transfer.
% - regular file transfer - transfers single regular file
-type job() :: #transfer_traverse_job{} | #transfer_regular_file_job{}.
-type job_ctx() :: #transfer_job_ctx{}.

-export_type([job/0, job_ctx/0]).

-record(state, {mod :: module()}).
-type state() :: #state{}.

-define(LIST_BATCH_SIZE, op_worker:get_env(
    file_transfer_list_batch_size, 1000
)).
-define(FILES_TO_PROCESS_THRESHOLD, 10 * ?LIST_BATCH_SIZE).
-define(MAX_RETRY_INTERVAL_SEC, op_worker:get_env(
    max_file_transfer_retry_interval_sec, 18000
)).

%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Callback called to get permissions required to check before starting transfer.
%% @end
%%--------------------------------------------------------------------
-callback required_permissions() -> [data_access_control:requirement()].


%%--------------------------------------------------------------------
%% @doc
%% Callback called to get maximum number of transfer retries attempts.
%% @end
%%--------------------------------------------------------------------
-callback max_transfer_retries() -> non_neg_integer().


%%--------------------------------------------------------------------
%% @doc
%% Callback called to enqueue transfer of specified file with given params,
%% retries and next retry timestamp.
%% @end
%%--------------------------------------------------------------------
-callback enqueue_data_transfer(
    file_ctx:ctx(),
    job_ctx(),
    RetriesLeft :: undefined | non_neg_integer(),
    NextRetryTimestamp :: undefined | non_neg_integer()
) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when transferring regular file.
%% @end
%%--------------------------------------------------------------------
-callback transfer_regular_file(file_ctx:ctx(), job_ctx()) ->
    ok | {error, term()}.


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
init([CallbackModule]) ->
    {ok, #state{mod = CallbackModule}}.


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
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
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
handle_cast(?TRANSFER_DATA_REQ(FileCtx, TransferJobCtx, Retries, NextRetryTimestamp), State) ->
    Mod = State#state.mod,
    TransferId = TransferJobCtx#transfer_job_ctx.transfer_id,
    case should_start(NextRetryTimestamp) of
        true ->
            case transfer_data(State, FileCtx, TransferJobCtx, Retries) of
                ok ->
                    ok;
                {retry, NewFileCtx} ->
                    NextRetry = next_retry(State, Retries),
                    Mod:enqueue_data_transfer(NewFileCtx, TransferJobCtx, Retries - 1, NextRetry);
                {error, not_found} ->
                    % todo VFS-4218 currently we ignore this case
                    mark_traverse_finished_in_case_of_traverse_job(State#state.mod, TransferJobCtx),
                    {ok, _} = transfer:increment_files_processed_counter(TransferId);
                {error, cancelled} ->
                    mark_traverse_finished_in_case_of_traverse_job(State#state.mod, TransferJobCtx),
                    {ok, _} = transfer:increment_files_processed_counter(TransferId);
                {error, already_ended} ->
                    mark_traverse_finished_in_case_of_traverse_job(State#state.mod, TransferJobCtx),
                    {ok, _} = transfer:increment_files_processed_counter(TransferId);
                {error, _Reason} ->
                    mark_traverse_finished_in_case_of_traverse_job(State#state.mod, TransferJobCtx),
                    {ok, _} = transfer:increment_files_failed_and_processed_counters(TransferId)
            end;
        _ ->
            Mod:enqueue_data_transfer(FileCtx, TransferJobCtx, Retries, NextRetryTimestamp)
    end,
    {noreply, State, hibernate};

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


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
handle_info(Info, State) ->
    ?log_bad_request(Info),
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
%% This functions check whether replication can be started.
%% if NextRetryTimestamp is:
%%  * undefined - replication can be started
%%  * greater than current timestamp - replication cannot be started
%%  * otherwise replication can be started
%% @end
%%-------------------------------------------------------------------
-spec should_start(undefined | time:seconds()) -> boolean().
should_start(undefined) ->
    true;
should_start(NextRetryTimestamp) ->
    global_clock:timestamp_seconds() >= NextRetryTimestamp.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Applies permissions check and if passed transfers data.
%% In case of error checks if transfer should be retried and if so returns
%% retry request and caught error otherwise.
%% @end
%%-------------------------------------------------------------------
-spec transfer_data(state(), file_ctx:ctx(), job_ctx(), non_neg_integer()) ->
    ok | {retry, file_ctx:ctx()} | {error, term()}.
transfer_data(State = #state{mod = Mod}, FileCtx0, TransferJobCtx, RetriesLeft) ->
    UserCtx = TransferJobCtx#transfer_job_ctx.user_ctx,
    AccessDefinitions = Mod:required_permissions(),
    TransferId = TransferJobCtx#transfer_job_ctx.transfer_id,

    try
        {ok, #document{value = Transfer}} = transfer:get(TransferId),

        assert_transfer_is_ongoing(Transfer),

        FileCtx1 = fslogic_authz:ensure_authorized(UserCtx, FileCtx0, AccessDefinitions),
        transfer_data_insecure(UserCtx, FileCtx1, State, Transfer, TransferJobCtx)
    of
        ok ->
            ok;
        Error = {error, _Reason} ->
            maybe_retry(FileCtx0, TransferJobCtx, RetriesLeft, Error)
    catch
        throw:cancelled ->
            {error, cancelled};
        throw:already_ended ->
            {error, already_ended};
        error:{badmatch, Error = {error, not_found}} ->
            maybe_retry(FileCtx0, TransferJobCtx, RetriesLeft, Error);
        Class:Reason:Stacktrace ->
            ?error_exception(
                "Unexpected error during transfer ~p", [TransferJobCtx#transfer_job_ctx.transfer_id],
                Class, Reason, Stacktrace
            ),
            maybe_retry(FileCtx0, TransferJobCtx, RetriesLeft, {Class, Reason})
    end.


%% @private
-spec assert_transfer_is_ongoing(transfer:transfer()) -> ok | no_return().
assert_transfer_is_ongoing(Transfer) ->
    case transfer:is_ongoing(Transfer) of
        true -> ok;
        false -> throw(already_ended)
    end.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether data transfer can be retried and if so returns retry request.
%% Otherwise returns given error.
%% @end
%%-------------------------------------------------------------------
-spec maybe_retry(file_ctx:ctx(), job_ctx(), non_neg_integer(), term()) ->
    {retry, file_ctx:ctx()} | {error, term()}.
maybe_retry(_FileCtx, TransferJobCtx, 0, Error = {error, not_found}) ->
    ?error(
        "Data transfer in scope of transfer ~p failed due to ~w~n"
        "No retries left", [TransferJobCtx#transfer_job_ctx.transfer_id, Error]
    ),
    Error;
maybe_retry(FileCtx, TransferJobCtx, Retries, Error = {error, not_found}) ->
    ?warning(
        "Data transfer in scope of transfer ~p failed due to ~w~n"
        "File transfer will be retried (attempts left: ~p)",
        [TransferJobCtx#transfer_job_ctx.transfer_id, Error, Retries - 1]
    ),
    {retry, FileCtx};
maybe_retry(FileCtx, TransferJobCtx, 0, Error) ->
    TransferId = TransferJobCtx#transfer_job_ctx.transfer_id,
    {Path, _FileCtx2} = file_ctx:get_canonical_path(FileCtx),

    ?error(
        "Transfer of file ~p in scope of transfer ~p failed~n"
        "FilePath: ~ts~n"
        "Error was: ~p~n"
        "No retries left", [
            file_ctx:get_logical_guid_const(FileCtx), TransferId,
            Path,
            Error
        ]
    ),
    {error, retries_per_file_transfer_exceeded};
maybe_retry(FileCtx, TransferJobCtx, Retries, Error) ->
    TransferId = TransferJobCtx#transfer_job_ctx.transfer_id,
    {Path, FileCtx2} = file_ctx:get_canonical_path(FileCtx),

    ?warning(
        "Transfer of file ~p in scope of transfer ~p failed~n"
        "FilePath: ~ts~n"
        "Error was: ~p~n"
        "File transfer will be retried (attempts left: ~p)", [
            file_ctx:get_logical_guid_const(FileCtx), TransferId,
            Path,
            Error,
            Retries - 1
        ]
    ),
    {retry, FileCtx2}.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function returns minimal timestamp for next retry.
%% Interval is generated basing on exponential backoff algorithm.
%% @end
%%-------------------------------------------------------------------
-spec next_retry(state(), non_neg_integer()) -> non_neg_integer().
next_retry(#state{mod = Mod}, RetriesLeft) ->
    MaxRetries = Mod:max_transfer_retries(),
    MinSecsToWait = backoff(MaxRetries - RetriesLeft, MaxRetries),
    global_clock:timestamp_seconds() + MinSecsToWait.


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
    min(rand:uniform(round(math:pow(2, min(RetryNum, MaxRetries)))), ?MAX_RETRY_INTERVAL_SEC).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Transfers data from appropriate source, that is either files returned by
%% querying specified view or file system subtree (regular file or
%% entire directory depending on file ctx).
%% @end
%%--------------------------------------------------------------------
-spec transfer_data_insecure(
    user_ctx:ctx(),
    file_ctx:ctx(),
    state(),
    transfer:transfer(),
    job_ctx()
) ->
    ok | {error, term()}.
transfer_data_insecure(_, FileCtx, State, _Transfer, TransferJobCtx = #transfer_job_ctx{
    job = #transfer_regular_file_job{}
}) ->
    case file_ctx:file_exists_const(FileCtx) of
        true ->
            CallbackModule = State#state.mod,
            CallbackModule:transfer_regular_file(FileCtx, TransferJobCtx);
        false ->
            {error, not_found}
    end;

transfer_data_insecure(UserCtx, RootFileCtx, State, #transfer{
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed
}, TransferJobCtx) ->
    case FilesToProcess - FilesProcessed > ?FILES_TO_PROCESS_THRESHOLD of
        true ->
            ?warning(
                "Postponing next regular file transfer jobs scheduling for transfer ~s "
                "as jobs threshold have been reached (possible large number of job retries)",
                [TransferJobCtx#transfer_job_ctx.transfer_id]
            ),
            CallbackModule = State#state.mod,
            CallbackModule:enqueue_data_transfer(RootFileCtx, TransferJobCtx);
        false ->
            process_traverse_job(UserCtx, RootFileCtx, State, TransferJobCtx)
    end.


%% @private
-spec process_traverse_job(user_ctx:ctx(), file_ctx:ctx(), state(), job_ctx()) ->
    ok | {error, term()}.
process_traverse_job(UserCtx, RootFileCtx, State, TransferJobCtx = #transfer_job_ctx{
    transfer_id = TransferId,
    job = TransferTraverseJob = #transfer_traverse_job{iterator = Iterator}
}) ->
    CallbackModule = State#state.mod,

    {ProgressMarker, Results, NewIterator} = transfer_iterator:get_next_batch(
        UserCtx, ?LIST_BATCH_SIZE, Iterator
    ),

    transfer:increment_files_to_process_counter(TransferId, length(Results)),
    TransferRegularFileJobCtx = TransferJobCtx#transfer_job_ctx{job = #transfer_regular_file_job{}},
    lists:foreach(fun
        (error) ->
            transfer:increment_files_failed_and_processed_counters(TransferId);
        ({ok, FileCtx}) ->
            CallbackModule:enqueue_data_transfer(FileCtx, TransferRegularFileJobCtx)
    end, Results),

    case ProgressMarker of
        done when CallbackModule =:= replication_worker ->
            transfer:mark_replication_traverse_finished(TransferId),
            ok;
        done when CallbackModule =:= replica_eviction_worker ->
            transfer:mark_eviction_traverse_finished(TransferId),
            ok;
        more ->
            CallbackModule:enqueue_data_transfer(RootFileCtx, TransferJobCtx#transfer_job_ctx{
                job = TransferTraverseJob#transfer_traverse_job{iterator = NewIterator}
            })
    end.


%% @private
-spec mark_traverse_finished_in_case_of_traverse_job(module(), job_ctx()) -> ok.
mark_traverse_finished_in_case_of_traverse_job(replication_worker, #transfer_job_ctx{
    transfer_id = TransferId,
    job = #transfer_traverse_job{}
}) ->
    transfer:mark_replication_traverse_finished(TransferId),
    ok;

mark_traverse_finished_in_case_of_traverse_job(replica_eviction_worker, #transfer_job_ctx{
    transfer_id = TransferId,
    job = #transfer_traverse_job{}
}) ->
    transfer:mark_eviction_traverse_finished(TransferId),
    ok;

mark_traverse_finished_in_case_of_traverse_job(_, _) ->
    ok.
