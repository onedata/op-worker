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

-record(state, {mod :: module()}).
-type state() :: #state{}.

-define(LIST_BATCH_SIZE, 100).
-define(FILES_TO_PROCESS_THRESHOLD, 10 * ?LIST_BATCH_SIZE).

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
%% Callback called to get chunk size when querying db view.
%% @end
%%--------------------------------------------------------------------
-callback view_querying_chunk_size() -> non_neg_integer().


%%--------------------------------------------------------------------
%% @doc
%% Callback called to enqueue transfer of specified file with given params,
%% retries and next retry timestamp.
%% @end
%%--------------------------------------------------------------------
-callback enqueue_data_transfer(file_ctx:ctx(), transfer_params(),
    RetriesLeft :: undefined | non_neg_integer(),
    NextRetryTimestamp :: undefined | non_neg_integer()) -> ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when transferring regular file.
%% @end
%%--------------------------------------------------------------------
-callback transfer_regular_file(file_ctx:ctx(), transfer_params()) ->
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
handle_cast(?TRANSFER_DATA_REQ(FileCtx, Params, Retries, NextRetryTimestamp), State) ->
    Mod = State#state.mod,
    TransferId = Params#transfer_params.transfer_id,
    case should_start(NextRetryTimestamp) of
        true ->
            case transfer_data(State, FileCtx, Params, Retries) of
                ok ->
                    ok;
                {retry, NewFileCtx} ->
                    NextRetry = next_retry(State, Retries),
                    Mod:enqueue_data_transfer(NewFileCtx, Params, Retries - 1, NextRetry);
                {error, not_found} ->
                    % todo VFS-4218 currently we ignore this case
                    {ok, _} = transfer:increment_files_processed_counter(TransferId);
                {error, cancelled} ->
                    {ok, _} = transfer:increment_files_processed_counter(TransferId);
                {error, already_ended} ->
                    {ok, _} = transfer:increment_files_processed_counter(TransferId);
                {error, _Reason} ->
                    {ok, _} = transfer:increment_files_failed_and_processed_counters(TransferId)
            end;
        _ ->
            Mod:enqueue_data_transfer(FileCtx, Params, Retries, NextRetryTimestamp)
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
-spec transfer_data(state(), file_ctx:ctx(), transfer_params(), non_neg_integer()) ->
    ok | {retry, file_ctx:ctx()} | {error, term()}.
transfer_data(State = #state{mod = Mod}, FileCtx0, Params, RetriesLeft) ->
    UserCtx = Params#transfer_params.user_ctx,
    AccessDefinitions = Mod:required_permissions(),
    TransferId = Params#transfer_params.transfer_id,

    try
        {ok, #document{value = Transfer}} = transfer:get(TransferId),

        assert_transfer_is_ongoing(Transfer),

        FileCtx1 = fslogic_authz:ensure_authorized(UserCtx, FileCtx0, AccessDefinitions),
        transfer_data_insecure(UserCtx, FileCtx1, State, Transfer, Params)
    of
        ok ->
            ok;
        Error = {error, _Reason} ->
            maybe_retry(FileCtx0, Params, RetriesLeft, Error)
    catch
        throw:cancelled ->
            {error, cancelled};
        throw:already_ended ->
            {error, already_ended};
        error:{badmatch, Error = {error, not_found}} ->
            maybe_retry(FileCtx0, Params, RetriesLeft, Error);
        Error:Reason:Stacktrace   ->
            ?error_stacktrace("Unexpected error ~p:~p during transfer ~p", [
                Error, Reason, Params#transfer_params.transfer_id
            ], Stacktrace),
            maybe_retry(FileCtx0, Params, RetriesLeft, {Error, Reason})
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
-spec maybe_retry(file_ctx:ctx(), transfer_params(), non_neg_integer(), term()) ->
    {retry, file_ctx:ctx()} | {error, term()}.
maybe_retry(_FileCtx, Params, 0, Error = {error, not_found}) ->
    ?error(
        "Data transfer in scope of transfer ~p failed due to ~p~n"
        "No retries left", [Params#transfer_params.transfer_id, Error]
    ),
    Error;
maybe_retry(FileCtx, Params, Retries, Error = {error, not_found}) ->
    ?warning(
        "Data transfer in scope of transfer ~p failed due to ~p~n"
        "File transfer will be retried (attempts left: ~p)",
        [Params#transfer_params.transfer_id, Error, Retries - 1]
    ),
    {retry, FileCtx};
maybe_retry(FileCtx, Params, 0, Error) ->
    TransferId = Params#transfer_params.transfer_id,
    {Path, _FileCtx2} = file_ctx:get_canonical_path(FileCtx),

    ?error(
        "Transfer of file ~p in scope of transfer ~p failed due to ~p~n"
        "No retries left", [Path, TransferId, Error]
    ),
    {error, retries_per_file_transfer_exceeded};
maybe_retry(FileCtx, Params, Retries, Error) ->
    TransferId = Params#transfer_params.transfer_id,
    {Path, FileCtx2} = file_ctx:get_canonical_path(FileCtx),

    ?warning(
        "Transfer of file ~p in scope of transfer ~p failed due to ~p~n"
        "File transfer will be retried (attempts left: ~p)",
        [Path, TransferId, Error, Retries - 1]
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
    rand:uniform(round(math:pow(2, min(RetryNum, MaxRetries)))).


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
    transfer_params()
) ->
    ok | {error, term()}.
transfer_data_insecure(_, FileCtx, State, _Transfer, Params = #transfer_params{
    iterator = undefined
}) ->
    case file_ctx:file_exists_const(FileCtx) of
        true ->
            CallbackModule = State#state.mod,
            CallbackModule:transfer_regular_file(FileCtx, Params);
        false ->
            {error, not_found}
    end;

transfer_data_insecure(_UserCtx, RootFileCtx, State, #transfer{files_to_process = FTP}, Params) when
    FTP > ?FILES_TO_PROCESS_THRESHOLD
->
    % Postpone next file batch scheduling to ensure there are not enormous number
    % of files already scheduled to process
    CallbackModule = State#state.mod,
    CallbackModule:enqueue_data_transfer(RootFileCtx, Params);

transfer_data_insecure(UserCtx, RootFileCtx, State, _Transfer, Params = #transfer_params{
    transfer_id = TransferId,
    iterator = Iterator
}) ->
    CallbackModule = State#state.mod,

    {ProgressMarker, Results, NewIterator} = transfer_iterator:get_next_batch(
        UserCtx, ?LIST_BATCH_SIZE, Iterator
    ),

    Length = length(Results),
    transfer:increment_files_to_process_counter(TransferId, Length),
    RegularFileTransferParams = Params#transfer_params{iterator = undefined},
    lists:foreach(fun
        (error) ->
            transfer:increment_files_failed_and_processed_counters(TransferId);
        ({ok, FileCtx}) ->
            CallbackModule:enqueue_data_transfer(FileCtx, RegularFileTransferParams)
    end, Results),

    case ProgressMarker of
        done ->
            transfer:increment_files_processed_counter(TransferId),
            ok;
        more ->
            CallbackModule:enqueue_data_transfer(RootFileCtx, Params#transfer_params{
                iterator = NewIterator
            })
    end.
