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
-include_lib("ctool/include/posix/acl.hrl").
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

-define(DOC_ID_MISSING, doc_id_missing).

%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Callback called to get permissions required to check before starting transfer.
%% @end
%%--------------------------------------------------------------------
-callback required_permissions() -> [data_access_rights:requirement()].


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
-spec should_start(undefined | non_neg_integer()) -> boolean().
should_start(undefined) ->
    true;
should_start(NextRetryTimestamp) ->
    time_utils:cluster_time_seconds() >= NextRetryTimestamp.


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

    try
        FileCtx1 = fslogic_authz:ensure_authorized(UserCtx, FileCtx0, AccessDefinitions),
        transfer_data_insecure(UserCtx, FileCtx1, State, Params)
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
        Error:Reason ->
            ?error_stacktrace("Unexpected error ~p:~p during transfer ~p", [
                Error, Reason, Params#transfer_params.transfer_id
            ]),
            maybe_retry(FileCtx0, Params, RetriesLeft, {Error, Reason})
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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Transfers data from appropriate source, that is either files returned by
%% querying specified view or file system subtree (regular file or
%% entire directory depending on file ctx).
%% @end
%%--------------------------------------------------------------------
-spec transfer_data_insecure(user_ctx:ctx(), file_ctx:ctx(), state(), transfer_params()) ->
    ok | {error, term()}.
transfer_data_insecure(_, FileCtx, State, Params = #transfer_params{view_name = undefined}) ->
    transfer_fs_subtree(State, FileCtx, Params);
transfer_data_insecure(_, FileCtx, State, Params) ->
    transfer_files_from_view(State, FileCtx, Params, undefined).


%% @private
-spec enqueue_files_transfer(state(), [file_ctx:ctx()], transfer_params()) -> ok.
enqueue_files_transfer(#state{mod = Mod}, FileCtxs, TransferParams) ->
    lists:foreach(fun(FileCtx) ->
        Mod:enqueue_data_transfer(FileCtx, TransferParams, undefined, undefined)
    end, FileCtxs).


%% @private
-spec transfer_fs_subtree(state(), file_ctx:ctx(), transfer_params()) ->
    ok | {error, term()}.
transfer_fs_subtree(State = #state{mod = Mod}, FileCtx, Params) ->
    case transfer:is_ongoing(Params#transfer_params.transfer_id) of
        true ->
            case file_ctx:file_exists_const(FileCtx) of
                true ->
                    case file_ctx:is_dir(FileCtx) of
                        {true, FileCtx2} ->
                            transfer_dir(State, FileCtx2, 0, Params);
                        {false, FileCtx2} ->
                            Mod:transfer_regular_file(FileCtx2, Params)
                    end;
                false ->
                    {error, not_found}
            end;
        false ->
            throw(already_ended)
    end.


%% @private
-spec transfer_dir(state(), file_ctx:ctx(), non_neg_integer(), transfer_params()) ->
    ok | {error, term()}.
transfer_dir(State, FileCtx, Offset, TransferParams = #transfer_params{
    transfer_id = TransferId,
    user_ctx = UserCtx
}) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    {Children, FileCtx2} = file_ctx:get_file_children(FileCtx, UserCtx, Offset, Chunk),

    Length = length(Children),
    transfer:increment_files_to_process_counter(TransferId, Length),
    enqueue_files_transfer(State, Children, TransferParams),

    case Length < Chunk of
        true ->
            transfer:increment_files_processed_counter(TransferId),
            ok;
        false ->
            transfer_dir(State, FileCtx2, Offset + Chunk, TransferParams)
    end.


%% @private
-spec transfer_files_from_view(state(), file_ctx:ctx(), transfer_params(),
    file_meta:uuid() | undefined | ?DOC_ID_MISSING) -> ok | {error, term()}.
transfer_files_from_view(State = #state{mod = Mod}, FileCtx, Params, LastDocId) ->
    case transfer:is_ongoing(Params#transfer_params.transfer_id) of
        true ->
            Chunk = Mod:view_querying_chunk_size(),
            transfer_files_from_view(State, FileCtx, Params, Chunk, LastDocId);
        false ->
            throw(already_ended)
    end.


%% @private
-spec transfer_files_from_view(state(), file_ctx:ctx(), transfer_params(),
    non_neg_integer(), file_meta:uuid() | undefined | ?DOC_ID_MISSING) -> ok | {error, term()}.
transfer_files_from_view(State, FileCtx, Params, Chunk, LastDocId) ->
    #transfer_params{
        transfer_id = TransferId,
        view_name = ViewName,
        query_view_params = QueryViewParams
    } = Params,

    QueryViewParams2 = case LastDocId of
        undefined ->
            [{skip, 0} | QueryViewParams];
        ?DOC_ID_MISSING ->
            % doc_id is missing when view has reduce function defined
            % in such case we must iterate over results using limit and skip
            [{skip, Chunk} | QueryViewParams];
        _ ->
            [{skip, 1}, {startkey_docid, LastDocId} | QueryViewParams]
    end,
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    case index:query(SpaceId, ViewName, [{limit, Chunk} | QueryViewParams2]) of
        {ok, #{<<"rows">> := Rows}} ->
            {NewLastDocId, FileCtxs} = lists:foldl(fun(Row, {_LastDocId, FileCtxsIn}) ->
                Value = maps:get(<<"value">>, Row),
                DocId = maps:get(<<"id">>, Row, ?DOC_ID_MISSING),
                ObjectIds = case is_list(Value) of
                    true -> lists:flatten(Value);
                    false -> [Value]
                end,
                transfer:increment_files_to_process_counter(TransferId, length(ObjectIds)),
                NewFileCtxs = lists:filtermap(fun(O) ->
                    try
                        {ok, G} = file_id:objectid_to_guid(O),
                        NewFileCtx0 = file_ctx:new_by_guid(G),
                        case file_ctx:file_exists_const(NewFileCtx0) of
                            true ->
                                % TODO VFS-6386 Enable and test view transfer with dirs
                                case file_ctx:is_dir(NewFileCtx0) of
                                    {true, _} ->
                                        transfer:increment_files_processed_counter(TransferId),
                                        false;
                                    {false, NewFileCtx1} ->
                                        {true, NewFileCtx1}
                                end;
                            false ->
                                % TODO VFS-4218 currently we silently omit garbage
                                % returned from view (view can return anything)
                                transfer:increment_files_processed_counter(TransferId),
                                false
                        end
                    catch
                        Error:Reason ->
                            transfer:increment_files_failed_and_processed_counters(TransferId),
                            ?error_stacktrace(
                                "Processing result of query view ~p "
                                "in space ~p failed due to ~p:~p", [
                                    ViewName, SpaceId, Error, Reason
                                ]
                            ),
                            false
                    end
                end, ObjectIds),
                {DocId, FileCtxsIn ++ NewFileCtxs}
            end, {undefined, []}, Rows),

            enqueue_files_transfer(State, FileCtxs, Params#transfer_params{
                view_name = undefined, query_view_params = undefined
            }),
            case length(Rows) < Chunk of
                true ->
                    transfer:increment_files_processed_counter(TransferId),
                    ok;
                false ->
                    transfer_files_from_view(State, FileCtx, Params, NewLastDocId)
            end;
        Error = {error, Reason} ->
            ?error("Querying view ~p failed due to ~p when processing transfer ~p", [
                ViewName, Reason, TransferId
            ]),
            Error
    end.
