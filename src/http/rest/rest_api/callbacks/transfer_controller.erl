%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Manages data transfers, which include starting the transfer and tracking transfer's status.
%%% Such gen_server is created for each data transfer, process.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_controller).
-author("Tomasz Lichon").

-behaviour(gen_server).

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([on_new_transfer_doc/1, on_transfer_doc_change/1, finish_transfer/1, failed_transfer/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    transfer_id :: transfer:id(),
    session_id :: session:id(),
    callback :: transfer:callback(),
    file_guid :: fslogic_worker:file_guid(),
    invalidate_source_replica :: boolean(),
    space_id :: od_space:id()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Callback called when new transfer doc is created locally.
%% (Because dbsync doesn't detect local changes).
%% @end
%%--------------------------------------------------------------------
-spec on_new_transfer_doc(transfer:doc()) -> ok.
on_new_transfer_doc(Transfer) ->
    on_transfer_doc_change(Transfer).

%%--------------------------------------------------------------------
%% @doc
%% Callback called when transfer doc is synced from some other provider.
%% @end
%%--------------------------------------------------------------------
-spec on_transfer_doc_change(transfer:doc()) -> ok.
on_transfer_doc_change(Transfer = #document{value = #transfer{
    transfer_status = scheduled,
    target_provider_id = TargetProviderId
}}) ->
    case TargetProviderId =:= oneprovider:get_provider_id() of
        true ->
            new_transfer(Transfer);
        false ->
            ok
    end;
on_transfer_doc_change(_ExistingTransfer) ->
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Stops transfer_controller process and marks transfer as completed.
%% @end
%%-------------------------------------------------------------------
-spec finish_transfer(pid()) -> ok.
finish_transfer(Pid) ->
    gen_server2:cast(Pid, finish_transfer),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Stops transfer_controller process and marks transfer as failed.
%% @end
%%-------------------------------------------------------------------
-spec failed_transfer(pid(), term()) -> ok.
failed_transfer(Pid, Error) ->
    gen_server2:cast(Pid, {failed_transfer, Error}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SessionId, TransferId, FileGuid, Callback, InvalidationSourceReplica]) ->
    ok = gen_server2:cast(self(), start_transfer),
    {ok, #state{
        transfer_id = TransferId,
        session_id = SessionId,
        file_guid = FileGuid,
        callback = Callback,
        invalidate_source_replica = InvalidationSourceReplica,
        space_id = fslogic_uuid:guid_to_space_id(FileGuid)
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast(start_transfer, State = #state{
    transfer_id = TransferId,
    session_id = SessionId,
    file_guid = FileGuid,
    space_id = SpaceId
}) ->
    try
        transfer:mark_active(TransferId),
        #provider_response{
            status = #status{code = ?OK}
        } = sync_req:replicate_file(user_ctx:new(SessionId),
            file_ctx:new_by_guid(FileGuid), undefined, TransferId
        ),
        {noreply, State}
    catch
        _:E ->
            ?error_stacktrace("Could not replicate file ~p due to ~p", [FileGuid, E]),
            transfer:mark_failed(TransferId, SpaceId),
            {stop, E, State}
    end;
handle_cast(finish_transfer, State = #state{
    transfer_id = TransferId,
    callback = Callback,
    invalidate_source_replica = InvalidationSourceReplica
}) ->
    transfer:mark_completed(TransferId),
    notify_callback(Callback, InvalidationSourceReplica),
    {stop, normal, State};
handle_cast({failed_transfer, Error}, State = #state{
    file_guid = FileGuid,
    transfer_id = TransferId,
    space_id = space_id = SpaceId
}) ->
    ?error_stacktrace("Could not replicate file ~p due to ~p", [FileGuid, Error]),
    transfer:mark_failed(TransferId, SpaceId),
    {stop, Error, State};
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_info(_Info, State) ->
    ?log_bad_request(_Info),
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
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term().
terminate(_Reason, #state{
    session_id = SessionId,
    transfer_id = TransferId
}) ->
    session:remove_transfer(SessionId, TransferId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================\

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts new transfer based on existing doc synchronized from other provider
%% @end
%%--------------------------------------------------------------------
-spec new_transfer(transfer:doc()) -> ok.
new_transfer(#document{
    key = TransferId,
    value = #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        callback = Callback,
        invalidate_source_replica = InvalidateSourceReplica
    }
}) ->
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    {ok, _Pid} = gen_server2:start(transfer_controller,
        [session:root_session_id(), TransferId, FileGuid, Callback, InvalidateSourceReplica], []),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notifies callback about successful transfer
%% @end
%%--------------------------------------------------------------------
-spec notify_callback(transfer:callback(), InvalidationSourceReplica :: boolean()) -> ok.
notify_callback(_Callback, true) -> ok;
notify_callback(undefined, false) -> ok;
notify_callback(<<>>, false) -> ok;
notify_callback(Callback, false) ->
    {ok, _, _, _} = http_client:get(Callback).