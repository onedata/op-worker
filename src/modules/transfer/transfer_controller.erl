%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Manages data transfers, which include starting the transfer and tracking transfer's status.
%%% Such gen_server is created for each data transfer process.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_controller).
-author("Tomasz Lichon").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([on_new_transfer_doc/1, on_transfer_doc_change/1, mark_finished/1, mark_failed/2
]).
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

-type state() :: #state{}.

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
    status = scheduled,
    target_provider_id = TargetProviderId
}}) ->
    case oneprovider:is_self(TargetProviderId) of
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
-spec mark_failed(pid(), term()) -> ok.
mark_failed(Pid, Reason) ->
    Pid ! {transfer_failed, Reason}.

%%-------------------------------------------------------------------
%% @doc
%% Stops transfer_controller process and marks transfer as completed.
%% @end
%%-------------------------------------------------------------------
-spec mark_finished(pid()) -> ok.
mark_finished(Pid) ->
    Pid ! transfer_finished.

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
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init(_Args) ->
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast({start_transfer, SessionId, SpaceId, TransferId, FileGuid, Callback, InvalidateSourceReplica}, State) ->
    sync_req:start_transfer(user_ctx:new(SessionId), file_ctx:new_by_guid(FileGuid), undefined, TransferId, self()),
    {ok, #document{value = #transfer{space_id = SpaceId}}} = transfer:get(SpaceId),
    receive
        transfer_finished ->
            transfer:mark_completed(TransferId, SpaceId, InvalidateSourceReplica),
            notify_callback(Callback, InvalidateSourceReplica);
        {transfer_failed, Reason} ->
            ?error_stacktrace("Transfer ~p failed due to ~p", [TransferId, Reason]),
            transfer:mark_failed(TransferId, SpaceId)
    end,
    {stop, normal, State};
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
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
    State :: state()) -> term().
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
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
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
    worker_pool:cast(?TRANSFER_CONTROLLERS_POOL, {
        start_transfer,
        session:root_session_id(),
        SpaceId,
        TransferId,
        FileGuid,
        Callback,
        InvalidateSourceReplica
    }).

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