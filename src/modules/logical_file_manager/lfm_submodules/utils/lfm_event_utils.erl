%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Auxiliary functions for sending and flushing events.
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_event_utils).
-author("Tomasz Lichon").

-include("modules/events/definitions.hrl").
-include("timeouts.hrl").

%% API
-export([maybe_emit_file_written/4, maybe_emit_file_read/4,
    emit_file_truncated/3, flush_event_queue/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends a file written event if DoEmit flag is set to true
%% @end
%%--------------------------------------------------------------------
-spec maybe_emit_file_written(fslogic_worker:file_guid(),
    fslogic_blocks:blocks(), session:id(), DoEmit :: boolean()) ->
    ok | {error, Reason :: term()}.
maybe_emit_file_written(_FileGuid, _WrittenBlocks, _SessionId, false) ->
    ok;
maybe_emit_file_written(FileGuid, WrittenBlocks, SessionId, true) ->
    event:emit(#file_written_event{
        file_uuid = FileGuid,
        blocks = WrittenBlocks
    }, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a file read event if DoEmit flag is set to true
%% @end
%%--------------------------------------------------------------------
-spec maybe_emit_file_read(fslogic_worker:file_guid(),
    fslogic_blocks:blocks(), session:id(), DoEmit :: boolean()) ->
    ok | {error, Reason :: term()}.
maybe_emit_file_read(_FileGuid, _ReadBlocks, _SessionId, false) ->
    ok;
maybe_emit_file_read(FileGuid, ReadBlocks, SessionId, true) ->
    event:emit(#file_read_event{
        file_uuid = FileGuid,
        blocks = ReadBlocks
    }, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a file truncated event if DoEmit flag is set to true
%% @end
%%--------------------------------------------------------------------
-spec emit_file_truncated(fslogic_worker:file_guid(), non_neg_integer(), session:id()) ->
    ok | {error, Reason :: term()}.
emit_file_truncated(FileGuid, Size, SessionId) ->
    event:emit(#file_written_event{
        file_uuid = FileGuid,
        blocks = [],
        file_size = Size
    }, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Flushes event streams associated with the file written subscription
%% for a given session, uuid and provider_id.
%% @end
%%--------------------------------------------------------------------
-spec flush_event_queue(session:id(), od_provider:id(), file_meta:uuid()) ->
    ok | {error, term()}.
flush_event_queue(SessionId, ProviderId, FileUuid) ->
    case session:is_special(SessionId) of
        true ->
            ok;
        false ->
            RecvRef = event:flush(ProviderId, FileUuid, ?FILE_WRITTEN_SUB_ID,
                self(), SessionId),
            receive
                {RecvRef, Response} ->
                    Response
            after ?DEFAULT_REQUEST_TIMEOUT ->
                {error, timeout}
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================