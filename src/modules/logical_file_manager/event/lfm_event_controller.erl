%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Auxiliary functions for flushing events.
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_event_controller).
-author("Michal Wrzeszcz").

-include("modules/events/definitions.hrl").
-include("timeouts.hrl").

%% API
-export([flush_event_queue/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Flushes event streams associated with the file written subscription
%% for a given session, uuid and provider_id.
%% @end
%%--------------------------------------------------------------------
-spec flush_event_queue(session:id(), od_provider:id(), file_meta:uuid()) ->
    ok | {error, term()}.
% TODO VFS-7448 - test production of events for hardlinks
flush_event_queue(SessionId, ProviderId, FileUuid) ->
    case session_utils:is_special(SessionId) of
        true ->
            ok;
        false ->
            [Manager] = event:get_event_managers(SessionId),
            RecvRef = event:flush(ProviderId, fslogic_uuid:ensure_referenced_uuid(FileUuid),
                ?FILE_WRITTEN_SUB_ID, self(), Manager),
            receive_loop(RecvRef, Manager)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for worker asynchronous process answer.
%% @end
%%--------------------------------------------------------------------
-spec receive_loop(reference(), pid()) -> ok | {error, term()}.
receive_loop(RecvRef, Manager) ->
    receive
        {RecvRef, Response} ->
            Response
    after
        ?DEFAULT_REQUEST_TIMEOUT ->
            case rpc:call(node(Manager), erlang, is_process_alive, [Manager]) of
                true ->
                    % VFS-5206 - handle heartbeats
%%                    receive_loop(RecvRef, Manager);
                    {error, timeout};
                _ ->
                    {error, timeout}
            end
    end.
