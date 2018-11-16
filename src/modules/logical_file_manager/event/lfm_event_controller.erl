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
-module(lfm_event_controller).
-author("Tomasz Lichon").

% MWevents - flush

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
flush_event_queue(SessionId, ProviderId, FileUuid) ->
    case session:is_special(SessionId) of
        true ->
            ok;
        false ->
            [Manager] = event:get_event_managers(SessionId),
            RecvRef = event:flush(ProviderId, FileUuid, ?FILE_WRITTEN_SUB_ID,
                self(), Manager),
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
                    % TODO - VFS-4131
%%                    receive_loop(RecvRef, Manager);
                    {error, timeout};
                _ ->
                    {error, timeout}
            end
    end.
