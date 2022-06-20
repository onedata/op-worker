%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for performing middleware interprovider communication. 
%%% All requests are performed with root effective session id, as it is assumed 
%%% that ensuing results are available to all providers.
%%% @end
%%%-------------------------------------------------------------------
-module(mi_interprovider_communicator).
-author("Michal Stanisz").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneprovider/mi_interprovider_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    gather/2,
    gather_from_cosupporting_providers/2,
    exec/4
]).

-type server_message() :: #server_message{}.
-type operation() :: middleware_worker:interprovider_operation().
-type gather_result() :: #{
    oneprovider:id() => {ok, middleware_worker:interprovider_result()} | errors:error()
}.

%%%===================================================================
%%% API
%%%===================================================================

-spec gather_from_cosupporting_providers(file_id:file_guid(), operation()) -> 
    gather_result().
gather_from_cosupporting_providers(FileGuid, Operation) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    {ok, Providers} = space_logic:get_provider_ids(SpaceId),
    Requests = maps_utils:generate_from_list(
        fun(ProviderId) -> {ProviderId, Operation} end, Providers),
    gather(FileGuid, Requests).


-spec gather(file_id:file_guid(), #{oneprovider:id() => operation()}) ->
    gather_result().
gather(FileGuid, ProviderOperations) ->
    ResList = lists_utils:pmap(fun({ProviderId, Operation}) ->
        {ProviderId, handle(FileGuid, ProviderId, Operation)}
    end, maps:to_list(ProviderOperations)),
    maps:from_list(ResList).


-spec exec(clproto_message_id:id() | undefined, session:id(), file_id:file_guid(), operation()) -> 
    {ok, server_message()}.
exec(MsgId, SessId, FileGuid, Operation) ->
    MessageBody = case middleware_worker:exec(SessId, FileGuid, Operation) of
        {ok, Result} ->
            #mi_interprovider_response{result = Result, status = ok};
        {error, _} = Error ->
            #mi_interprovider_response{result = Error, status = error}
    end,
    {ok, #server_message{
        message_id = MsgId,
        message_body = MessageBody
    }}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec handle(file_id:file_guid(), oneprovider:id(), operation()) -> 
    {ok, middleware_worker:interprovider_result()} | errors:error().
handle(FileGuid, ProviderId, Operation) ->
    ReceivedRes = case oneprovider:is_self(ProviderId) of
        true ->
            exec(undefined, ?ROOT_SESS_ID, FileGuid, Operation);
        false ->
            case connection:is_connected_to_provider(ProviderId) of
                true ->
                    SessId = session_utils:get_provider_session_id(outgoing, ProviderId),
                    Message = #client_message{
                        message_body = #mi_interprovider_request{
                            file_guid = FileGuid,
                            operation = Operation
                        },
                        effective_session_id = ?ROOT_SESS_ID
                    },
                    communicator:communicate_with_provider(SessId, Message);
                false ->
                    ?ERROR_NO_CONNECTION_TO_PEER_ONEPROVIDER
            end
    end,
    case ReceivedRes of
        {ok, #server_message{message_body = #mi_interprovider_response{result = ProviderResult, status = ok}}} ->
            {ok, ProviderResult};
        {ok, #server_message{message_body = #mi_interprovider_response{result = Error, status = error}}} ->
            Error;
        {error, _} = Error ->
            Error
    end.
