%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for provider RPC communication.
%%% All requests are performed with root effective session id, as it is assumed 
%%% that ensuing results are available to all providers.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_rpc).
-author("Michal Stanisz").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/data_distribution.hrl").
-include("proto/oneprovider/provider_rpc_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([
    call/3,
    gather/2,
    gather_from_cosupporting_providers/2
]).

-type request() :: provider_rpc_worker:request().
-type result() :: provider_rpc_worker:result().

-type gather_result() :: #{
    oneprovider:id() => {ok, result()} | errors:error()
}.

-export_type([request/0, result/0, gather_result/0]).

%%%===================================================================
%%% API
%%%===================================================================


-spec call(oneprovider:id(), file_id:file_guid(), request()) ->
    {ok, result()} | errors:error().
call(ProviderId, FileGuid, Request) ->
    ProviderRpcCall = #provider_rpc_call{
        file_guid = FileGuid,
        request = Request
    },
    ReceivedRes = case oneprovider:is_self(ProviderId) of
        true ->
            provider_rpc_worker:handle(ProviderRpcCall);
        false ->
            case connection:find_outgoing_session(ProviderId) of
                {ok, SessId} ->
                    Message = #client_message{
                        message_body = ProviderRpcCall
                    },
                    case communicator:communicate_with_provider(SessId, Message) of
                        {ok, #server_message{message_body = MessageBody}} -> {ok, MessageBody};
                        {error, _} = CommunicatorError -> CommunicatorError
                    end;
                error ->
                    ?ERROR_NO_CONNECTION_TO_PEER_ONEPROVIDER
            end
    end,
    case ReceivedRes of
        {ok, #provider_rpc_response{result = ProviderResult, status = ok}} ->
            {ok, ProviderResult};
        {ok, #provider_rpc_response{result = Error, status = error}} ->
            Error;
        {error, _} = Error ->
            Error
    end.


-spec gather_from_cosupporting_providers(file_id:file_guid(), request()) ->
    gather_result().
gather_from_cosupporting_providers(FileGuid, Request) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    {ok, Providers} = space_logic:get_provider_ids(SpaceId),
    Requests = maps_utils:generate_from_list(
        fun(ProviderId) -> {ProviderId, Request} end, Providers),
    gather(FileGuid, Requests).


-spec gather(file_id:file_guid(), #{oneprovider:id() => request()}) ->
    gather_result().
gather(FileGuid, ProviderRequests) ->
    ResList = lists_utils:pmap(fun({ProviderId, Request}) ->
        {ProviderId, call(ProviderId, FileGuid, Request)}
    end, maps:to_list(ProviderRequests)),
    maps:from_list(ResList).
