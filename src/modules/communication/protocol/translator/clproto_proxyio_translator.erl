%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module handling translations between protobuf and internal protocol.
%%% op-worker is the server of these messages, so requests are translated
%%% only from protobuf and responses only to protobuf.
%%% @end
%%%-------------------------------------------------------------------
-module(clproto_proxyio_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("proto/oneclient/proxyio_messages.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================


-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'ProxyIORequest'{
    parameters = ProtoParameters,
    storage_id = SID,
    file_id = FID,
    proxyio_request = {_, Record}
}) ->
    Parameters = [clproto_common_translator:from_protobuf(P) || P <- ProtoParameters],
    #proxyio_request{
        parameters = maps:from_list(Parameters),
        storage_id = SID,
        file_id = FID,
        proxyio_request = from_protobuf(Record)
    };
from_protobuf(#'RemoteRead'{
    offset = Offset,
    size = Size
}) ->
    #remote_read{
        offset = Offset,
        size = Size
    };
from_protobuf(#'RemoteWrite'{byte_sequence = ByteSequences}) ->
    #remote_write{
        byte_sequence = [from_protobuf(BS) || BS <- ByteSequences]
    };
from_protobuf(#'ByteSequence'{offset = Offset, data = Data}) ->
    #byte_sequence{offset = Offset, data = Data};


%% OTHER
from_protobuf(undefined) -> undefined.


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#proxyio_response{
    status = Status,
    proxyio_response = ProxyIOResponse
}) ->
    {status, StatProto} = clproto_common_translator:to_protobuf(Status),
    {proxyio_response, #'ProxyIOResponse'{
        status = StatProto,
        proxyio_response = to_protobuf(ProxyIOResponse)
    }};
to_protobuf(#remote_data{data = Data}) ->
    {remote_data, #'RemoteData'{data = Data}};
to_protobuf(#remote_write_result{wrote = Wrote}) ->
    {remote_write_result, #'RemoteWriteResult'{wrote = Wrote}};


%% OTHER
to_protobuf(undefined) -> undefined.
