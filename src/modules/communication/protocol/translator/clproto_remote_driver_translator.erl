%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module handling translations between protobuf and internal protocol.
%%% @end
%%%-------------------------------------------------------------------
-module(clproto_remote_driver_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("proto/oneprovider/remote_driver_messages.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'GetRemoteDocument'{
    model = Model,
    key = Key,
    routing_key = RoutingKey
}) ->
    #get_remote_document{
        model = binary_to_atom(Model, utf8),
        key = Key,
        routing_key = RoutingKey
    };
from_protobuf(#'RemoteDocument'{
    status = Status,
    compressed_data = Data
}) ->
    #remote_document{
        status = clproto_common_translator:from_protobuf(Status),
        compressed_data = Data
    };

%% OTHER
from_protobuf(undefined) -> undefined.


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#get_remote_document{
    model = Model,
    key = Key,
    routing_key = RoutingKey
}) ->
    {get_remote_document, #'GetRemoteDocument'{
        model = atom_to_binary(Model, utf8),
        key = Key,
        routing_key = RoutingKey
    }};
to_protobuf(#remote_document{
    compressed_data = Data,
    status = Status
}) ->
    {status, StatusProto} = clproto_common_translator:to_protobuf(Status),
    {remote_document, #'RemoteDocument'{
        status = StatusProto,
        compressed_data = Data
    }};


%% OTHER
to_protobuf(undefined) -> undefined.
