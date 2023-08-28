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
-module(clproto_rtransfer_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("proto/oneprovider/rtransfer_messages.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'GenerateRTransferConnSecret'{secret = Secret}) ->
    #generate_rtransfer_conn_secret{secret = Secret};
from_protobuf(#'RTransferConnSecret'{secret = Secret}) ->
    #rtransfer_conn_secret{secret = Secret};
from_protobuf(#'GetRTransferNodesIPs'{}) ->
    #get_rtransfer_nodes_ips{};
from_protobuf(#'RTransferNodesIPs'{nodes = undefined}) ->
    #rtransfer_nodes_ips{nodes = []};
from_protobuf(#'RTransferNodesIPs'{nodes = Nodes}) ->
    #rtransfer_nodes_ips{
        nodes = [clproto_common_translator:from_protobuf(N) || N <- Nodes]
    };

%% OTHER
from_protobuf(undefined) -> undefined.


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#generate_rtransfer_conn_secret{
    secret = Secret
}) ->
    {generate_rtransfer_conn_secret, #'GenerateRTransferConnSecret'{
        secret = Secret
    }};
to_protobuf(#rtransfer_conn_secret{
    secret = Secret
}) ->
    {rtransfer_conn_secret, #'RTransferConnSecret'{
        secret = Secret
    }};
to_protobuf(#get_rtransfer_nodes_ips{}) ->
    {get_rtransfer_nodes_ips, #'GetRTransferNodesIPs'{}};
to_protobuf(#rtransfer_nodes_ips{nodes = Nodes}) ->
    {rtransfer_nodes_ips, #'RTransferNodesIPs'{
        nodes = [clproto_common_translator:to_protobuf(N) || N <- Nodes]
    }};

%% OTHER
to_protobuf(undefined) -> undefined.
