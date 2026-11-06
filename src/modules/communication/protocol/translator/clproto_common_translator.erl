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
-module(clproto_common_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'Status'{
    code = Code,
    description = Desc
}) ->
    #status{
        code = Code,
        description = Desc
    };
from_protobuf(#'FileBlock'{
    offset = Offset,
    size = Size
}) ->
    #file_block{
        offset = Offset,
        size = Size
    };
from_protobuf(#'FileRenamedEntry'{
    old_uuid = OldGuid,
    new_uuid = NewGuid,
    new_parent_uuid = NewParentGuid,
    new_name = NewName
}) ->
    #file_renamed_entry{
        old_guid = OldGuid,
        new_guid = NewGuid,
        new_parent_guid = NewParentGuid,
        new_name = NewName
    };
from_protobuf(#'IpAndPort'{
    ip = IpString,
    port = Port
}) ->
    {ok, IP} = inet:parse_ipv4strict_address(binary_to_list(IpString)),
    #ip_and_port{ip = IP, port = Port};
from_protobuf(#'Dir'{uuid = UUID}) ->
    #dir{guid = UUID};
from_protobuf(#'Parameter'{
    key = Key,
    value = Value
}) ->
    {Key, Value};

%% OTHER
from_protobuf(undefined) -> undefined.


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#status{
    code = Code,
    description = Desc
}) ->
    {status, #'Status'{
        code = Code,
        description = Desc
    }};
to_protobuf(#file_block{
    offset = Off,
    size = S
}) ->
    #'FileBlock'{
        offset = Off,
        size = S
    };
to_protobuf(#file_renamed_entry{} = Record) ->
    #'FileRenamedEntry'{
        old_uuid = Record#'file_renamed_entry'.old_guid,
        new_uuid = Record#'file_renamed_entry'.new_guid,
        new_parent_uuid = Record#'file_renamed_entry'.new_parent_guid,
        new_name = Record#'file_renamed_entry'.new_name
    };
to_protobuf(#ip_and_port{
    ip = IP,
    port = Port
}) ->
    #'IpAndPort'{
        ip = list_to_binary(inet:ntoa(IP)),
        port = Port
    };
to_protobuf(#dir{guid = UUID}) ->
    {dir, #'Dir'{uuid = UUID}};


%% OTHER
to_protobuf(undefined) -> undefined.
