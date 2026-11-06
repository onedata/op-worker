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
-module(clproto_provider_req_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'ProviderRequest'{
    context_guid = ContextGuid,
    provider_request = {_, Record}
}) ->
    #provider_request{
        context_guid = ContextGuid,
        provider_request = from_protobuf(Record)
    };
from_protobuf(#'GetParent'{}) ->
    #get_parent{};
from_protobuf(#'GetAcl'{}) ->
    #get_acl{};
from_protobuf(#'SetAcl'{acl = Acl}) ->
    #set_acl{acl = from_protobuf(Acl)};
from_protobuf(#'GetFilePath'{}) ->
    #get_file_path{};
from_protobuf(#'FSync'{
    data_only = DataOnly,
    handle_id = HandleId
}) ->
    #fsync{
        data_only = DataOnly,
        handle_id = HandleId
    };
from_protobuf(#'ProviderResponse'{
    status = Status,
    provider_response = {_, ProviderResponse}
}) ->
    #provider_response{
        status = clproto_common_translator:from_protobuf(Status),
        provider_response = from_protobuf(ProviderResponse)
    };
from_protobuf(#'ProviderResponse'{status = Status}) ->
    #provider_response{
        status = clproto_common_translator:from_protobuf(Status)
    };
from_protobuf(#'Acl'{value = Value}) ->
    #acl{value = acl:from_json(json_utils:decode(Value), cdmi)};
from_protobuf(#'FilePath'{value = Value}) ->
    #file_path{value = Value};
from_protobuf(#'CheckPerms'{flag = Flag}) ->
    #check_perms{flag = open_flag_translate_from_protobuf(Flag)};

%% OTHER
from_protobuf(undefined) -> undefined;
from_protobuf(Other) -> clproto_common_translator:from_protobuf(Other).


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#provider_request{
    context_guid = ContextGuid,
    provider_request = Record
}) ->
    {provider_request, #'ProviderRequest'{
        context_guid = ContextGuid,
        provider_request = to_protobuf(Record)
    }};
to_protobuf(#get_parent{}) ->
    {get_parent, #'GetParent'{}};
to_protobuf(#get_acl{}) ->
    {get_acl, #'GetAcl'{}};
to_protobuf(#set_acl{acl = Acl}) ->
    {_, PAcl} = to_protobuf(Acl),
    {set_acl, #'SetAcl'{acl = PAcl}};
to_protobuf(#get_file_path{}) ->
    {get_file_path, #'GetFilePath'{}};


to_protobuf(#provider_response{
    status = Status,
    provider_response = ProviderResponse
}) ->
    {status, StatProto} = clproto_common_translator:to_protobuf(Status),
    {provider_response, #'ProviderResponse'{
        status = StatProto,
        provider_response = to_protobuf(ProviderResponse)
    }};
to_protobuf(#acl{value = Value}) ->
    {acl, #'Acl'{
        value = json_utils:encode(acl:to_json(Value, cdmi))}
    };
to_protobuf(#file_path{value = Value}) ->
    {file_path, #'FilePath'{value = Value}};
to_protobuf(#check_perms{flag = Flag}) ->
    {check_perms, #'CheckPerms'{
        flag = open_flag_translate_to_protobuf(Flag)
    }};


%% OTHER
to_protobuf(undefined) -> undefined;
to_protobuf(Other) -> clproto_common_translator:to_protobuf(Other).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec open_flag_translate_to_protobuf(fslogic_worker:open_flag()) ->
    'READ_WRITE' | 'READ' | 'WRITE'.
open_flag_translate_to_protobuf(read) -> 'READ';
open_flag_translate_to_protobuf(write) -> 'WRITE';
open_flag_translate_to_protobuf(_) -> 'READ_WRITE'.


-spec open_flag_translate_from_protobuf('READ_WRITE' | 'READ' | 'WRITE') ->
    fslogic_worker:open_flag().
open_flag_translate_from_protobuf('READ') -> read;
open_flag_translate_from_protobuf('WRITE') -> write;
open_flag_translate_from_protobuf(_) -> rdwr.