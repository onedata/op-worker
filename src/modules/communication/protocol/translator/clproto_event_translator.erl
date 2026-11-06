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
-module(clproto_event_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("proto/oneclient/event_messages.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'Event'{type = {_, Record}}) ->
    #event{type = from_protobuf(Record)};
from_protobuf(#'Events'{events = Evts}) ->
    #events{events = [from_protobuf(Evt) || Evt <- Evts]};
from_protobuf(#'FlushEvents'{
    provider_id = ProviderId,
    subscription_id = SubscriptionId,
    context = Ctx
}) ->
    #flush_events{
        provider_id = ProviderId,
        subscription_id = integer_to_binary(SubscriptionId),
        context = Ctx
    };
from_protobuf(#'FileReadEvent'{
    counter = Counter,
    file_uuid = FileGuid,
    size = Size,
    blocks = Blocks
}) ->
    #file_read_event{
        counter = Counter,
        file_guid = FileGuid,
        size = Size,
        blocks = [clproto_common_translator:from_protobuf(B) || B <- Blocks]
    };
from_protobuf(#'FileWrittenEvent'{
    counter = Counter,
    file_uuid = FileGuid,
    size = Size,
    file_size = FileSize,
    blocks = Blocks
}) ->
    #file_written_event{
        counter = Counter,
        file_guid = FileGuid,
        size = Size,
        file_size = FileSize,
        blocks = [clproto_common_translator:from_protobuf(B) || B <- Blocks]
    };
from_protobuf(#'FileAttrChangedEvent'{file_attr = FileAttr}) ->
    #file_attr_changed_event{
        file_attr = clproto_fuse_translator:from_protobuf(FileAttr)
    };
from_protobuf(#'FileLocationChangedEvent'{
    file_location = FileLocation,
    change_beg_offset = O,
    change_end_offset = S
}) ->
    #file_location_changed_event{
        file_location = clproto_fuse_translator:from_protobuf(FileLocation),
        change_beg_offset = O,
        change_end_offset = S
    };
from_protobuf(#'FilePermChangedEvent'{
    file_uuid = FileGuid
}) ->
    #file_perm_changed_event{
        file_guid = FileGuid
    };
from_protobuf(#'FileRemovedEvent'{file_uuid = FileGuid}) ->
    #file_removed_event{file_guid = FileGuid};
from_protobuf(#'FileRenamedEvent'{
    top_entry = TopEntry,
    child_entries = ChildEntries
}) ->
    #file_renamed_event{
        top_entry = clproto_common_translator:from_protobuf(TopEntry),
        child_entries = [clproto_common_translator:from_protobuf(E) || E <- ChildEntries]
    };
from_protobuf(#'QuotaExceededEvent'{spaces = Spaces}) ->
    #quota_exceeded_event{spaces = Spaces};
from_protobuf(#'HelperParamsChangedEvent'{storage_id = StorageId}) ->
    #helper_params_changed_event{storage_id = StorageId};

%% OTHER
from_protobuf(undefined) -> undefined.


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#event{type = Type}) ->
    #'Event'{type = to_protobuf(Type)};
to_protobuf(#events{events = Evts}) ->
    {events, #'Events'{
        events = [to_protobuf(Evt) || Evt <- Evts]}
    };
to_protobuf(#flush_events{
    provider_id = ProviderId,
    subscription_id = SubId,
    context = Context
}) ->
    {flush_events, #'FlushEvents'{
        provider_id = ProviderId,
        subscription_id = binary_to_integer(SubId),
        context = Context}
    };
to_protobuf(#file_read_event{
    counter = Counter,
    file_guid = FileGuid,
    size = Size,
    blocks = Blocks
}) ->
    {file_read, #'FileReadEvent'{
        counter = Counter,
        file_uuid = FileGuid,
        size = Size,
        blocks = [clproto_common_translator:to_protobuf(B) || B <- Blocks]
    }};
to_protobuf(#file_written_event{
    counter = Counter,
    file_guid = FileGuid,
    size = Size,
    file_size = FileSize,
    blocks = Blocks
}) ->
    {file_written, #'FileWrittenEvent'{
        counter = Counter,
        file_uuid = FileGuid,
        size = Size,
        file_size = FileSize,
        blocks = [clproto_common_translator:to_protobuf(B) || B <- Blocks]
    }};
to_protobuf(#file_attr_changed_event{file_attr = FileAttr}) ->
    {_, Record} = clproto_fuse_translator:to_protobuf(FileAttr),
    {file_attr_changed, #'FileAttrChangedEvent'{
        file_attr = Record
    }};
to_protobuf(#file_location_changed_event{
    file_location = FileLocation,
    change_beg_offset = O,
    change_end_offset = S
}) ->
    {_, Record} = clproto_fuse_translator:to_protobuf(FileLocation),
    {file_location_changed, #'FileLocationChangedEvent'{
        file_location = Record,
        change_beg_offset = O,
        change_end_offset = S
    }};
to_protobuf(#file_perm_changed_event{
    file_guid = FileGuid
}) ->
    {file_perm_changed, #'FilePermChangedEvent'{
        file_uuid = FileGuid
    }};
to_protobuf(#file_removed_event{
    file_guid = FileGuid
}) ->
    {file_removed, #'FileRemovedEvent'{
        file_uuid = FileGuid
    }};
to_protobuf(#file_renamed_event{
    top_entry = TopEntry,
    child_entries = ChildEntries
}) ->
    {file_renamed, #'FileRenamedEvent'{
        top_entry = clproto_common_translator:to_protobuf(TopEntry),
        child_entries = [clproto_common_translator:to_protobuf(Entry) || Entry <- ChildEntries]
    }};
to_protobuf(#quota_exceeded_event{spaces = Spaces}) ->
    {quota_exceeded, #'QuotaExceededEvent'{spaces = Spaces}};
to_protobuf(#helper_params_changed_event{storage_id = StorageId}) ->
    {helper_params_changed, #'HelperParamsChangedEvent'{storage_id = StorageId}};


%% OTHER
to_protobuf(undefined) -> undefined.
