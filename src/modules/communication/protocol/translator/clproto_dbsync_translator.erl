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
-module(clproto_dbsync_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("proto/oneprovider/dbsync_messages.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'DBSyncRequest'{
    message_body = {_, MessageBody}
}) ->
    #dbsync_request{
        message_body = from_protobuf(MessageBody)
    };
from_protobuf(#'TreeBroadcast'{
    message_body = {_, MessageBody},
    depth = Depth,
    excluded_providers = ExcludedProv,
    l_edge = LEdge,
    r_edge = REgde,
    request_id = ReqId,
    space_id = SpaceId
}) ->
    #tree_broadcast{
        message_body = from_protobuf(MessageBody),
        depth = Depth,
        l_edge = LEdge,
        r_edge = REgde,
        space_id = SpaceId,
        request_id = ReqId,
        excluded_providers = ExcludedProv
    };
from_protobuf(#'BatchUpdate'{
    space_id = SpaceId,
    since_seq = Since,
    until_seq = Until,
    changes_encoded = Changes
}) ->
    #batch_update{
        space_id = SpaceId,
        since_seq = Since,
        until_seq = Until,
        changes_encoded = Changes
    };
from_protobuf(#'StatusReport'{
    space_id = SpaceId,
    seq_num = SeqNum
}) ->
    #status_report{
        space_id = SpaceId,
        seq = SeqNum
    };
from_protobuf(#'StatusRequest'{}) ->
    #status_request{};
from_protobuf(#'ChangesRequest'{
    since_seq = Since,
    until_seq = Until
}) ->
    #changes_request{
        since_seq = Since,
        until_seq = Until
    };
from_protobuf(#'DBSyncMessage'{
    message_body = {_, MB}
}) ->
    #dbsync_message{
        message_body = from_protobuf(MB)
    };
from_protobuf(#'TreeBroadcast2'{
    src_provider_id = SrcProviderId,
    low_provider_id = LowProviderId,
    high_provider_id = HighProviderId,
    message_id = MsgId,
    changes_batch = CB
}) ->
    #tree_broadcast2{
        src_provider_id = SrcProviderId,
        low_provider_id = LowProviderId,
        high_provider_id = HighProviderId,
        message_id = MsgId,
        message_body = from_protobuf(CB)
    };
from_protobuf(#'ChangesBatch'{
    space_id = SpaceId,
    since = Since,
    until = Until,
    timestamp = Timestamp,
    compressed = Compressed,
    docs = Docs
}) ->
    Timestamp2 = case Timestamp of
        0 -> undefined;
        _ -> Timestamp
    end,
    #changes_batch{
        space_id = SpaceId,
        since = Since,
        until = Until,
        timestamp = Timestamp2,
        compressed = Compressed,
        docs = Docs
    };
from_protobuf(#'ChangesRequest2'{
    space_id = SpaceId,
    since = Since,
    until = Until,
    included_mutators = IncludedMutators
}) ->
    #changes_request2{
        space_id = SpaceId,
        since = Since,
        until = Until,
        included_mutators = IncludedMutators
    };

%% OTHER
from_protobuf(undefined) -> undefined.


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#dbsync_request{
    message_body = MessageBody
}) ->
    {dbsync_request, #'DBSyncRequest'{
        message_body = to_protobuf(MessageBody)
    }};
to_protobuf(#tree_broadcast{
    message_body = MessageBody,
    depth = Depth,
    excluded_providers = ExcludedProv,
    l_edge = LEdge,
    r_edge = REgde,
    request_id = ReqId,
    space_id = SpaceId
}) ->
    {tree_broadcast, #'TreeBroadcast'{
        message_body = to_protobuf(MessageBody),
        depth = Depth,
        l_edge = LEdge,
        r_edge = REgde,
        space_id = SpaceId,
        request_id = ReqId,
        excluded_providers = ExcludedProv
    }};
to_protobuf(#batch_update{
    space_id = SpaceId,
    since_seq = Since,
    until_seq = Until,
    changes_encoded = Changes
}) ->
    {batch_update, #'BatchUpdate'{
        space_id = SpaceId,
        since_seq = Since,
        until_seq = Until,
        changes_encoded = Changes
    }};
to_protobuf(#status_report{
    space_id = SpaceId,
    seq = SeqNum
}) ->
    {status_report, #'StatusReport'{
        space_id = SpaceId,
        seq_num = SeqNum
    }};
to_protobuf(#status_request{}) ->
    {status_request, #'StatusRequest'{}};
to_protobuf(#changes_request{
    since_seq = Since,
    until_seq = Until
}) ->
    {changes_request, #'ChangesRequest'{
        since_seq = Since,
        until_seq = Until
    }};
to_protobuf(#dbsync_message{
    message_body = MB
}) ->
    {dbsync_message, #'DBSyncMessage'{
        message_body = to_protobuf(MB)
    }};
to_protobuf(#tree_broadcast2{message_body = CB} = TB) ->
    {_, CB2} = to_protobuf(CB),
    {tree_broadcast, #'TreeBroadcast2'{
        src_provider_id = TB#'tree_broadcast2'.src_provider_id,
        low_provider_id = TB#'tree_broadcast2'.low_provider_id,
        high_provider_id = TB#'tree_broadcast2'.high_provider_id,
        message_id = TB#'tree_broadcast2'.message_id,
        changes_batch = CB2
    }};
to_protobuf(#changes_batch{} = CB) ->
    Timestamp = case CB#'changes_batch'.timestamp of
        undefined -> 0;
        Other -> Other
    end,
    {changes_batch, #'ChangesBatch'{
        space_id = CB#'changes_batch'.space_id,
        since = CB#'changes_batch'.since,
        until = CB#'changes_batch'.until,
        timestamp = Timestamp,
        compressed = CB#'changes_batch'.compressed,
        docs = CB#'changes_batch'.docs
    }};
to_protobuf(#changes_request2{} = CR) ->
    {changes_request, #'ChangesRequest2'{
        space_id = CR#'changes_request2'.space_id,
        since = CR#'changes_request2'.since,
        until = CR#'changes_request2'.until,
        included_mutators = CR#'changes_request2'.included_mutators
    }};


%% OTHER
to_protobuf(undefined) -> undefined.
