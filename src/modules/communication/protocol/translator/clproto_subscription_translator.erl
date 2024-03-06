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
-module(clproto_subscription_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("global_definitions.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'Subscription'{
    id = Id,
    type = {_, Record}
}) ->
    #subscription{
        id = integer_to_binary(Id),
        type = from_protobuf(Record)
    };
from_protobuf(#'FileReadSubscription'{
    counter_threshold = CounterThreshold,
    time_threshold = TimeThreshold
}) ->
    #file_read_subscription{
        counter_threshold = CounterThreshold,
        time_threshold = TimeThreshold
    };
from_protobuf(#'FileWrittenSubscription'{
    counter_threshold = CounterThreshold,
    time_threshold = TimeThreshold
}) ->
    #file_written_subscription{
        counter_threshold = CounterThreshold,
        time_threshold = TimeThreshold
    };
from_protobuf(#'FileAttrChangedSubscription'{
    file_uuid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    #file_attr_changed_subscription{
        file_guid = FileGuid,
        time_threshold = TimeThreshold
    };
from_protobuf(#'ReplicaStatusChangedSubscription'{
    file_uuid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    #replica_status_changed_subscription{
        file_guid = FileGuid,
        time_threshold = TimeThreshold
    };
from_protobuf(#'FileLocationChangedSubscription'{
    file_uuid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    #file_location_changed_subscription{
        file_guid = FileGuid,
        time_threshold = TimeThreshold
    };
from_protobuf(#'FilePermChangedSubscription'{
    file_uuid = FileGuid
}) ->
    #file_perm_changed_subscription{
        file_guid = FileGuid
    };
from_protobuf(#'FileRemovedSubscription'{
    file_uuid = FileGuid
}) ->
    #file_removed_subscription{
        file_guid = FileGuid
    };
from_protobuf(#'FileRenamedSubscription'{
    file_uuid = FileGuid
}) ->
    #file_renamed_subscription{
        file_guid = FileGuid
    };
from_protobuf(#'QuotaExceededSubscription'{}) ->
    #quota_exceeded_subscription{};
from_protobuf(#'HelperParamsChangedSubscription'{
    storage_id = StorageId
}) ->
    #helper_params_changed_subscription{
        storage_id = StorageId
    };
from_protobuf(#'SubscriptionCancellation'{id = Id}) ->
    #subscription_cancellation{id = integer_to_binary(Id)};

%% OTHER
from_protobuf(undefined) -> undefined.


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#subscription{
    id = Id,
    type = Type
}) ->
    {subscription, #'Subscription'{
        id = binary_to_integer(Id),
        type = to_protobuf(Type)}
    };
to_protobuf(#file_read_subscription{
    counter_threshold = CounterThreshold,
    time_threshold = TimeThreshold
}) ->
    {file_read, #'FileReadSubscription'{
        counter_threshold = CounterThreshold,
        time_threshold = TimeThreshold
    }};
to_protobuf(#file_written_subscription{
    counter_threshold = CounterThreshold,
    time_threshold = TimeThreshold
}) ->
    {file_written, #'FileWrittenSubscription'{
        counter_threshold = CounterThreshold,
        time_threshold = TimeThreshold
    }};
to_protobuf(#file_attr_changed_subscription{
    file_guid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    {file_attr_changed, #'FileAttrChangedSubscription'{
        file_uuid = FileGuid,
        time_threshold = TimeThreshold
    }};
to_protobuf(#replica_status_changed_subscription{
    file_guid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    {replica_status_changed, #'ReplicaStatusChangedSubscription'{
        file_uuid = FileGuid,
        time_threshold = TimeThreshold
    }};
to_protobuf(#file_location_changed_subscription{
    file_guid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    {file_location_changed, #'FileLocationChangedSubscription'{
        file_uuid = FileGuid,
        time_threshold = TimeThreshold
    }};
to_protobuf(#file_perm_changed_subscription{
    file_guid = FileGuid
}) ->
    {file_perm_changed, #'FilePermChangedSubscription'{
        file_uuid = FileGuid
    }};
to_protobuf(#file_removed_subscription{
    file_guid = FileGuid
}) ->
    {file_removed, #'FileRemovedSubscription'{
        file_uuid = FileGuid
    }};
to_protobuf(#file_renamed_subscription{
    file_guid = FileGuid
}) ->
    {file_renamed, #'FileRenamedSubscription'{
        file_uuid = FileGuid
    }};
to_protobuf(#quota_exceeded_subscription{}) ->
    {quota_exceeded, #'QuotaExceededSubscription'{}};
to_protobuf(#helper_params_changed_subscription{
    storage_id = StorageId
}) ->
    {helper_params_changed, #'HelperParamsChangedSubscription'{
        storage_id = StorageId
    }};
to_protobuf(#subscription_cancellation{id = Id}) ->
    {subscription_cancellation, #'SubscriptionCancellation'{
        id = binary_to_integer(Id)}
    };


%% OTHER
to_protobuf(undefined) -> undefined.
