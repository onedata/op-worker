%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definition of a top level subscription wrapper and
%%% subsequent subscription types.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(OP_WORKER_MODULES_EVENTS_SUBSCRIPTIONS_HRL).
-define(OP_WORKER_MODULES_EVENTS_SUBSCRIPTIONS_HRL, 1).

-define(FILE_READ_SUB_ID, subscription:generate_id(<<"file_read">>)).
-define(FILE_WRITTEN_SUB_ID, subscription:generate_id(<<"file_written">>)).
-define(MONITORING_SUB_ID, subscription:generate_id(<<"monitoring">>)).

%% definition of a top level subscription wrapper
%% id     - ID of a subscription
%% type   - specific subscription
%% stream - definition of an event stream
-record(subscription, {
    id :: undefined | subscription:id(),
    type :: undefined | subscription:type(),
    stream :: undefined | event:stream()
}).

%% definition of a subscription concerning file changes
%% sessions - set of sessions that are interested in notifications about file changes
-record(file_subscription, {
    sessions = gb_sets:new() :: gb_sets:set()
}).

%% definition of a subscription for read operations in the file system
%% counter_threshold - maximal number of aggregated events before emission
%% time_threshold    - maximal delay in milliseconds between successive events
%%                     emissions
-record(file_read_subscription, {
    counter_threshold :: undefined | non_neg_integer(),
    time_threshold :: undefined | non_neg_integer()
}).

%% definition of a subscription for write operations in the file system
%% counter_threshold - maximal number of aggregated events before emission
%% time_threshold    - maximal delay in milliseconds between successive events
%%                     emissions
-record(file_written_subscription, {
    counter_threshold :: undefined | non_neg_integer(),
    time_threshold :: undefined | non_neg_integer()
}).

%% definition of a subscription for file attributes changes
%% file_uuid         - UUID of a file for which notifications should be sent
%% counter_threshold - maximal number of aggregated events before emission
%% time_threshold    - maximal delay in milliseconds between successive events
%%                     emissions
-record(file_attr_changed_subscription, {
    file_uuid :: file_meta:uuid(),
    counter_threshold :: undefined | non_neg_integer(),
    time_threshold :: undefined | non_neg_integer()
}).

%% definition of a subscription for file location changes
%% file_uuid         - UUID of a file for which notifications should be sent
%% counter_threshold - maximal number of aggregated events before emission
%% time_threshold    - maximal delay in milliseconds between successive events
%%                     emissions
-record(file_location_changed_subscription, {
    file_uuid :: file_meta:uuid(),
    counter_threshold :: undefined | non_neg_integer(),
    time_threshold :: undefined | non_neg_integer()
}).

%% definition of a subscription for permission changes
%% file_uuid         - UUID of a file for which notifications should be sent
-record(file_perm_changed_subscription, {
    file_uuid :: file_meta:uuid()
}).

%% definition of a subscription for file removal
%% file_uuid         - UUID of a file for which notifications should be sent
-record(file_removed_subscription, {
    file_uuid :: file_meta:uuid()
}).

%% definition of a subscription for quota watcher
-record(quota_exceeded_subscription, {
}).

%% definition of a subscription for file renaming
%% file_uuid         - UUID of a file for which notifications should be sent
-record(file_renamed_subscription, {
    file_uuid :: file_meta:uuid()
}).

%% definition of a subscription for monitoring events
%% time_threshold - maximal delay in milliseconds between successive events
%%                  emissions
-record(monitoring_subscription, {
    time_threshold :: non_neg_integer()
}).

%% definition of an subscription cancellation
%% id - ID of a subscription to be cancelled
-record(subscription_cancellation, {
    id :: subscription:id()
}).

-endif.
