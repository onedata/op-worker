%%%--------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Monitoring events definitions.
%%% @end
%%%--------------------------------------------------------------------
-ifndef(MONITORING_EVENTS_HRL).
-define(MONITORING_EVENTS_HRL, 1).

%% definition of event triggered when storage usage is changed
%% space_id        - ID of space
%% user_id         - ID of user
%% size_difference - size difference of storage usage in bytes since last update
-record(storage_used_updated, {
    space_id :: od_space:id(),
    user_id :: undefined | od_user:id(),
    size_difference :: integer()
}).

%% definition of event triggered when space record has changed
-record(od_space_updated, {
    space_id :: od_space:id()
}).

%% definition of event with read/write statistics
%% space_id           - ID of space
%% user_id            - ID of user
%% data_access_read   - number of read bytes
%% data_access_write  - number of write bytes
%% block_access_write - number of read blocks
%% block_access_read  - number of write blocks
-record(file_operations_statistics, {
    space_id :: od_space:id(),
    user_id :: undefined | od_user:id(),
    data_access_read = 0 :: non_neg_integer(),
    data_access_write = 0 :: non_neg_integer(),
    block_access_read = 0 :: non_neg_integer(),
    block_access_write = 0 :: non_neg_integer()
}).

%% definition of event with rtransfer statistics
%% space_id    - ID of space
%% user_id     - ID of user
%% transfer_in - data replicated to provider in bytes
-record(rtransfer_statistics, {
    space_id :: od_space:id(),
    user_id :: undefined | od_user:id(),
    transfer_in = 0 :: non_neg_integer()
}).

-endif.
