%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definition of records used for events routing.
%%% TODO VFS-7458 - Integrate events and caveats.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(OP_WORKER_MODULES_EVENTS_ROUTING_HRL).
-define(OP_WORKER_MODULES_EVENTS_ROUTING_HRL, 1).

%% definition of keys used to find session subscribed for particular event:
%% main_key - key associated with event
%% filter - events for space dirs have to be additionally filtered checking if particular session has access to space
%% additional_keys - if file has hardlink, it is necessary to find subscribers also for them
%%                   they are identified by guid/uuid (uuid for #file_location_changed_event{}, guid for other events)
-record(event_routing_keys, {
    file_ctx :: file_ctx:ctx() | undefined,
    main_key :: subscription_manager:key(),
    filter :: undefined | od_space:id(),
    additional_keys = [] :: [{file_id:file_guid() | file_meta:uuid(), subscription_manager:key()}],

    auth_check_type = attrs :: event_type:auth_check_type()
}).

%% subscribers for particular event:
%% subscribers - main list of subscribers
%% subscribers_for_links - if file has hardlink, it is necessary to find subscribers also for them,
%%                         the field contains list of tuples {guid/uuid of link, subscribers list}
%%                         (uuid for #file_location_changed_event{}, guid for other events)
-record(event_subscribers, {
    subscribers = [] :: [session:id()],
    subscribers_for_links = [] :: [{subscription_manager:link_subscription_context(), [session:id()]}]
}).

-endif.
