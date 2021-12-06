%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definition of record used for events routing.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(OP_WORKER_MODULES_EVENTS_ROUTING_HRL).
-define(OP_WORKER_MODULES_EVENTS_ROUTING_HRL, 1).

%% Record used to find session subscribed for particular event:
%% manager_key - key associated with event
%% space_id_filter - events for space dirs have to be additionally filtered checking
%%                   if particular session has access to space
%% auth_check_type - events are filtered to prevent sending events to clients that should not know about
%%                   file existence ; filtering process differs for different event types
-record(event_routing_key, {
    file_ctx :: file_ctx:ctx() | undefined,
    manager_key :: subscription_manager:key(),
    space_id_filter :: undefined | od_space:id(),
    auth_check_type = attrs :: event_type:auth_check_type()
}).

-endif.