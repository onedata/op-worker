%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-ifndef(EVENTS_HRL).
-define(EVENTS_HRL, 1).

-include("read_event.hrl").
-include("write_event.hrl").
-include("proto/oneclient/event_messages.hrl").

-export_type([event/0, event_subscription/0, event_producer/0, event_request/0]).

-type event() :: #read_event{} | #write_event{}.
-type event_subscription() :: #read_event_subscription{}
                            | #write_event_subscription{}.
-type event_producer() :: all_fuse_clients.

-type event_request() :: #'ReadEventSubscription'{}
                       | #'WriteEventSubscription'{}.

-endif.