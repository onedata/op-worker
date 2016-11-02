%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common includes, defines and macros for gateway modules.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(GATEWAY_HRL).
-define(GATEWAY_HRL, true).

-include_lib("clproto/include/messages.hrl").

-define(connection_close_timeout, timer:minutes(5)).

%% Description for a `fetch` action commissioned to the gateway module
-record(gw_fetch, {
    file_id :: binary(),
    offset :: non_neg_integer(),
    size :: pos_integer(),
    remote :: {inet:ip_address(), inet:port_number()}, %% IP address and port of remote node that should fulfill the request
    notify :: [pid() | atom()],                        %% a process to be notified of action's results
    retry :: integer()
}).

-define(log_call(Request),
    ?warning("~p: ~p - bad request ~p", [?MODULE, ?LINE, Request])
).

-endif.
