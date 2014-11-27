%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc Common includes, defines and macros for gateway modules.
%% ===================================================================

-ifndef(GATEWAY_HRL).
-define(GATEWAY_HRL, true).

-include("gwproto_pb.hrl").
-include_lib("ctool/include/logging.hrl").

-define(connection_close_timeout, timer:minutes(1)).

%% Description for a `fetch` action commisioned to the gateway module
-record(gw_fetch, {
    file_id :: string(),
    offset :: non_neg_integer(),
    size :: pos_integer(),
    remote :: {inet:ip_address(), inet:port_number()}, %% IP address and port of remote node that should fulfill the request
    notify :: [pid() | atom()],                        %% a process to be notified of action's results
    retry :: pos_integer()
}).

-define(log_terminate(Reason, State),
    case Reason of
        normal -> ok;
        shutdown -> ok;
        {shutdown, _} -> ok;
        _ -> ?error("~p terminated with ~p (state: ~p)", [?MODULE, Reason, State])
    end
).

-define(log_call(Request),
    ?warning("~p: ~p - bad request ~p", [?MODULE, ?LINE, Request])
).

-endif.
