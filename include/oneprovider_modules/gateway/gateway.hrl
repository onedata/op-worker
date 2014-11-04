%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @TODO: write me
%% @end
%% ===================================================================

-ifndef(GATEWAY_HRL).
-define(GATEWAY_HRL, true).

-include("gwproto_pb.hrl").
-include_lib("ctool/include/logging.hrl").

-define(gw_port, 8877).

-record(fetch, {
    request :: #fetchrequest{},
    remote :: inet:ip_address(),
    notify :: pid()
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
