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

-define(gw_port, 8877).

-record(fetch, {
    request :: #fetchrequest{},
    remote :: inet:ip_address(),
    notify :: pid()
}).

-endif.
