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

-module(gateway_reply_processor).
-author("Konrad Zemek").

-include("gwproto_pb.hrl").
-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-export([loop/2]).

%% ====================================================================
%% API functions
%% ====================================================================

-spec loop(ConnectionManager :: pid(), Socket :: gen_tcp:socket()) -> ok.
loop(ConnectionManager, Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Packet} ->
            %% @TODO: Implementation
            ?warning("Received packet of size ~p", [size(Packet)]),
            ?MODULE:loop(ConnectionManager, Socket);

        {error, closed} ->
            ok;

        {error, Reason} ->
            ConnectionManager ! {tcp_error, Socket, Reason}
    end,
    ok.
