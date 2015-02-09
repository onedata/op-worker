%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------

-ifndef(SERVER_MESSAGES_HRL).
-define(SERVER_MESSAGES_HRL, 1).

-record(server_message, {
    response_id :: integer(),
    seq_num :: integer(),
    last_message :: boolean(),
    server_message :: tuple()
}).

-endif.
