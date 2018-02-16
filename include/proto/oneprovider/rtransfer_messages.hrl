%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Protocol messages for rtransfer.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(RTRANSFER_MESSAGES_HRL).
-define(RTRANSFER_MESSAGES_HRL, 1).

-record(generate_rtransfer_conn_secret, {
}).

-record(rtransfer_conn_secret, {
    secret :: binary()
}).

-endif.