%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Client credentials used in protocol_handler to extend client_message
%%% @end
%%%-------------------------------------------------------------------

-ifndef(CREDENTIALS_HRL).
-define(CREDENTIALS_HRL, 1).

-export_type([fuse_id/0]).

-type fuse_id() :: binary().

-record(credentials, {
    fuse_id = <<"ID">> :: fuse_id()
}).

-endif.
