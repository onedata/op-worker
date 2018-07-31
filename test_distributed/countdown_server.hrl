%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------


-define(COUNTDOWN_SERVER(Node),
    binary_to_atom(<<"countdown_server_", (atom_to_binary(Node, latin1))/binary>>, latin1)
).

-define(COUNTDOWN_FINISHED, countdown_finished).