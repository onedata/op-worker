%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Session model definitions
%%% @end
%%%-------------------------------------------------------------------

-ifndef(SESSION_HRL).
-define(SESSION_HRL, 1).

-type session_id() :: binary().

-record(credentials, {
    user_id :: binary()
}).

%% session:
%% credentials - owner credentials
%% connections - list of connections' pids
-record(session,{
    credentials :: #credentials{},
    connections :: list() %todo consider extracting connection to independent record
}).

-endif.
