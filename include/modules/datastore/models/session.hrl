%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Session model definitions.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(SESSION_HRL).
-define(SESSION_HRL, 1).

-record(credentials, {
    user_id :: binary()
}).

%% session:
%% cred - owner credentials
-record(session, {
    credentials :: #credentials{},
    node :: node(),
    session_sup :: pid(),
    event_manager :: pid(),
    sequencer_manager :: pid(),
    communicator :: pid()
}).

-endif.
