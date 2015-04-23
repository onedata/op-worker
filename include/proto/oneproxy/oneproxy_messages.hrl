%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% internal records for oneproxy messages
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ONEPROXY_MESSAGES_HRL).
-define(ONEPROXY_MESSAGES_HRL, 1).

-record(certificate_info, {
    client_subject_dn :: binary(),
    client_session_id :: binary()
}).

-endif.