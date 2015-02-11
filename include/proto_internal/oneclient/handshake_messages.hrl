%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal version of protocol handshake messages.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HANDSHAKE_MESSAGES_HRL).
-define(HANDSHAKE_MESSAGES_HRL, 1).

-include("common_messages.hrl").
-include("cluster_elements/protocol_handler/credentials.hrl").

-record(token, {
    value :: binary()
}).

-record(certificate, {
    client_session_id :: binary(),
    client_subject_dn :: binary()
}).

-record(handshake_request, {
    auth_method :: #token{} | #certificate{},
    session_id :: session_id()
}).

-record(handshake_response, {
    session_id :: binary()
}).

-endif.
