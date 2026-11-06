%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Credentials used to authenticate a user together with context
%%% information such as peer IP or interface (see auth_manager).
%%% @end
%%%-------------------------------------------------------------------

-ifndef(CREDENTIALS_HRL).
-define(CREDENTIALS_HRL, 1).

-record(client_tokens, {
    access_token :: tokens:serialized(),
    consumer_token = undefined :: undefined | tokens:serialized()
}).

-endif.
