%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Credentials used to (together with context such as peer ip or
%%% interface hr is connected on) create token_auth necessary to
%%% verify user identity.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(CREDENTIALS_HRL).
-define(CREDENTIALS_HRL, 1).

-record(credentials, {
    access_token :: tokens:serialized(),
    audience_token = undefined :: undefined | tokens:serialized()
}).

-endif.
