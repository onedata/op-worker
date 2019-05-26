%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Credentials that can be exchanged for user identity in identity model
%%% @end
%%%-------------------------------------------------------------------

-ifndef(CREDENTIALS_HRL).
-define(CREDENTIALS_HRL, 1).

% Record containing macaroons for user authorization in OZ.
-record(macaroon_auth, {
    macaroon :: binary(),
    disch_macaroons = [] :: [binary()]
}).

-endif.
