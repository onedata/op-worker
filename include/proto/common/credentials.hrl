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

-include_lib("public_key/include/public_key.hrl").

% Record containing macaroons for user authorization in OZ.
-record(macaroon_auth, {
    macaroon = <<"">> :: binary(),
    disch_macaroons = [] :: [binary()]
}).

% Record containing a (generic) token for user authorization in OZ.
-record(token_auth, {
    token = <<"">> :: binary()
}).

% Record containing HTTP basic auth headers for user authorization in OZ.
-record(basic_auth, {
    % Credentials are in form "Basic base64(user:password)"
    credentials = <<"">> :: binary()
}).

-record(certificate_auth, {
    otp_cert :: #'OTPCertificate'{},
    chain = [] :: [#'OTPCertificate'{}]
}).

-endif.
