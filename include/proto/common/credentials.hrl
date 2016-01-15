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

% Record containing macaroons for user authorization in GR.
% All macaroons are in serialized form.
-record(auth, {
    macaroon :: binary(),
    disch_macaroons = [] :: [binary()]
}).

-record(certificate, {
    otp_cert :: #'OTPCertificate'{},
    chain :: [#'OTPCertificate'{}]
}).

-endif.
