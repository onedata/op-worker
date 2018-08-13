%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains macros defining paths to dynamic GUI pages.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(GUI_PATHS_HRL).
-define(GUI_PATHS_HRL, 1).

%% Endpoints used to get Onezone or Oneprovider version
-define(PROVIDER_VERSION_PATH, "/version").
-define(ZONE_VERSION_PATH, "/version").

%% Endpoints used to get current configuration of services
-define(PROVIDER_CONFIGURATION_PATH, "/configuration").
-define(ZONE_CONFIGURATION_PATH, "/configuration").

% Endpoint for logging in
-define(LOGIN_PATH, "/login.html").

% Endpoint for logging out
-define(LOGOUT_PATH, "/logout.html").

% Endpoint for logging in via Onezone
-define(VALIDATE_LOGIN_PATH, "/onezone-login/consume").
-define(VALIDATE_LOGIN_PATH_DEPRECATED, "/validate_login.html").

% Endpoint for nagios healthcheck
-define(NAGIOS_PATH, "/nagios").

% Endpoint for nagios healthcheck
-define(NAGIOS_OZ_CONNECTIVITY_PATH, "/nagios/oz_connectivity").

% Endpoint for uploading files
-define(FILE_UPLOAD_PATH, "/upload").

% Endpoint for downloading files
-define(FILE_DOWNLOAD_PATH, "/download").

%% Endpoint used to get provider's identity macaroon
-define(IDENTITY_MACAROON_PATH, "/get_identity_macaroon").

%% Endpoint used to verify authorization nonces issued by this provider
-define(NONCE_VERIFY_PATH, "/verify_authorization_nonce").

%% Endpoint used to connect as a client to protocol endpoint
-define(CLIENT_PROTOCOL_PATH, "/clproto").

%% Protocol name for HTTP upgrade headers on client protocol endpoint
-define(CLIENT_PROTOCOL_UPGRADE_NAME, "clproto").

-endif.



