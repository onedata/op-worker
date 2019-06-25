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

%% Endpoints used to get current configuration of services
-define(DEPRECATED_PROVIDER_CONFIGURATION_PATH, "/configuration").
-define(PROVIDER_CONFIGURATION_PATH, "/api/v3/oneprovider/configuration").
-define(DEPRECATED_ZONE_CONFIGURATION_PATH, "/configuration").
-define(ZONE_CONFIGURATION_PATH, "/api/v3/onezone/configuration").

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

% Endpoint for a dummy favicon used to check connectivity in GUI
-define(FAVICON_PATH, "/favicon.ico").

% Endpoint for mocking transfers
% (rtransfer returns immediately without fetching remote blocks)
-define(RTRANSFER_MOCK_PATH, "/api/v3/oneprovider/debug/transfers_mock").

%% Endpoint used to connect as a client to protocol endpoint
-define(CLIENT_PROTOCOL_PATH, "/clproto").

%% Protocol name for HTTP upgrade headers on client protocol endpoint
-define(CLIENT_PROTOCOL_UPGRADE_NAME, "clproto").

% Endpoint for viewing public shares
-define(SHARE_ID_BINDING, share_id).
-define(PUBLIC_SHARE_COWBOY_ROUTE, "/share/:share_id").

-endif.



