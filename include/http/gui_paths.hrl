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
-define(ZONE_COMPATIBILITY_REGISTRY_PATH, "/compatibility.json").

% Endpoint for nagios healthcheck
-define(NAGIOS_PATH, "/nagios").

% Endpoint for nagios healthcheck
-define(NAGIOS_OZ_CONNECTIVITY_PATH, "/nagios/oz_connectivity").

% New endpoint for uploading files
-define(FILE_UPLOAD_PATH, "/file_upload").

% Endpoint for downloading files
-define(FILE_DOWNLOAD_PATH, "/download").

%% Endpoint used to get provider's identity token
-define(IDENTITY_TOKEN_PATH, "/identity_token").

%% Endpoint used to connect as a client to protocol endpoint
-define(CLIENT_PROTOCOL_PATH, "/clproto").

% TODO VFS-7628 make openfaas respond to https
%% Endpoint used as callback for Openfaas
-define(ATM_TASK_FINISHED_CALLBACK_PATH, "/tasks/").

%% Protocol name for HTTP upgrade headers on client protocol endpoint
-define(CLIENT_PROTOCOL_UPGRADE_NAME, "clproto").

% Endpoint for viewing public shares
-define(SHARE_ID_BINDING, share_id).
-define(PUBLIC_SHARE_COWBOY_ROUTE, "/share/:share_id").

%% All requests to this endpoint will be proxied to onepanel.
-define(PANEL_REST_PROXY_PATH, "/api/v3/onepanel/").

% Graph Sync websocket endpoint
-define(GUI_GRAPH_SYNC_WS_PATH, "/graph_sync/gui").

% WebSocket endpoint for reporting OpenFaaS activity (e.g. pod status changes or live logs)
-define(OPENFAAS_ACTIVITY_FEED_WS_PATH, "/automation/openfaas/activity-feed").

-endif.
