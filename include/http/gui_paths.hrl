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

% Expands to a centralized endpoint in Onezone used to fetch info and contents
% of any file/directory being a part of any share (by file id) - redirects
% to a REST endpoint in one of the supporting providers.
-define(ZONE_SHARED_DATA_CURL_COMMAND_TEMPLATE(SubPath), [
    <<"curl">>, <<"-L">>, oneprovider:get_oz_url(<<"/api/v3/onezone/shares/data/{id}", SubPath>>)
]).

-define(XROOTD_URI(Domain, Path), str_utils:format_bin("root://~s~s", [Domain, Path])).
-define(XROOTD_DOWNLOAD_SHARED_FILE_COMMAND_TEMPLATE(Domain), [
    <<"xrdcp">>, ?XROOTD_URI(Domain, <<"//data/{spaceId}/{spaceId}/{shareId}{path}">>), <<".">>
]).
-define(XROOTD_DOWNLOAD_SHARED_DIRECTORY_COMMAND_TEMPLATE(Domain), [
    <<"xrdcp">>, <<"-r">>, ?XROOTD_URI(Domain, <<"//data/{spaceId}/{spaceId}/{shareId}{path}">>), <<".">>
]).
-define(XROOTD_LIST_SHARED_DIRECTORY_COMMAND_TEMPLATE(Domain), [
    <<"xrdfs">>, ?XROOTD_URI(Domain, <<"">>), <<"ls">>, <<"/data/{spaceId}/{spaceId}/{shareId}{path}">>
]).

%% All requests to this endpoint will be proxied to onepanel.
-define(PANEL_REST_PROXY_PATH, "/api/v3/onepanel/").

% Graph Sync websocket endpoint
-define(GUI_GRAPH_SYNC_WS_PATH, "/graph_sync/gui").

-endif.
