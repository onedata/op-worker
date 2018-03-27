%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for modules regarding http.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(HTTP_COMMON_HRL).
-define(HTTP_COMMON_HRL, 1).

-include("global_definitions.hrl").

%% Includes from cowboy
-type req() :: cowboy_req:req().

%% Endpoint used to get provider's id
-define(provider_id_path, "/get_provider_id").

%% Endpoint used to get provider's identity macaroon
-define(identity_macaroon_path, "/get_identity_macaroon").

%% Endpoint used to verify authorization nonces issued by this provider
-define(nonce_verify_path, "/verify_authorization_nonce").

%% Endpoint used to connect as a client to protocol endpoint
-define(client_protocol_path, "/clproto").

%% Protocol name for HTTP upgrade headers on client protocol endpoint
-define(client_protocol_upgrade_name, "clproto").

%% Endpoints used to get Onezone or Oneprovider version
-define(provider_version_path, "/version").
-define(zone_version_path, "/version").

%% Endpoints used to get current configuration of services
-define(provider_configuration_path, "/configuration").
-define(zone_configuration_path, "/configuration").

-endif.
