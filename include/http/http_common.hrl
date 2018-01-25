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
-define(provider_version_path, "/version").
-define(zone_version_path, "/version").

-endif.
