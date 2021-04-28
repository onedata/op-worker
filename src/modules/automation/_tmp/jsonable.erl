%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO writeme.
%%% @end
%%%-------------------------------------------------------------------
-module(jsonable).
-author("Bartosz Walkowicz").


%%--------------------------------------------------------------------
%% @doc
%% Returns current version of model (will be used to compare and upgrade
%% older versions).
%% @end
%%--------------------------------------------------------------------
-callback version() -> json_serializer:model_version().


%%--------------------------------------------------------------------
%% @doc
%% Encodes model record as json object.
%% @end
%%--------------------------------------------------------------------
-callback to_json(json_serializer:model_record()) -> json_utils:json_map().


%%--------------------------------------------------------------------
%% @doc
%% Decodes model record from json object.
%% @end
%%--------------------------------------------------------------------
-callback from_json(json_utils:json_map()) -> json_serializer:model_record().


%%--------------------------------------------------------------------
%% @doc
%% Upgrades older models (must be implemented if model version > 1).
%% @end
%%--------------------------------------------------------------------
-callback upgrade_json(json_serializer:model_version(), json_utils:json_map()) ->
    {json_serializer:model_version(), json_utils:json_map()}.


-optional_callbacks([upgrade_json/2]).
