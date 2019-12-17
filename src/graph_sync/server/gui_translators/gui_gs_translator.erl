%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gs_translator_behaviour and is used to translate
%%% Graph Sync request results into format understood by GUI client.
%%% @end
%%%-------------------------------------------------------------------
-module(gui_gs_translator).
-author("Bartosz Walkowicz").

-behaviour(gs_translator_behaviour).

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([handshake_attributes/1, translate_value/3, translate_resource/3]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_translator_behaviour} callback handshake_attributes/1.
%% @end
%%--------------------------------------------------------------------
-spec handshake_attributes(aai:auth()) ->
    gs_protocol:handshake_attributes().
handshake_attributes(_Client) ->
    {ok, ProviderName} = provider_logic:get_name(),

    #{
        <<"providerName">> => ProviderName,
        <<"serviceVersion">> => oneprovider:get_version(),
        <<"onezoneUrl">> => oneprovider:get_oz_url(),
        <<"transfersHistoryLimitPerFile">> => transferred_file:get_history_limit()
    }.


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_translator_behaviour} callback translate_value/3.
%% @end
%%--------------------------------------------------------------------
-spec translate_value(gs_protocol:protocol_version(), gri:gri(),
    Value :: term()) -> no_return().
translate_value(_, #gri{type = op_provider} = GRI, Value) ->
    provider_gui_gs_translator:translate_value(GRI, Value);
translate_value(_, #gri{type = op_space} = GRI, Value) ->
    space_gui_gs_translator:translate_value(GRI, Value);
translate_value(_, #gri{type = op_file} = GRI, Value) ->
    file_gui_gs_translator:translate_value(GRI, Value);
translate_value(_, #gri{type = op_replica} = GRI, Value) ->
    replica_gui_gs_translator:translate_value(GRI, Value);
translate_value(_, #gri{type = op_share} = GRI, Value) ->
    share_gui_gs_translator:translate_value(GRI, Value);
translate_value(_, #gri{type = op_transfer} = GRI, Value) ->
    transfer_gui_gs_translator:translate_value(GRI, Value);
translate_value(ProtocolVersion, GRI, Data) ->
    ?error("Cannot translate graph sync create result for:~n"
           "ProtocolVersion: ~p~n"
           "GRI: ~p~n"
           "Data: ~p~n", [
        ProtocolVersion, GRI, Data
    ]),
    throw(?ERROR_INTERNAL_SERVER_ERROR).


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_translator_behaviour} callback translate_resource/3.
%% @end
%%--------------------------------------------------------------------
-spec translate_resource(gs_protocol:protocol_version(), gri:gri(),
    ResourceData :: term()) -> Result | fun((aai:auth()) -> Result) when
    Result :: gs_protocol:data() | errors:error() | no_return().
translate_resource(_, #gri{type = op_provider} = GRI, Data) ->
    provider_gui_gs_translator:translate_resource(GRI, Data);
translate_resource(_, #gri{type = op_space} = GRI, Data) ->
    space_gui_gs_translator:translate_resource(GRI, Data);
translate_resource(_, #gri{type = op_user} = GRI, Data) ->
    user_gui_gs_translator:translate_resource(GRI, Data);
translate_resource(_, #gri{type = op_group} = GRI, Data) ->
    group_gui_gs_translator:translate_resource(GRI, Data);
translate_resource(_, #gri{type = op_file} = GRI, Data) ->
    file_gui_gs_translator:translate_resource(GRI, Data);
translate_resource(_, #gri{type = op_replica} = GRI, Data) ->
    replica_gui_gs_translator:translate_resource(GRI, Data);
translate_resource(_, #gri{type = op_share} = GRI, Data) ->
    share_gui_gs_translator:translate_resource(GRI, Data);
translate_resource(_, #gri{type = op_transfer} = GRI, Data) ->
    transfer_gui_gs_translator:translate_resource(GRI, Data);
translate_resource(ProtocolVersion, GRI, Data) ->
    ?error("Cannot translate graph sync get result for:~n"
           "ProtocolVersion: ~p~n"
           "GRI: ~p~n"
           "Data: ~p~n", [
        ProtocolVersion, GRI, Data
    ]),
    throw(?ERROR_INTERNAL_SERVER_ERROR).
