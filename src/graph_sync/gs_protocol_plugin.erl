%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model implements gs_protocol_plugin_behaviour and is called by
%%% gs_protocol to customize message encoding/decoding.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_protocol_plugin).
-behaviour(gs_protocol_plugin_behaviour).
-author("Lukasz Opiola").

-include_lib("ctool/include/api_errors.hrl").

%% API
-export([encode_entity_type/1, decode_entity_type/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link gs_protocol_plugin_behaviour} callback encode_entity_type/1.
%% @end
%%--------------------------------------------------------------------
-spec encode_entity_type(gs_protocol:entity_type()) -> binary().
encode_entity_type(od_user) -> <<"user">>;
encode_entity_type(od_group) -> <<"group">>;
encode_entity_type(od_space) -> <<"space">>;
encode_entity_type(od_share) -> <<"share">>;
encode_entity_type(od_provider) -> <<"provider">>;
encode_entity_type(od_handle_service) -> <<"handleService">>;
encode_entity_type(od_handle) -> <<"handle">>;
encode_entity_type(od_cluster) -> <<"cluster">>;
encode_entity_type(_) -> throw(?ERROR_BAD_TYPE).


%%--------------------------------------------------------------------
%% @doc
%% {@link gs_protocol_plugin_behaviour} callback decode_entity_type/1.
%% @end
%%--------------------------------------------------------------------
-spec decode_entity_type(binary()) -> gs_protocol:entity_type().
decode_entity_type(<<"user">>) -> od_user;
decode_entity_type(<<"group">>) -> od_group;
decode_entity_type(<<"space">>) -> od_space;
decode_entity_type(<<"share">>) -> od_share;
decode_entity_type(<<"provider">>) -> od_provider;
decode_entity_type(<<"handleService">>) -> od_handle_service;
decode_entity_type(<<"handle">>) -> od_handle;
decode_entity_type(<<"cluster">>) -> od_cluster;
decode_entity_type(_) -> throw(?ERROR_BAD_TYPE).
