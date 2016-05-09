%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for file_distribution allowing to check and change data distribution
%%% in file replicas.
%%% @end
%%%--------------------------------------------------------------------
-module(file_distribution_handler).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_accepted/2, resource_exists/2]).

%% Content type routing functions
-export([handle_json_data/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #{} | {error, term()}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {true | {false, binary()} | halt, req(), #{}}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #{}) -> {[{binary(), atom()}], req(), #{}}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, handle_json_data}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec resource_exists(req(), #{}) -> {term(), req(), #{}}.
resource_exists(Req, State) ->
    {false, Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with "application/json" content-type
%% @end
%%--------------------------------------------------------------------
-spec handle_json_data(req(), #{}) -> {term(), req(), #{}}.
handle_json_data(Req, State = #{auth := _Auth}) ->
    {true, Req, State}.
