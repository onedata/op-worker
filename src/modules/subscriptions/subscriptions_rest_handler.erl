%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% @end
%%%--------------------------------------------------------------------
-module(subscriptions_rest_handler).
-behaviour(protocol_plugin_behaviour).
-author("Michal Zmuda").

%% API
-export([routes/0]).
-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_accepted/2, delete_resource/2, resource_exists/2]).

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
    {[<<"POST">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {boolean(), req(), #{}}.
is_authorized(Req, State) ->
    {true, Req, State}.

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

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:delete_resource/2
%%--------------------------------------------------------------------
-spec delete_resource(req(), #{}) -> {term(), req(), #{}}.
delete_resource(Req, State) ->
    ?error("QQ ~p ~p", [Req, State]),
    {true, Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with "application/json" content-type
%% @end
%%--------------------------------------------------------------------
-spec handle_json_data(req(), #{}) -> {term(), req(), #{}}.
handle_json_data(Req, State) ->
    {ok, Body, Req2} = cowboy_req:body(Req),
    Json = json_utils:decode(Body),
    ?error("BODY ~p", [Json]),%% todo handle
    {true, Req2, State}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns routes to rest protocol.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{Route :: string(), protocol_plugin_behaviour:handler()}].
routes() ->
    [
        {"/updates/[...]", #{handler => ?MODULE}}
    ].


%%%===================================================================
%%% Internal functions
%%%===================================================================