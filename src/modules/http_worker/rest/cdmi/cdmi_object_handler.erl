%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This is a cowboy handler module, implementing cowboy_rest interface.
%%% It handles cdmi object PUT, GET and DELETE requests
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_object_handler).
-author("Tomasz Lichon").

-include("modules/http_worker/http_common.hrl").

%% the state of request, it is created in rest_init function,
%% and passed to every cowboy callback functions
-record(state, {}).

%% API
-export([rest_init/2, terminate/3, resource_exists/2, malformed_request/2,
    allowed_methods/2, content_types_provided/2, content_types_accepted/2,
    delete_resource/2]).

%% Content type routing functions
-export([get/2, put/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #state{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #state{}) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #state{} | {error, term()}) -> {[binary()], req(), #state{}}.
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:malformed_request/2
%%--------------------------------------------------------------------
-spec malformed_request(req(), #state{}) -> {boolean(), req(), #state{}}.
malformed_request(Req, State) ->
    {false, Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
resource_exists(Req, State) ->
    {false, Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #state{}) ->
    {[{binary(), atom()}], req(), #state{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-object">>, get}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #state{}) ->
    {[{binary(), atom()}], req(), #state{}}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-object">>, put}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:delete_resource/2
%%--------------------------------------------------------------------
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
delete_resource(Req, State) ->
    {true, Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles GET with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec get(req(), #state{}) -> {term(), req(), #state{}}.
get(Req, State) ->
    {<<"ok">>, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec put(req(), #state{}) -> {term(), req(), #state{}}.
put(Req, State) ->
    {true, Req, State}.
