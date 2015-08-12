%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This is a cowboy handler module, implementing cowboy_rest interface.
%%% It handles cdmi object/container PUT, GET and DELETE requests
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_handler).
-author("Tomasz Lichon").

-include("modules/http_worker/http_common.hrl").

%% the state of request, it is created in rest_init function,
%% and passed to every cowboy callback functions
-record(state, {}).

%% API
-export([init/3, terminate/3, rest_init/2, resource_exists/2, malformed_request/2,
    allowed_methods/2, content_types_provided/2, content_types_accepted/2,
    delete_resource/2]).

%% Content type routing functions
-export([get_cdmi_container/2, put_cdmi_container/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Imposes a cowboy upgrade protocol to cowboy_rest - this module is
%% now treated as REST module by cowboy.
%% @end
%%--------------------------------------------------------------------
-spec init(term(), term(), term()) -> {upgrade, protocol, cowboy_rest, req(), term()}.
init(_, Req, Opts) ->
    {upgrade, protocol, cowboy_rest, Req, Opts}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Handles cleanup
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #state{}) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Called right after protocol upgrade to init the request context.
%% Will shut down the connection if the peer doesn't provide a valid
%% proxy certificate.
%% @end
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #state{}}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns methods that are allowed.
%% @end
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #state{} | {error, term()}) -> {[binary()], req(), #state{}}.
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Checks if request contains all mandatory fields and their values are set properly
%% depending on requested operation
%% @end
%%--------------------------------------------------------------------
-spec malformed_request(req(), #state{}) -> {boolean(), req(), #state{}}.
malformed_request(Req, State) ->
    {false, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Determines if resource identified by Filepath exists.
%% @end
%%--------------------------------------------------------------------
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
resource_exists(Req, State) ->
    {false, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Returns content types that can be provided.
%% @end
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #state{}) ->
    {[{binary(), atom()}], req(), #state{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-container">>, get_cdmi_container}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc Cowboy callback function
%% Returns content-types that are accepted by REST handler and what
%% functions should be used to process the requests.
%% @end
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #state{}) ->
    {[{binary(), atom()}], req(), #state{}}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-container">>, put_cdmi_container}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback function
%% Handles DELETE requests.
%% @end
%%--------------------------------------------------------------------
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
delete_resource(Req, State) ->
    {true, Req, State}.

%%%===================================================================
%%% Content type routing functions
%%%===================================================================
%%--------------------------------------------------------------------
%% This functions are needed by cowboy for registration in
%% content_types_accepted/content_types_provided methods and simply delegates
%% their responsibility to adequate handler modules
%%-------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Handles GET with "application/cdmi-container" content-type
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_container(req(), #state{}) -> {term(), req(), #state{}}.
get_cdmi_container(Req, State) ->
    {<<"ok">>, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with "application/cdmi-container" content-type
%% @end
%%--------------------------------------------------------------------
-spec put_cdmi_container(req(), #state{}) -> {term(), req(), #state{}}.
put_cdmi_container(Req, State) ->
    {true, Req, State}.
