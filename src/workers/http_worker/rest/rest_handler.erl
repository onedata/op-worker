%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This is a cowboy handler module, implementing cowboy_rest interface.
%% It handles REST requests by routing them to proper rest module.
%% @end
%% ===================================================================

-module(rest_handler).

-include("workers/http_worker/rest/rest.hrl").
-include("cluster_elements/oneproxy/oneproxy.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").


%% Callbacks
-export([init/3, rest_init/2, resource_exists/2, malformed_request/2, allowed_methods/2, content_types_provided/2, content_types_accepted/2, delete_resource/2]).
%% Content type routing functions
-export([get_json/2]).
-export([put_json/2]).



%% ====================================================================
%% API functions
%% ====================================================================

%% init/3
%% ====================================================================
%% @doc Cowboy callback function
%% Imposes a cowboy upgrade protocol to cowboy_rest - this module is 
%% now treated as REST module by cowboy.
%% @end
-spec init(any(), any(), any()) -> {upgrade, protocol, cowboy_rest}.
%% ====================================================================
init(_, Req, Opts) ->
    NewOpts =
        case gsi_handler:get_certs_from_req(?ONEPROXY_REST, Req) of
            {ok, {OtpCert, Certs}} ->
                [{certs, {OtpCert, Certs}}];
            {error, _} ->
                []
        end,
    {upgrade, protocol, cowboy_rest, Req, NewOpts ++ Opts}.

%% rest_init/2
%% ====================================================================
%% @doc Cowboy callback function
%% Called right after protocol upgrade to init the request context.
%% Will shut down the connection if the peer doesn't provide a valid
%% proxy certificate.
%% @end
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
%% ====================================================================
rest_init(Req, _Opts) ->
    {ok, Req, #state{}}.

%% allowed_methods/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns methods that are allowed.
%% @end
-spec allowed_methods(req(), #state{} | {error, term()}) -> {[binary()], req(), #state{}}.
%% ====================================================================
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}.

%% malformed_request/2
%% ====================================================================
%% @doc Cowboy callback function
%% Checks if request contains all mandatory fields and their values are set properly
%% depending on requested operation
%% @end
-spec malformed_request(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
malformed_request(Req, State) ->
    {false, Req, State}.

%% resource_exists/2
%% ====================================================================
%% @doc Cowboy callback function
%% Determines if resource identified by Filepath exists.
%% @end
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req, State) ->
    {false, Req, State}.

%% content_types_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content types that can be provided.
%% @end
-spec content_types_provided(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_json}
    ], Req, State}.

%% content_types_accepted/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content-types that are accepted by REST handler and what
%% functions should be used to process the requests.
%% @end
-spec content_types_accepted(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, put_json}
    ], Req, State}.


%% delete_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles DELETE requests.
%% @end
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
delete_resource(Req, State) ->
    {true, Req, State}.

%% ====================================================================
%% Content type routing functions
%% ====================================================================

%% ====================================================================
%% This functions are needed by cowboy for registration in
%% content_types_accepted/content_types_provided methods and simply delegates
%% their responsibility to adequate handler modules
%% ====================================================================
get_json(Req,State) ->
    {<<"ok">>, Req, State}.
put_json(Req,State) ->
    {true, Req, State}.