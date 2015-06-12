%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This is a cowboy handler module, implementing cowboy_rest interface.
%%% It handles REST requests by routing them to proper rest module.
%%% @end
%%%--------------------------------------------------------------------
-module(rest_handler).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("modules/http_worker/http_common.hrl").
-include("modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

% the state of request, it is created in rest_init function, and passed to every cowboy callback functions
-record(state, {
    identity :: #identity{}
}).

%% API
-export([init/3, terminate/3, rest_init/2, allowed_methods/2, malformed_request/2,
    is_authorized/2, resource_exists/2, content_types_provided/2, content_types_accepted/2, delete_resource/2]).

%% Content type routing functions
-export([get_resource/2, handle_json_data/2]).

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
%% Return whether the user is authorized to perform the action.
%% This function should be used to perform any necessary authentication of the
%% user before attempting to perform any action on the resource.
%% If the authentication fails, the value returned will be sent as the value for
%% the www-authenticate header in the 401 Unauthorized response.
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(req(), #state{}) -> {boolean(), req(), #state{}}.
is_authorized(Req, State) ->
    case rest_auth:authenticate(Req) of
        {{ok, Iden}, NewReq} ->
            {true, NewReq, State#state{identity = Iden}};
        {{error, {not_found, _}}, NewReq} ->
            GrUrl = gr_plugin:get_gr_url(),
            ProviderId = oneprovider:get_provider_id(),
            {_, NewReq2} = cowboy_req:host(NewReq),
            {<<"http://", Url/binary>>, NewReq3} = cowboy_req:url(NewReq2),

            {ok, NewReq4} = cowboy_req:reply(
                307,
                [
                    {<<"location">>, <<(list_to_binary(GrUrl))/binary,
                        "/user/providers/", ProviderId/binary, "/auth_proxy?ref=https://", Url/binary>>}
                ],
                <<"">>,
                NewReq3
            ),
            {halt, NewReq4, State};
        {{error, Error}, NewReq} ->
            ?debug("Authentication error ~p", [Error]),
            {{false, <<"authentication_error">>}, NewReq, State}
    end.

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
-spec content_types_provided(req(), #state{}) -> {[{binary(), atom()}], req(), #state{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-container">>, get_resource}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc Cowboy callback function
%% Returns content-types that are accepted by REST handler and what
%% functions should be used to process the requests.
%% @end
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #state{}) -> {[{binary(), atom()}], req(), #state{}}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, handle_json_data}
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
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Handles GET with "application/json" content-type
%% @end
%%--------------------------------------------------------------------
-spec get_resource(req(), #state{}) -> {term(), req(), #state{}}.
get_resource(Req, State) ->
    {<<"ok">>, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with "application/json" content-type
%% @end
%%--------------------------------------------------------------------
-spec handle_json_data(req(), #state{}) -> {term(), req(), #state{}}.
handle_json_data(Req, State = #state{identity = ?GLOBALREGISTRY_IDENTITY}) ->
    case cowboy_req:path_info(Req) of
        {[<<"auth">>], _}  ->
            {ok, Body, Req2} = cowboy_req:body(Req),
            Json = mochijson2:decode(Body, [{format, proplist}]),
            User = proplists:get_value(<<"user">>, Json),
            Cert = proplists:get_value(<<"cert">>, Json),

            UserId = proplists:get_value(<<"userId">>, User),
            UserName = proplists:get_value(<<"name">>, User),

            case lists:any(fun(X) -> X =:= undefined end, [UserId, UserName, Cert]) of
                true -> {false, Req2, State};
                false ->
                    {ok, _} = onedata_user:save(#document{key = UserId, value = #onedata_user{name = UserName}}),
                    [{'Certificate', DerCert, _}] = public_key:pem_decode(Cert),
                    OtpCert = public_key:pkix_decode_cert(DerCert, otp),
                    {ok, _} = identity:save(#document{key = OtpCert, value = #identity{user_id = UserId}}),
                    {true, Req2, State}
            end
    end;
handle_json_data(Req, State) ->
    {ok, Req2} = cowboy_req:reply(401, [], <<"">>, Req),
    {halt, Req2, State}.