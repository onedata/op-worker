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
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

% the state of request, it is created in rest_init function, and passed to every cowboy callback functions
-record(state, {
    identity :: #identity{}
}).

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
%% @doc @equiv pre_handler:is_authorized/2
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
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #state{}) -> {[{binary(), atom()}], req(), #state{}}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, handle_json_data}
    ], Req, State}.



%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec resource_exists(req(), #state{}) -> {term(), req(), #state{}}.
resource_exists(Req, State) ->
    {false, Req, State}.

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
%% Handles PUT with "application/json" content-type
%% @end
%%--------------------------------------------------------------------
-spec handle_json_data(req(), #state{}) -> {term(), req(), #state{}}.
handle_json_data(Req, State = #state{identity = ?GLOBALREGISTRY_IDENTITY}) ->
    case cowboy_req:path_info(Req) of
        {[<<"auth">>], _}  ->
            {ok, Body, Req2} = cowboy_req:body(Req),
            Json = jiffy:decode(Body),
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