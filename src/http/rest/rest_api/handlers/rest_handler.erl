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
-spec is_authorized(req(), #{}) -> {boolean(), req(), #{}}.
is_authorized(Req, State) ->
    rest_auth:is_authorized(Req, State).

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
handle_json_data(Req, State = #{auth := Auth}) ->
    case Auth =:= session:get_rest_session_id(?OZ_IDENTITY) of
        true ->
            case cowboy_req:path_info(Req) of
                {[<<"auth">>], _}  ->
                    {ok, Body, Req2} = cowboy_req:body(Req),
                    Json = json_utils:decode(Body),
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
        false ->
            {ok, Req2} = cowboy_req:reply(401, [], <<"">>, Req),
            {halt, Req2, State}
    end.
