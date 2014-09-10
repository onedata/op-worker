%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module implements rest_module_behaviour and provides tokens that can
%% be attached as "X-Auth-Token" header to rest requests, replacing standard certificate
%% based authentication. To generate token user must obtain one-shot code from globalregistry,
%% and send it with his token request.
%% @end
%% ===================================================================
-module(rest_token).
-behaviour(rest_module_behaviour).

%% Includes
-include_lib("ctool/include/global_registry/gr_openid.hrl").
-include("err.hrl").
-include("veil_modules/control_panel/common.hrl").

%% API
-export([methods_and_versions_info/1, exists/3]).
-export([get/3, delete/3, post/4, put/4]).

%% methods_and_versions_info/1
%% ====================================================================
%% @doc Should return list of tuples, where each tuple consists of version of API version and
%% list of methods available in the API version. Latest version must be at the end of list.
%% e.g.: `[{<<"1.0">>, [<<"GET">>, <<"POST">>]}]'
%% @end
-spec methods_and_versions_info(req()) -> {[{binary(), [binary()]}], req()}.
%% ====================================================================
methods_and_versions_info(Req) ->
    {[{<<"1.0">>, [<<"POST">>]}], Req}.

%% exists/3
%% ====================================================================
%% @doc Should return whether resource specified by given ID exists.
%% Will be called for GET, PUT and DELETE when ID is contained in the URL.
%% @end
-spec exists(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
exists(Req, _Version, _Id) ->
    {true,Req}.

%% get/3
%% ====================================================================
%% @doc Will be called for GET requests. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec get(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
get(Req, <<"1.0">>, _Id) ->
    {error,Req}.

%% delete/3
%% ====================================================================
%% @doc Will be called for DELETE request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec delete(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
delete(Req, <<"1.0">>, _Id) ->
    {error, Req}.

%% post/4
%% ====================================================================
%% @doc Will be called for POST request. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec post(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
post(_Req, <<"1.0">>, _Id, Data) ->
    AccessCode = proplists:get_value(<<"authorizationCode">>, Data),
    case gr_openid:get_token_response(client,[{<<"grant_type">>, <<"authorization_code">>}, {<<"code">>, AccessCode}]) of
        {ok, #token_response{access_token = AccessToken, id_token = #id_token{sub = GRUID}}} ->
            {body, [{<<"accessToken">>, base64:encode(<<AccessToken/binary,";",GRUID/binary>>)}]};
        {error,Reason} ->
            ?warning("Token generation failed with reason: ~p", [Reason]),
            {error, <<"Cannot take token from Global Registry">>}
    end.

%% put/4
%% ====================================================================
%% @doc Will be called for PUT request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec put(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
put(Req, <<"1.0">>, _Id, _Data) ->
    {error, Req}.
