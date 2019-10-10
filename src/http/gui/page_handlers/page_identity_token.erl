%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when identity token page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_identity_token).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/codes.hrl").

-export([handle/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"GET">>, Req) ->
    case tokens:parse_access_token_header(Req) of
        undefined ->
            throw(?ERROR_BAD_TOKEN);
        PeerAccessToken ->
            case token_logic:verify_identity(PeerAccessToken) of
                {ok, ?SUB(?ONEPROVIDER, ProviderId)} ->
                    {ok, IdentityToken} = provider_auth:get_identity_token(ProviderId),
                    cowboy_req:reply(
                        ?HTTP_200_OK,
                        #{<<"content-type">> => <<"text/plain">>},
                        IdentityToken,
                        Req
                    );
                {ok, _} ->
                    throw(?ERROR_BAD_TOKEN);
                {error, _} = Error ->
                    throw(Error)
            end
    end.
