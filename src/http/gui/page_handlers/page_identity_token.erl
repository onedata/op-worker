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
%%% @todo VFS-5554 Deprecated, included for backward compatibility
%%% Must be supported in the 19.09.* line, later the counterpart in the
%%% configuration endpoint can be used.
%%% @end
%%%-------------------------------------------------------------------
-module(page_identity_token).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").

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
            throw(?ERROR_UNAUTHORIZED);
        PeerAccessToken ->
            case token_logic:verify_provider_identity_token(PeerAccessToken) of
                {ok, ?SUB(?ONEPROVIDER, PeerProviderId)} ->
                    Audience = ?AUD(?OP_WORKER, PeerProviderId),
                    {ok, IdentityToken} = provider_auth:get_identity_token(Audience),
                    cowboy_req:reply(
                        ?HTTP_200_OK,
                        #{?HDR_CONTENT_TYPE => <<"text/plain">>},
                        IdentityToken,
                        Req
                    );
                {ok, _} ->
                    throw(?ERROR_TOKEN_SUBJECT_INVALID);
                {error, _} = Error ->
                    throw(Error)
            end
    end.
