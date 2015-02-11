%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module handles requests directed to http and returns a 301 redirect to https.
%%% @end
%%%--------------------------------------------------------------------
-module(opn_redirect_handler).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").

%% API
-export([init/3, handle/2, terminate/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no state is required
%% @end
%%--------------------------------------------------------------------
-spec init(term(), term(), term()) -> {ok, term(), []}.
init(_Type, Req, _Opts) ->
    {ok, Req, []}.

%%--------------------------------------------------------------------
%% @doc
%% Handles a request returning a HTTP Redirect (301 - Moved permanently).
%% @end
%%--------------------------------------------------------------------
-spec handle(term(), term()) -> {ok, term(), term()}.
handle(Req, State) ->
    {FullHostname, _} = cowboy_req:header(<<"host">>, Req),
    {QS, _} = cowboy_req:qs(Req),
    % Remove the leading 'www.' if present
    Hostname = case FullHostname of
                   <<"www.", Rest/binary>> -> Rest;
                   _ -> FullHostname
               end,
    {Path, _} = cowboy_req:path(Req),
    {ok, Req2} = opn_cowboy_bridge:apply(cowboy_req, reply, [
        301,
        [
            {<<"location">>, <<"https://", Hostname/binary, Path/binary, "?", QS/binary>>},
            {<<"content-type">>, <<"text/html">>}
        ],
        <<"">>,
        Req
    ]),
    {ok, Req2, State}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no cleanup needed
%%--------------------------------------------------------------------
-spec terminate(term(), term(), term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.