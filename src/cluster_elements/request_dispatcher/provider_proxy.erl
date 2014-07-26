%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jul 2014 03:07
%%%-------------------------------------------------------------------
-module(provider_proxy).
-author("RoXeon").

%% API
-export([communicate/4]).

communicate({ProviderId, [URL | _]}, AccessToken, FuseId, Message) ->
    ok.
