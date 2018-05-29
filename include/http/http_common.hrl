%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for modules regarding HTTP servers.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(HTTP_COMMON_HRL).
-define(HTTP_COMMON_HRL, 1).

%% Alias for cowboy_req
-type req() :: cowboy_req:req().

%% ETS that holds ETS sub caches dedicated for LS results caching.
%% There is one such ETS per node.
-define(LS_CACHE_ETS, ls_cache).
%% ETS that holds actual LS results, one per websocket connection.
%% These ETS are not named, so they all can share the same name.
%% LS_CACHE holds the mapping PID -> LS_SUB_CACHE.
-define(LS_SUB_CACHE_ETS, ls_sub_cache).

-endif.