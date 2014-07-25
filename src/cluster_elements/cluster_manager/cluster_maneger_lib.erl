%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Jul 2014 15:36
%%%-------------------------------------------------------------------
-module(cluster_maneger_lib).
-author("RoXeon").

%% API
-export([get_provider_id/0]).


get_provider_id() ->
    global_registry:provider_request(get, "provider").

