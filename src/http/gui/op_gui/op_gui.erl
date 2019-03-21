%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This is the main module for Oneprovider GUI application.
%%% op_gui:init/0 and op_gui:cleanup/0 should be called from including
%%% application to set up everything that is needed, such as session plugin.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui).
-author("Lukasz Opiola").

-include("http/http_common.hrl").

%% API
-export([init/0, cleanup/0]).


%%--------------------------------------------------------------------
%% @doc
%% Should be called from the application including gui to set up gui modules.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ets:new(?LS_CACHE_ETS, [
        set, public, named_table,
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Should be called from the application including gui to clean up gui modules.
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok.
cleanup() ->
    ets:delete(?LS_CACHE_ETS),
    ok.
