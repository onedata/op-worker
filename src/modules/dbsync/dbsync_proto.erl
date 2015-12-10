%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_proto).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([send_batch/2]).

%%%===================================================================
%%% API
%%%===================================================================


send_batch(global, BatchToSend) ->
    ?info("[ DBSync ] Sending batch to all providers: ~p", [BatchToSend]),
    ok;
send_batch({provider, Provider, _}, BatchToSend) ->
    ?info("[ DBSync ] Sending batch to provider ~p: ~p", [Provider, BatchToSend]),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================