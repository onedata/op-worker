%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Internal common definitions for datastore
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_INTERNAL_HRL).
-define(DATASTORE_INTERNAL_HRL, 1).

%% Drivers definitions
-define(PERSISTENCE_DRIVER, riak_datastore_driver).
-define(LOCAL_CACHE_DRIVER, ets_cache_driver).
-define(DISTRIBUTED_CACHE_DRIVER, mnesia_cache_driver).

-define(MODEL_NAME, ?MODULE).

-endif.