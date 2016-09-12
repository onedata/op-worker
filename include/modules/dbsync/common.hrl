%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Common records and defines for dbsync module
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DBSYNC_COMMON_HRL).
-define(DBSYNC_COMMON_HRL, 1).

%% Single change from DB.
-record(change, {
    seq :: non_neg_integer(),
    doc :: datastore:document(),
    model :: model_behaviour:model_type()
}).

%% Collection of changes from DB.
-record(batch, {
    since :: undefined | non_neg_integer(),
    until :: non_neg_integer(),
    changes = [] :: [dbsync_worker:change()]
}).

-endif.