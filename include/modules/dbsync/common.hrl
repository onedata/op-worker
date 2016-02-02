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
-author("Rafal Slota").

-record(change, {
    seq :: non_neg_integer(),
    doc :: datastore:document(),
    model :: model_behaviour:model_type()
}).

-record(batch, {
    changes = [] :: [dbsync_worker:change()],
    since :: non_neg_integer(),
    until :: non_neg_integer()
}).
