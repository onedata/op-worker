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
    seq,
    doc,
    model
}).

-record(seq_range, {
    since,
    until
}).

-record(batch, {
    changes = [],
    since,
    until
}).


-record(queue, {
    key,
    since = 0,
    batch_map = #{},
    last_send,
    removed = false
}).