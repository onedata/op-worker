%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common includes, defines and macros for rtransfer modules.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(RTRANSFER_HRL).
-define(RTRANSFER_HRL, true).

-record(request_transfer, {
    file_id :: binary(),
    offset :: non_neg_integer(),
    size :: pos_integer(),
    provider_id :: binary(),
    notify :: undefined | rtransfer:notify_fun(),
    on_complete :: undefined | rtransfer:on_complete_fun()
}).

-endif.
