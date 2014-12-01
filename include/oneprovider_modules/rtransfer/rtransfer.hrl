%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc Common includes, defines and macros for rtransfer modules.
%% ===================================================================

-ifndef(RTRANSFER_HRL).
-define(RTRANSFER_HRL, true).

-include_lib("ctool/include/logging.hrl").

-record(request_transfer, {
    file_id :: string(),
    offset :: non_neg_integer(),
    size :: pos_integer(),
    provider_id :: binary(),
    notify :: pid() | atom()
}).

-endif.
