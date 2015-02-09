%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal versions of common protocol messages.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(COMMON_MESSAGES_HRL).
-define(COMMON_MESSAGES_HRL, 1).

-record(file_block, {
    offset :: non_neg_integer(),
    size :: non_neg_integer()
}).

-record(environment_variable, {
    name :: binary(),
    value :: binary()
}).

-endif.
