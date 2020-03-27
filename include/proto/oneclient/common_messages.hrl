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

-include_lib("ctool/include/errors.hrl").

-record(status, {
    code :: code(),
    description :: undefined | binary()
}).

-record(file_block, {
    offset :: non_neg_integer(),
    size :: integer()
}).

-record(file_renamed_entry, {
    old_guid :: fslogic_worker:file_guid(),
    new_guid :: fslogic_worker:file_guid(),
    new_parent_guid :: fslogic_worker:file_guid(),
    new_name :: file_meta:name()
}).

-record(dir, {
    guid :: fslogic_worker:file_guid()
}).

-record(ip_and_port, {
    ip:: inet:ip4_address(),
    port :: 0..65535
}).

-endif.
