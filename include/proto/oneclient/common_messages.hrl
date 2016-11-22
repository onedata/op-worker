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

-include_lib("ctool/include/posix/errors.hrl").

-record(status, {
    code :: code(),
    description :: undefined | binary()
}).

-record(file_block, {
    offset :: non_neg_integer(),
    size :: integer(),
    file_id :: undefined | binary(),
    storage_id :: undefined | storage:id()
}).

-record(file_renamed_entry, {
    old_uuid :: fslogic_worker:file_guid(),
    new_uuid :: fslogic_worker:file_guid(),
    new_parent_uuid :: fslogic_worker:file_guid(),
    new_name :: file_meta:name()
}).

-record(dir, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid()
}).

-endif.
