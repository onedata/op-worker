%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model responsible for saving information about storage_import scans.
%%%
%%% @TODO VFS-6767 deprecated, included for upgrade procedure. Remove in next major release.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_monitoring).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([get/2, delete/2]).

% export for CT tests
-export([id/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{model => ?MODULE}).

-type id() :: datastore:key().
-type record() :: #storage_sync_monitoring{}.
-type doc() :: datastore_doc:doc(record()).
-type error() :: {error, term()}.


%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Deletes document associated with given SpaceId and StorageId.
%% @end
%%-------------------------------------------------------------------
-spec delete(od_space:id(), storage:id()) -> ok | error().
delete(SpaceId, StorageId) ->
    datastore_model:delete(?CTX, id(SpaceId, StorageId)).

%%-------------------------------------------------------------------
%% @doc
%% Returns
%% @end
%%-------------------------------------------------------------------
-spec get(od_space:id(), storage:id()) -> {ok, doc()}  | error().
get(SpaceId, StorageId) ->
    datastore_model:get(?CTX, id(SpaceId, StorageId)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns id of storage_sync_monitoring document for given SpaceId
%% and StorageId.
%% @end
%%-------------------------------------------------------------------
-spec id(od_space:id(), storage:id()) -> id().
id(SpaceId, StorageId) ->
    datastore_key:build_adjacent(SpaceId, StorageId).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {scans, integer},
        {import_start_time, integer},
        {import_finish_time, integer},
        {last_update_start_time, integer},
        {last_update_finish_time, integer},

        {to_process, integer},
        {imported, integer},
        {updated, integer},
        {deleted, integer},
        {failed, integer},
        {other_processed, integer},

        {imported_sum, integer},
        {updated_sum, integer},
        {deleted_sum, integer},

        {imported_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {imported_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {imported_day_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},

        {updated_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {updated_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {updated_day_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},

        {deleted_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {deleted_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {deleted_day_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},

        {queue_length_min_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {queue_length_hour_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}},
        {queue_length_day_hist, {record, [
            {start_time, integer},
            {last_update_time, integer},
            {time_window, integer},
            {values, [integer]},
            {size, integer},
            {type, atom}
        ]}}
    ]}.