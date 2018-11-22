%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module containing functions used to emit monitoring events.
%%% @end
%%%-------------------------------------------------------------------
-module(monitoring_event_emmiter).
-author("Michal Wrzeszcz").

-include("modules/events/definitions.hrl").
-include("modules/monitoring/events.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit_storage_used_updated/3, emit_od_space_updated/1,
    emit_file_read_statistics/4, emit_file_written_statistics/4,
    emit_rtransfer_statistics/3]).

-export_type([type/0]).

-type type() :: #storage_used_updated{} | #od_space_updated{} |
#file_operations_statistics{} | #rtransfer_statistics{}.

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about storage usage update.
%% @end
%%--------------------------------------------------------------------
-spec emit_storage_used_updated(SpaceId :: od_space:id(), UserId :: od_user:id(),
    SizeDifference :: integer()) -> ok | {error, Reason :: term()}.
emit_storage_used_updated(SpaceId, UserId, SizeDifference) ->
    Type = #storage_used_updated{
        space_id = SpaceId,
        size_difference = SizeDifference
    },
    emit(#monitoring_event{type = Type#storage_used_updated{user_id = undefined}}),
    case UserId of
        ?ROOT_USER_ID -> ok;
        ?GUEST_USER_ID -> ok; % todo store guest statistics
        _ ->
            emit(#monitoring_event{type = Type#storage_used_updated{user_id = UserId}})
    end.


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about space info update.
%% @end
%%--------------------------------------------------------------------
-spec emit_od_space_updated(SpaceId :: od_space:id()) -> ok | {error, Reason :: term()}.
emit_od_space_updated(SpaceId) ->
    emit(#monitoring_event{type = #od_space_updated{space_id = SpaceId}}).


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about file operations statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_operations_statistics(SpaceId :: od_space:id(), UserId :: od_user:id(),
    DataAccessRead :: non_neg_integer(), BlockAccessRead :: non_neg_integer(),
    DataAccessWrite :: non_neg_integer(), BlockAccessWrite :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
emit_file_operations_statistics(SpaceId, UserId, DataAccessRead, DataAccessWrite,
    BlockAccessRead, BlockAccessWrite) ->
    case UserId of
        ?ROOT_USER_ID -> ok;
        ?GUEST_USER_ID -> ok;
        _ ->
            Type = #file_operations_statistics{
                space_id = SpaceId,
                data_access_read = DataAccessRead,
                data_access_write = DataAccessWrite,
                block_access_read = BlockAccessRead,
                block_access_write = BlockAccessWrite
            },
            emit(#monitoring_event{type = Type#file_operations_statistics{user_id = undefined}}),
            emit(#monitoring_event{type = Type#file_operations_statistics{user_id = UserId}})
    end.


%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about read operations statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_read_statistics(SpaceId :: od_space:id(), UserId :: od_user:id(),
    DataAccessRead :: non_neg_integer(), BlockAccessRead :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
emit_file_read_statistics(SpaceId, UserId, DataAccessRead, BlockAccessRead) ->
    emit_file_operations_statistics(SpaceId, UserId, DataAccessRead, 0, BlockAccessRead, 0).

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about write operations statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_written_statistics(SpaceId :: od_space:id(), UserId :: od_user:id(),
    DataAccessWrite :: non_neg_integer(), BlockAccessWrite :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
emit_file_written_statistics(SpaceId, UserId, DataAccessWrite, BlockAccessWrite) ->
    emit_file_operations_statistics(SpaceId, UserId, 0, DataAccessWrite, 0,
        BlockAccessWrite).

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about rtransfer statistics.
%% @end
%%--------------------------------------------------------------------
-spec emit_rtransfer_statistics(SpaceId :: od_space:id(), UserId :: od_user:id(),
    TransferIn :: non_neg_integer()) -> ok | {error, Reason :: term()}.
emit_rtransfer_statistics(SpaceId, UserId, TransferIn) ->
    Type = #rtransfer_statistics{
        space_id = SpaceId,
        transfer_in = TransferIn
    },
    emit(#monitoring_event{type = Type#rtransfer_statistics{user_id = undefined}}),
    case UserId of
        ?GUEST_USER_ID -> ok;
        ?ROOT_USER_ID -> ok;
        _ ->
            emit(#monitoring_event{type = Type#rtransfer_statistics{user_id = UserId}})
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Emits event using the root session.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: event:type()) -> ok | {error, Reason :: term()}.
emit(Evt) ->
    event:emit(Evt, ?ROOT_SESS_ID).