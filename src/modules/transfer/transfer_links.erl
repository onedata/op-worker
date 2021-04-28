%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Util functions for operating on transfer links trees.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_links).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
% link utils functions
-export([
    add_waiting/1, delete_waiting/1,
    add_ongoing/1, delete_ongoing/1,
    add_ended/1, delete_ended/1,

    move_from_ongoing_to_ended/1,
    move_to_ended_if_not_migration/1
]).
-export([list/5]).
-export([link_key/2]).

-type link_key() :: binary().
-type virtual_list_id() :: binary(). % ?(WAITING|ONGOING|ENDED)_TRANSFERS_KEY
-type offset() :: integer().
-type list_limit() :: transfer:list_limit().

-export_type([link_key/0]).

-define(CTX, (transfer:get_ctx())).

-define(LINK_NAME_ID_PART_LENGTH, 6).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Adds given transfer to waiting transfer links tree and to
%% ongoing transfers for file (transferred_file doc).
%% @end
%%--------------------------------------------------------------------
-spec add_waiting(transfer:doc()) -> ok.
add_waiting(#document{key = TransferId, value = Transfer}) ->
    SpaceId = Transfer#transfer.space_id,
    ScheduleTime = Transfer#transfer.schedule_time,
    case Transfer#transfer.file_uuid of
        undefined ->
            ok;
        FileUuid ->
            FileGuid = file_id:pack_guid(FileUuid, SpaceId),
            transferred_file:report_transfer_start(FileGuid, TransferId, ScheduleTime)
    end,
    ok = add_link(?WAITING_TRANSFERS_KEY, TransferId, SpaceId, ScheduleTime).


-spec add_ongoing(transfer:doc()) -> ok.
add_ongoing(#document{key = TransferId, value = Transfer}) ->
    SpaceId = Transfer#transfer.space_id,
    ScheduleTime = Transfer#transfer.schedule_time,
    ok = add_link(?ONGOING_TRANSFERS_KEY, TransferId, SpaceId, ScheduleTime).


%%--------------------------------------------------------------------
%% @doc
%% Adds given transfer to ended transfer links tree and removes it from
%% ongoing transfers for file (transferred_file doc).
%% @end
%%--------------------------------------------------------------------
-spec add_ended(transfer:doc()) -> ok.
add_ended(#document{key = TransferId, value = Transfer}) ->
    SpaceId = Transfer#transfer.space_id,
    ScheduleTime = Transfer#transfer.schedule_time,
    FinishTime = Transfer#transfer.finish_time,
    case Transfer#transfer.file_uuid of
        undefined ->
            ok;
        FileUuid ->
            FileGuid = file_id:pack_guid(FileUuid, SpaceId),
            transferred_file:report_transfer_finish(FileGuid, TransferId, ScheduleTime, FinishTime)
    end,
    ok = add_link(?ENDED_TRANSFERS_KEY, TransferId, SpaceId, FinishTime).


-spec delete_waiting(transfer:doc()) -> ok.
delete_waiting(#document{key = TransferId, value = Transfer}) ->
    SpaceId = Transfer#transfer.space_id,
    ScheduleTime = Transfer#transfer.schedule_time,
    ok = delete_links(?WAITING_TRANSFERS_KEY, TransferId, SpaceId, ScheduleTime).


-spec delete_ongoing(transfer:doc()) -> ok.
delete_ongoing(#document{key = TransferId, value = Transfer}) ->
    SpaceId = Transfer#transfer.space_id,
    ScheduleTime = Transfer#transfer.schedule_time,
    ok = delete_links(?ONGOING_TRANSFERS_KEY, TransferId, SpaceId, ScheduleTime).


-spec delete_ended(transfer:doc()) -> ok.
delete_ended(#document{key = TransferId, value = Transfer}) ->
    SpaceId = Transfer#transfer.space_id,
    FinishTime = Transfer#transfer.finish_time,
    ok = delete_links(?ENDED_TRANSFERS_KEY, TransferId, SpaceId, FinishTime).


-spec move_from_ongoing_to_ended(transfer:doc()) -> ok.
move_from_ongoing_to_ended(Doc) ->
    add_ended(Doc),
    delete_ongoing(Doc).


%%--------------------------------------------------------------------
%% @doc
%% Moves the transfer link from ongoing to ended links tree if transfer was
%% not migration.
%% @end
%%--------------------------------------------------------------------
-spec move_to_ended_if_not_migration(transfer:doc()) -> ok.
move_to_ended_if_not_migration(Doc = #document{value = Transfer}) ->
    case transfer:is_migration(Transfer) of
        true -> ok;
        false -> move_from_ongoing_to_ended(Doc)
    end.


-spec list(SpaceId :: od_space:id(), virtual_list_id(),
    transfer:id() | undefined, offset(), list_limit()) -> [transfer:id()].
list(SpaceId, ListDocId, StartId, Offset, Limit) ->
    Opts = #{offset => Offset},

    Opts2 = case StartId of
        undefined -> Opts;
        _ -> Opts#{prev_link_name => StartId}
    end,

    Opts3 = case Limit of
        all -> Opts2;
        _ -> Opts2#{size => Limit}
    end,

    {ok, Transfers} = for_each_link(ListDocId, fun(_LinkName, TransferId, Acc) ->
        [TransferId | Acc]
    end, [], SpaceId, Opts3),
    lists:reverse(Transfers).


-spec link_key(transfer:id(), non_neg_integer()) -> link_key().
link_key(TransferId, Timestamp) ->
    TimestampPart = (integer_to_binary(?EPOCH_INFINITY - Timestamp)),
    IdPart = binary:part(TransferId, 0, ?LINK_NAME_ID_PART_LENGTH),
    <<TimestampPart/binary, IdPart/binary>>.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns links tree root for given space.
%% @end
%%-------------------------------------------------------------------
-spec link_root(binary(), od_space:id()) -> binary().
link_root(Prefix, SpaceId) ->
    <<Prefix/binary, "_", SpaceId/binary>>.


%%--------------------------------------------------------------------
%% @doc
%% Adds link to transfer. Links are added to link tree associated with
%% given space.
%% Real link source_id will be obtained from link_root/2 function.
%% @end
%%--------------------------------------------------------------------
-spec add_link(SourceId :: virtual_list_id(), TransferId :: transfer:id(),
    od_space:id(), transfer:timestamp()) -> ok.
add_link(SourceId, TransferId, SpaceId, Timestamp) ->
    TreeId = oneprovider:get_id(),
    Ctx = ?CTX#{scope => SpaceId},
    Key = link_key(TransferId, Timestamp),
    LinkRoot = link_root(SourceId, SpaceId),
    case datastore_model:add_links(Ctx, LinkRoot, TreeId, {Key, TransferId}) of
        {ok, _} ->
            ok;
        {error, already_exists} ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes link/links to transfer.
%% Real link source_id will be obtained from link_root/2 function.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(SourceId :: virtual_list_id(), TransferId :: transfer:id(),
    od_space:id(), transfer:timestamp()) -> ok.
delete_links(SourceId, TransferId, SpaceId, Timestamp) ->
    LinkRoot = link_root(SourceId, SpaceId),
    Key = link_key(TransferId, Timestamp),
    case datastore_model:get_links(?CTX, LinkRoot, all, Key) of
        {error, not_found} ->
            ok;
        {ok, Links} ->
            lists:foreach(fun(#link{tree_id = ProviderId, name = LinkName}) ->
                case oneprovider:is_self(ProviderId) of
                    true ->
                        ok = datastore_model:delete_links(
                            ?CTX#{scope => SpaceId}, LinkRoot,
                            ProviderId, LinkName
                        );
                    false ->
                        ok = datastore_model:mark_links_deleted(
                            ?CTX#{scope => SpaceId}, LinkRoot,
                            ProviderId, LinkName
                        )
                end
            end, Links)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes callback for each transfer.
%% @end
%%--------------------------------------------------------------------
-spec for_each_link(
    virtual_list_id(),
    Callback :: fun((link_key(), transfer:id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id(), datastore_model:fold_opts()) ->
    {ok, Acc :: term()} | {error, term()}.
for_each_link(ListDocId, Callback, Acc0, SpaceId, Options) ->
    datastore_model:fold_links(?CTX, link_root(ListDocId, SpaceId), all, fun
        (#link{name = Name, target = Target}, Acc) ->
            {ok, Callback(Name, Target, Acc)}
    end, Acc0, Options).
