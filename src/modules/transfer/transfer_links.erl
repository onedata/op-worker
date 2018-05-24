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
-export([delete_scheduled_transfer_link/3, delete_active_transfer_link/2,
    delete_active_transfer_link/3, delete_past_transfer_link/3,
    add_scheduled_transfer_link/3, add_active_transfer_link/3,
    add_past_transfer_link/3, list_transfers/5, for_each_current_transfer/3]).
-export([link_key/2]).

-type link_key() :: binary().
-type virtual_list_id() :: binary(). % ?(SCHEDULED|CURRENT|PAST)_TRANSFERS_KEY
-type offset() :: integer().
-type list_limit() :: transfer:list_limit().

-export_type([link_key/0]).

-define(CTX, (transfer:get_ctx())).

-define(LINK_NAME_ID_PART_LENGTH, 6).
-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39



%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv add_link(?SCHEDULED_TRANSFERS_KEY, TransferId, SpaceId, StartTime).
%% @end
%%--------------------------------------------------------------------
-spec add_scheduled_transfer_link(TransferId :: transfer:id(), od_space:id(),
    transfer:timestamp()) -> ok.
add_scheduled_transfer_link(TransferId, SpaceId, ScheduleTime) ->
    add_link(?SCHEDULED_TRANSFERS_KEY, TransferId, SpaceId, ScheduleTime).

%%--------------------------------------------------------------------
%% @doc
%% @equiv add_link(?CURRENT_TRANSFERS_KEY, TransferId, SpaceId, StartTime).
%% @end
%%--------------------------------------------------------------------
-spec add_active_transfer_link(TransferId :: transfer:id(), od_space:id(),
    transfer:timestamp()) -> ok.
add_active_transfer_link(TransferId, SpaceId, ScheduleTime) ->
    add_link(?CURRENT_TRANSFERS_KEY, TransferId, SpaceId, ScheduleTime).

%%--------------------------------------------------------------------
%% @doc
%% @equiv add_link(?PAST_TRANSFERS_KEY, TransferId, SpaceId, FinishTime).
%% @end
%%--------------------------------------------------------------------
-spec add_past_transfer_link(TransferId :: transfer:id(), od_space:id(),
    transfer:timestamp()) -> ok.
add_past_transfer_link(TransferId, SpaceId, FinishTime) ->
    add_link(?PAST_TRANSFERS_KEY, TransferId, SpaceId, FinishTime).

%%--------------------------------------------------------------------
%% @doc
%% Adds link to transfer. Links are added to link tree associated with
%% given space.
%% Real link source_id will be obtained from link_root/2 function.
%% @end
%%--------------------------------------------------------------------
-spec add_link(SourceId :: virtual_list_id(), TransferId :: transfer:id(), od_space:id(),
    transfer:timestamp()) -> ok.
add_link(SourceId, TransferId, SpaceId, Timestamp) ->
    TreeId = oneprovider:get_id(),
    Ctx = ?CTX#{scope => SpaceId},
    Key = link_key(TransferId, Timestamp),
    case datastore_model:add_links(Ctx, link_root(SourceId, SpaceId), TreeId,
        {Key, TransferId})
    of
        {ok, _} ->
            ok;
        {error, already_exists} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv delete_links(?SCHEDULED_TRANSFERS_KEY, TransferId, SpaceId, StartTime).
%% @end
%%--------------------------------------------------------------------
-spec delete_scheduled_transfer_link(TransferId :: transfer:id(), od_space:id(),
    transfer:timestamp()) -> ok.
delete_scheduled_transfer_link(TransferId, SpaceId, ScheduleTime) ->
    delete_links(?SCHEDULED_TRANSFERS_KEY, TransferId, SpaceId, ScheduleTime).

%%--------------------------------------------------------------------
%% @doc
%% @equiv delete_active_transfer_link(TransferId, SpaceId, get_start_time(TransferId)).
%% @end
%%--------------------------------------------------------------------
-spec delete_active_transfer_link(TransferId :: transfer:id(), od_space:id()) -> ok.
delete_active_transfer_link(TransferId, SpaceId) ->
    delete_active_transfer_link(TransferId, SpaceId,
        transfer_utils:get_schedule_time(TransferId)).

%%--------------------------------------------------------------------
%% @doc
%% @equiv delete_links(?CURRENT_TRANSFERS_KEY, TransferId, SpaceId, StartTime).
%% @end
%%--------------------------------------------------------------------
-spec delete_active_transfer_link(TransferId :: transfer:id(), od_space:id(),
    transfer:timestamp()) -> ok.
delete_active_transfer_link(TransferId, SpaceId, StartTime) ->
    delete_links(?CURRENT_TRANSFERS_KEY, TransferId, SpaceId, StartTime).

%%--------------------------------------------------------------------
%% @doc
%% @equiv delete_links(?PAST_TRANSFERS_KEY, TransferId, SpaceId, FinishTime).
%% @end
%%--------------------------------------------------------------------
-spec delete_past_transfer_link(TransferId :: transfer:id(), od_space:id(),
    transfer:timestamp()) -> ok.
delete_past_transfer_link(TransferId, SpaceId, FinishTime) ->
    delete_links(?PAST_TRANSFERS_KEY, TransferId, SpaceId, FinishTime).

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
                            ?CTX#{scope => SpaceId}, LinkRoot, ProviderId, LinkName
                        );
                    false ->
                        ok = datastore_model:mark_links_deleted(
                            ?CTX#{scope => SpaceId}, LinkRoot, ProviderId, LinkName
                        )
                end
            end, Links)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Lists transfers.
%% @end
%%--------------------------------------------------------------------
-spec list_transfers(SpaceId :: od_space:id(), virtual_list_id(),
    transfer:id() | undefined, offset(), list_limit()) -> [{transfer:id(), link_key()}].
list_transfers(SpaceId, ListDocId, StartId, Offset, Limit) ->
    Opts = #{offset => Offset},

    Opts2 = case StartId of
        undefined -> Opts;
        _ -> Opts#{prev_link_name => StartId}
    end,

    Opts3 = case Limit of
        all -> Opts2;
        _ -> Opts2#{size => Limit}
    end,

    {ok, Transfers} = for_each_transfer(ListDocId, fun(LinkName, TransferId, Acc) ->
        [{TransferId, LinkName} | Acc]
    end, [], SpaceId, Opts3),
    lists:reverse(Transfers).

%%--------------------------------------------------------------------
%% @doc
%% Executes callback for each ongoing transfer.
%% @end
%%--------------------------------------------------------------------
-spec for_each_current_transfer(
    Callback :: fun((link_key(), transfer:id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id()) -> {ok, Acc :: term()} | {error, term()}.
for_each_current_transfer(Callback, Acc0, SpaceId) ->
    for_each_transfer(?CURRENT_TRANSFERS_KEY, Callback, Acc0, SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Returns transfer link key based on transfer's Id and Timestamp.
%% @end
%%-------------------------------------------------------------------
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
%% @private
%% @doc
%% @equiv for_each_transfer(ListDocId, Callback,  Acc0, SpaceId, #{}).
%% @end
%%--------------------------------------------------------------------
-spec for_each_transfer(
    virtual_list_id(),
    Callback :: fun((link_key(), transfer:id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id()) -> {ok, Acc :: term()} | {error, term()}.
for_each_transfer(ListDocId, Callback, Acc0, SpaceId) ->
    for_each_transfer(ListDocId, Callback, Acc0, SpaceId, #{}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes callback for each transfer.
%% @end
%%--------------------------------------------------------------------
-spec for_each_transfer(
    virtual_list_id(),
    Callback :: fun((link_key(), transfer:id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id(), datastore_model:fold_opts()) ->
    {ok, Acc :: term()} | {error, term()}.
for_each_transfer(ListDocId, Callback, Acc0, SpaceId, Options) ->
    datastore_model:fold_links(?CTX, link_root(ListDocId, SpaceId), all, fun
        (#link{name = Name, target = Target}, Acc) ->
            {ok, Callback(Name, Target, Acc)}
    end, Acc0, Options).
