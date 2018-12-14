%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Util functions for operating on autocleaning_runs links trees.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_run_links).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

-type link_key() :: binary().
-type offset() :: integer().
-type list_limit() :: integer() | all.

-export_type([offset/0, list_limit/0]).

%% API
-export([add_link/3, delete_link/3, list/4, list_since/2, link_key/2]).


-define(LINK_PREFIX, <<"autocleaning_">>).
-define(CTX, (autocleaning_run:get_ctx())).

-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39
-define(LINK_NAME_ID_PART_LENGTH, 6).
-define(LINK_TIMESTAMP_PART_LENGTH, 10).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds link to autocleaning_run document.
%% @end
%%--------------------------------------------------------------------
-spec add_link(autocleaning_run:id(), od_space:id(), non_neg_integer()) -> ok.
add_link(ARId, SpaceId, Timestamp) ->
    Ctx = ?CTX#{scope => SpaceId},
    TreeId = oneprovider:get_id(),
    Key = link_key(ARId, Timestamp),
    case datastore_model:add_links(Ctx, space_link_root(SpaceId), TreeId,
        {Key, ARId}) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Delete link to autocleaning_run document.
%% @end
%%--------------------------------------------------------------------
-spec delete_link(autocleaning_run:id(), od_space:id(), non_neg_integer()) -> ok.
delete_link(ARId, SpaceId, Timestamp) ->
    Ctx = ?CTX#{scope => SpaceId},
    TreeId = oneprovider:get_id(),
    Key = link_key(ARId, Timestamp),
    ok = datastore_model:delete_links(Ctx, space_link_root(SpaceId), TreeId, Key).

%%-------------------------------------------------------------------
%% @doc
%% Lists all autocleaning_run document ids, with timestamp newer
%% than Timestamp
%% @end
%%-------------------------------------------------------------------
-spec list_since(od_space:id(), non_neg_integer()) -> {ok, [autocleaning_run:id()]}.
list_since(SpaceId, Timestamp) ->
    {ok, ARIds} = for_each_link(fun(LinkName, ARId, AccIn) ->
        case key_to_timestamp(LinkName) < Timestamp of
            true ->
                {stop, AccIn};
            false ->
                {ok, [ARId | AccIn]}
        end
    end, [], SpaceId, #{}),
    {ok, lists:reverse(ARIds)}.

%%-------------------------------------------------------------------
%% @doc
%% Lists autocleaning document ids.
%% @end
%%-------------------------------------------------------------------
-spec list(od_space:id(), autocleaning_run:id() | undefined,
    offset(), list_limit()) -> {ok, [autocleaning_run:id()]}.
list(SpaceId, StartId, Offset, Limit) ->
    Opts = #{offset => Offset},

    Opts2 = case StartId of
        undefined -> Opts;
        _ -> Opts#{prev_link_name => StartId}
    end,

    Opts3 = case Limit of
        all -> Opts2;
        _ -> Opts2#{size => Limit}
    end,

    {ok, AutocleaningRunIds} = for_each_link(fun(_LinkName, ARId, Acc) ->
        {ok, [ARId | Acc]}
    end, [], SpaceId, Opts3),
    {ok, lists:reverse(AutocleaningRunIds)}.

%%-------------------------------------------------------------------
%% @doc
%% Returns link key for given autocleaning_run id and its Timestamp.
%% @end
%%-------------------------------------------------------------------
-spec link_key(autocleaning_run:id(), non_neg_integer()) -> link_key().
link_key(ARId, Timestamp) ->
    TimestampPart = (integer_to_binary(?EPOCH_INFINITY - Timestamp)),
    IdPart = binary:part(ARId, 0, ?LINK_NAME_ID_PART_LENGTH),
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
-spec space_link_root(od_space:id()) -> binary().
space_link_root(SpaceId) ->
    <<?LINK_PREFIX/binary, SpaceId/binary>>.

-spec key_to_timestamp(link_key()) -> non_neg_integer().
key_to_timestamp(LinkKey) ->
    TimestampPart = binary:part(LinkKey, 0, ?LINK_TIMESTAMP_PART_LENGTH),
    ?EPOCH_INFINITY - binary_to_integer(TimestampPart).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes callback for each transfer.
%% @end
%%--------------------------------------------------------------------
-spec for_each_link(
    Callback :: fun((link_key(), autocleaning_run:id(), Acc0 :: term()) ->
        {ok, Acc :: term()} | {stop, Acc :: term()}
    ),
    Acc0 :: term(), od_space:id(), datastore_model:fold_opts()) ->
    {ok, Acc :: term()} | {error, term()}.
for_each_link(Callback, Acc0, SpaceId, Options) ->
    datastore_model:fold_links(?CTX, space_link_root(SpaceId), all, fun
        (#link{name = Name, target = Target}, Acc) ->
            Callback(Name, Target, Acc)
    end, Acc0, Options).
