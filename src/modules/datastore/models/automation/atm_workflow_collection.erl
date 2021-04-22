%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Module which implements generic workflow collection using datastore links.
%%% Each link is associated with exactly one workflow of given state.
%%%
%%% Collections are sorted by indices consisting of two parts:
%%% 1) timestamp part - actually it is (?EPOCH_INFINITY - specified Timestamp).
%%%                     This causes the newer entries (with higher timestamps)
%%%                     to be added at the beginning of collection.
%%% 2) workflow id part - to disambiguate between workflows reaching given state
%%%                       at the same time.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_collection).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_wokflow.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([list/3, add/4, delete/4]).


-type index() :: binary().
-type offset() :: integer().
-type limit() :: pos_integer().

-type listing_opts() :: #{
    start_index => index(),
    offset => offset(),
    limit => limit()
}.

-export_type([index/0, offset/0, limit/0, listing_opts/0]).


-define(CTX, (atm_workflow:get_ctx())).

-define(FOREST(__SPACE_ID), <<"WORKFLOWS_FOREST_", __SPACE_ID/binary>>).


%%%===================================================================
%%% API
%%%===================================================================


-spec list(od_space:id(), atm_workflow:state(), listing_opts()) ->
    [{atm_workflow:id(), index()}].
list(SpaceId, ListDocId, ListingOpts) ->
    {ok, Workflows} = for_each_link(SpaceId, ListDocId, fun(Index, WorkflowId, Acc) ->
        [{WorkflowId, Index} | Acc]
    end, [], sanitize_listing_opts(ListingOpts)),

    lists:reverse(Workflows).


-spec add(od_space:id(), atm_workflow:state(), atm_workflow:id(), time:seconds()) ->
    ok.
add(SpaceId, WorkflowState, WorkflowId, Timestamp) ->
    Index = index(WorkflowId, Timestamp),

    case datastore_model:add_links(?CTX, ?FOREST(SpaceId), WorkflowState, {Index, WorkflowId}) of
        {ok, _} ->
            ok;
        {error, already_exists} ->
            ok
    end.


-spec delete(od_space:id(), atm_workflow:state(), atm_workflow:id(), time:seconds()) ->
    ok.
delete(SpaceId, WorkflowState, WorkflowId, Timestamp) ->
    LinkName = index(WorkflowId, Timestamp),

    ok = datastore_model:delete_links(?CTX, ?FOREST(SpaceId), WorkflowState, LinkName).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec index(atm_workflow:id(), time:seconds()) -> index().
index(WorkflowId, Timestamp) ->
    TimestampPart = integer_to_binary(?EPOCH_INFINITY - Timestamp),
    <<TimestampPart/binary, WorkflowId/binary>>.


%% @private
-spec sanitize_listing_opts(listing_opts()) -> datastore_model:fold_opts() | no_return().
sanitize_listing_opts(Opts) ->
    SanitizedOpts = try
        middleware_sanitizer:sanitize_data(Opts, #{
            at_least_one => #{
                offset => {integer, any},
                start_index => {binary, any}
            },
            optional => #{limit => {integer, {not_lower_than, 1}}}
        })
    catch _:_ ->
        %% TODO VFS-7208 uncomment after introducing API errors to fslogic
        throw(?EINVAL)
    end,

    kv_utils:copy_found([
        {offset, offset},
        {limit, size},
        {start_index, prev_link_name}
    ], SanitizedOpts).


%% @private
-spec for_each_link(
    od_space:id(),
    atm_workflow:state(),
    fun((index(), atm_workflow:id(), Acc0 :: term()) -> Acc :: term()),
    term(),
    datastore_model:fold_opts()
) ->
    {ok, Acc :: term()} | {error, term()}.
for_each_link(SpaceId, WorkflowState, Callback, Acc0, Options) ->
    datastore_model:fold_links(?CTX, ?FOREST(SpaceId), WorkflowState, fun
        (#link{name = Index, target = WorkflowId}, Acc) -> {ok, Callback(Index, WorkflowId, Acc)}
    end, Acc0, Options).
