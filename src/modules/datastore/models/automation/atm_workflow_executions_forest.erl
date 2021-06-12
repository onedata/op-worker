%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Module which implements generic automation workflow executions collection
%%% using datastore links (each link is associated with exactly one execution
%%% in given state).
%%%
%%% Collections are sorted by indices consisting of two parts:
%%% 1) timestamp part - actually it is '?EPOCH_INFINITY - specified Timestamp'.
%%%                     This causes the newer entries (with higher timestamps)
%%%                     to be added at the beginning of collection.
%%% 2) atm workflow execution id part - to disambiguate between executions
%%%                                     reaching given state at the same time.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_executions_forest).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([list/3, add/4, delete/4]).


-type forest() :: binary().
-type tree_id() :: od_atm_inventory:id().
-type tree_ids() :: tree_id() | [tree_id()] | all.

-type index() :: binary().
-type offset() :: integer().
-type limit() :: pos_integer().

-type listing_opts() :: #{
    start_index => index(),
    offset => offset(),
    limit => limit()
}.
-type listing() :: [{atm_workflow_execution:id(), index()}].

-export_type([forest/0, tree_id/0, tree_ids/0]).
-export_type([index/0, offset/0, limit/0, listing_opts/0, listing/0]).


-define(CTX, (atm_workflow_execution:get_ctx())).


%%%===================================================================
%%% API
%%%===================================================================


-spec list(forest(), tree_ids(), listing_opts()) -> listing().
list(Forest, TreeIds, ListingOpts) ->
    FoldFun = fun(#link{name = Index, target = AtmWorkflowExecutionId}, Acc) ->
        {ok, [{AtmWorkflowExecutionId, Index} | Acc]}
    end,
    {ok, AtmWorkflowExecutions} = datastore_model:fold_links(
        ?CTX, Forest, TreeIds, FoldFun, [], sanitize_listing_opts(ListingOpts)
    ),
    lists:reverse(AtmWorkflowExecutions).


-spec add(forest(), tree_id(), atm_workflow_execution:id(), atm_workflow_execution:timestamp()) ->
    ok.
add(Forest, TreeId, AtmWorkflowExecutionId, Timestamp) ->
    Link = {index(AtmWorkflowExecutionId, Timestamp), AtmWorkflowExecutionId},

    case datastore_model:add_links(?CTX, Forest, TreeId, Link) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.


-spec delete(forest(), tree_id(), atm_workflow_execution:id(), atm_workflow_execution:timestamp()) ->
    ok.
delete(Forest, TreeId, AtmWorkflowExecutionId, Timestamp) ->
    LinkName = index(AtmWorkflowExecutionId, Timestamp),

    ok = datastore_model:delete_links(?CTX, Forest, TreeId, LinkName).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec index(atm_workflow_execution:id(), atm_workflow_execution:timestamp()) ->
    index().
index(AtmWorkflowExecutionId, Timestamp) ->
    TimestampPart = integer_to_binary(?EPOCH_INFINITY - Timestamp),
    <<TimestampPart/binary, AtmWorkflowExecutionId/binary>>.


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
        %% TODO VFS-7208 do not catch errors after introducing API errors to fslogic
        throw(?EINVAL)
    end,

    kv_utils:copy_found([
        {offset, offset},
        {limit, size},
        {start_index, prev_link_name}
    ], SanitizedOpts).
