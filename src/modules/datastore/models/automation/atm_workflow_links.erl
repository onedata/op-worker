%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for operating on workflows links trees.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_links).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_wokflow.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([link_key/2]).
-export([
    add_waiting/1, delete_waiting/1,
    add_ongoing/1, delete_ongoing/1,
    add_ended/1, delete_ended/1,

    move_from_waiting_to_ongoing/1,
    move_from_ongoing_to_ended/1
]).
-export([list/3]).

-type link_key() :: binary().
-type virtual_list_id() :: binary(). % ?(WAITING|ONGOING|ENDED)_WORKFLOWS_KEY

-type start_index() :: binary().
-type offset() :: integer().
-type limit() :: pos_integer().

-type listing_opts() :: #{
    offset => offset(),
    start_index => start_index(),
    limit => limit()
}.

-export_type([
    link_key/0, virtual_list_id/0,
    start_index/0, offset/0, limit/0, listing_opts/0
]).

-define(CTX, (atm_workflow:get_ctx())).

-define(LINK_NAME_ID_PART_LENGTH, 6).
-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39


%%%===================================================================
%%% API
%%%===================================================================


-spec link_key(atm_workflow:id(), time:seconds()) -> link_key().
link_key(WorkflowId, Timestamp) ->
    TimestampPart = (integer_to_binary(?EPOCH_INFINITY - Timestamp)),
    IdPart = binary:part(WorkflowId, 0, ?LINK_NAME_ID_PART_LENGTH),
    <<TimestampPart/binary, IdPart/binary>>.


-spec add_waiting(atm_workflow:doc()) -> ok.
add_waiting(#document{key = WorkflowId, value = #atm_workflow{
    space_id = SpaceId,
    schedule_time = ScheduleTime
}}) ->
    ok = add_link(?WAITING_WORKFLOWS_KEY, WorkflowId, SpaceId, ScheduleTime).


-spec delete_waiting(atm_workflow:doc()) -> ok.
delete_waiting(#document{key = WorkflowId, value = #atm_workflow{
    space_id = SpaceId,
    schedule_time = ScheduleTime
}}) ->
    ok = delete_link(?WAITING_WORKFLOWS_KEY, WorkflowId, SpaceId, ScheduleTime).


-spec add_ongoing(atm_workflow:doc()) -> ok.
add_ongoing(#document{key = WorkflowId, value = #atm_workflow{
    space_id = SpaceId,
    start_time = StartTime
}}) ->
    ok = add_link(?ONGOING_WORKFLOWS_KEY, WorkflowId, SpaceId, StartTime).


-spec delete_ongoing(atm_workflow:doc()) -> ok.
delete_ongoing(#document{key = WorkflowId, value = #atm_workflow{
    space_id = SpaceId,
    start_time = StartTime
}}) ->
    ok = delete_link(?ONGOING_WORKFLOWS_KEY, WorkflowId, SpaceId, StartTime).


-spec add_ended(atm_workflow:doc()) -> ok.
add_ended(#document{key = WorkflowId, value = #atm_workflow{
    space_id = SpaceId,
    finish_time = FinishTime
}}) ->
    ok = add_link(?ENDED_WORKFLOWS_KEY, WorkflowId, SpaceId, FinishTime).


-spec delete_ended(atm_workflow:doc()) -> ok.
delete_ended(#document{key = WorkflowId, value = #atm_workflow{
    space_id = SpaceId,
    finish_time = FinishTime
}}) ->
    ok = delete_link(?ENDED_WORKFLOWS_KEY, WorkflowId, SpaceId, FinishTime).


-spec move_from_waiting_to_ongoing(atm_workflow:doc()) -> ok.
move_from_waiting_to_ongoing(Doc) ->
    add_ongoing(Doc),
    delete_waiting(Doc).


-spec move_from_ongoing_to_ended(atm_workflow:doc()) -> ok.
move_from_ongoing_to_ended(Doc) ->
    add_ended(Doc),
    delete_ongoing(Doc).


-spec list(od_space:id(), virtual_list_id(), listing_opts()) ->
    [{atm_workflow:id(), link_key()}].
list(SpaceId, ListDocId, ListingOpts) ->
    {ok, Workflows} = for_each_link(ListDocId, fun(LinkName, WorkflowId, Acc) ->
        [{WorkflowId, LinkName} | Acc]
    end, [], SpaceId, sanitize_listing_opts(ListingOpts)),

    lists:reverse(Workflows).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec add_link(virtual_list_id(), atm_workflow:id(), od_space:id(), time:seconds()) ->
    ok.
add_link(SourceId, WorkflowId, SpaceId, Timestamp) ->
    Key = link_key(WorkflowId, Timestamp),
    LinkRoot = link_root(SourceId, SpaceId),

    case datastore_model:add_links(?CTX, LinkRoot, ?WORKFLOWS_TREE_ID, {Key, WorkflowId}) of
        {ok, _} ->
            ok;
        {error, already_exists} ->
            ok
    end.


%% @private
-spec delete_link(virtual_list_id(), atm_workflow:id(), od_space:id(), time:seconds()) ->
    ok.
delete_link(SourceId, WorkflowId, SpaceId, Timestamp) ->
    LinkRoot = link_root(SourceId, SpaceId),
    Key = link_key(WorkflowId, Timestamp),

    ok = datastore_model:delete_links(?CTX, LinkRoot, ?WORKFLOWS_TREE_ID, Key).


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
    virtual_list_id(),
    fun((link_key(), atm_workflow:id(), Acc0 :: term()) -> Acc :: term()),
    term(),
    od_space:id(),
    datastore_model:fold_opts()
) ->
    {ok, Acc :: term()} | {error, term()}.
for_each_link(ListDocId, Callback, Acc0, SpaceId, Options) ->
    datastore_model:fold_links(?CTX, link_root(ListDocId, SpaceId), all, fun
        (#link{name = Name, target = Target}, Acc) -> {ok, Callback(Name, Target, Acc)}
    end, Acc0, Options).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns links tree root for given space.
%% @end
%%-------------------------------------------------------------------
-spec link_root(binary(), od_space:id()) -> binary().
link_root(Prefix, SpaceId) ->
    <<Prefix/binary, "_", SpaceId/binary>>.
