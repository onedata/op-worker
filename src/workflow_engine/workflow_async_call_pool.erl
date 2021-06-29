%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules counts number of tasks being currently processed
%%% by single task processing engine (not to be confused with
%%% workflow_engine). It allows limiting number of tasks being
%%% processed in parallel.
%%% TODO VFS-7788 - count calls for each task separately
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_async_call_pool).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/2, increment_slot_usage/1, decrement_slot_usage/1, decrement_slot_usage/2]).
%% Test API
-export([get_slot_usage/1]).

-type id() :: binary().
-type record() :: #workflow_async_call_pool{}.

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

-export_type([id/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(id(), non_neg_integer()) -> ok.
init(Id, SlotsLimit) ->
    Doc = #document{key = Id, value = #workflow_async_call_pool{slots_limit = SlotsLimit}},
    case datastore_model:create(?CTX, Doc) of
        {ok, _} -> ok;
        ?ERROR_ALREADY_EXISTS -> ok
    end.

-spec increment_slot_usage(id()) -> ok | ?WF_ERROR_LIMIT_REACHED.
increment_slot_usage(Id) ->
    case datastore_model:update(?CTX, Id, fun increment_slot_usage_internal/1) of
        {ok, _} -> ok;
        ?WF_ERROR_LIMIT_REACHED = Error -> Error
    end.

-spec decrement_slot_usage(id()) -> ok.
decrement_slot_usage(Id) ->
    decrement_slot_usage(Id, 1).

-spec decrement_slot_usage(id(), non_neg_integer()) -> ok.
decrement_slot_usage(Id, Change) ->
    {ok, _} = datastore_model:update(?CTX, Id, fun(Record) -> decrement_slot_usage_internal(Record, Change) end),
    ok.

%%%===================================================================
%%% Test API
%%%===================================================================

-spec get_slot_usage(id()) -> non_neg_integer().
get_slot_usage(Id) ->
    {ok, #document{value = #workflow_async_call_pool{slots_used = SlotsUsed}}} = datastore_model:get(?CTX, Id),
    SlotsUsed.

%%%===================================================================
%%% Functions operating on record
%%%===================================================================

-spec increment_slot_usage_internal(record()) -> {ok, record()} | ?WF_ERROR_LIMIT_REACHED.
increment_slot_usage_internal(#workflow_async_call_pool{slots_used = SlotsTotal, slots_limit = SlotsTotal}) ->
    ?WF_ERROR_LIMIT_REACHED;
increment_slot_usage_internal(Record = #workflow_async_call_pool{slots_used = SlotsUsed}) ->
    {ok, Record#workflow_async_call_pool{
        slots_used = SlotsUsed + 1
    }}.

-spec decrement_slot_usage_internal(record(), non_neg_integer()) -> {ok, record()}.
decrement_slot_usage_internal(Record = #workflow_async_call_pool{slots_used = SlotsUsed}, Change) ->
    {ok, Record#workflow_async_call_pool{
        slots_used = SlotsUsed - Change
    }}.