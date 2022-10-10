%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Implementation of iterator behaviour to be used in tests.
%%% TODO VFS-7784 - move to test directory when problem with mocks is solved
%%% @end
%%%--------------------------------------------------------------------
-module(workflow_test_iterator).
-author("Michal Wrzeszcz").

-behaviour(iterator).
-behaviour(persistent_record).

%% API
-export([initialize/1, initialize/2]).
%% Iterator API
-export([get_next/2]).
%% Persistent record API
-export([db_encode/2, db_decode/2, version/0]).

-record(workflow_test_iterator, {
    item_number :: non_neg_integer(),
    item_count :: non_neg_integer(),
    fail_on_item = -1 :: integer() % -1 if iterator should not fail
}).

-type iterator() :: #workflow_test_iterator{}.
-type item() :: binary().
-export_type([item/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec initialize(non_neg_integer()) -> iterator().
initialize(ItemCount) ->
    #workflow_test_iterator{item_number = 1, item_count = ItemCount}.

-spec initialize(non_neg_integer(), non_neg_integer()) -> iterator().
initialize(ItemCount, FailOnItem) ->
    #workflow_test_iterator{item_number = 1, item_count = ItemCount, fail_on_item = FailOnItem}.

%%%===================================================================
%%% Iterator API
%%%===================================================================

-spec get_next(workflow_engine:execution_context(), iterator()) -> {ok, item(), iterator()} | stop.
get_next(Context, #workflow_test_iterator{item_number = ItemNumber, fail_on_item = ItemNumber} = Iterator) ->
    case op_worker:get_env(ignore_workflow_test_iterator_fail_config, false) of
        false ->
            throw(test_error);
        true ->
            get_next(Context, Iterator#workflow_test_iterator{fail_on_item = undefined})
    end;
get_next(_Context, #workflow_test_iterator{item_number = ItemNumber, item_count = ItemCount})
    when ItemNumber > ItemCount ->
    stop;
get_next(_Context, #workflow_test_iterator{item_number = Number} = Iterator) ->
    {ok, integer_to_binary(Number), Iterator#workflow_test_iterator{item_number = Number + 1}}.

%%%===================================================================
%%% Persistent record API
%%%===================================================================

-spec db_encode(jsonable_record:record(), persistent_record:nested_record_encoder()) -> json_utils:json_term().
db_encode(#workflow_test_iterator{item_number = ItemNumber, item_count = ItemCount, fail_on_item = FailOnItem}, _) ->
    jiffy:encode(#{<<"item_number">> => ItemNumber, <<"item_count">> => ItemCount, <<"fail_on_item">> => FailOnItem}).

-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) -> jsonable_record:record().
db_decode(Term, _) ->
    #{<<"item_number">> := ItemNumber, <<"item_count">> := ItemCount, <<"fail_on_item">> := FailOnItem} =
        jiffy:decode(Term, [return_maps]),
    #workflow_test_iterator{item_number = ItemNumber, item_count = ItemCount, fail_on_item = FailOnItem}.

-spec version() -> persistent_record:record_version().
version() ->
    1.