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
-export([get_first/1]).
%% Iterator API
-export([get_next/2, forget_before/1, mark_exhausted/1]).
%% Persistent record API
-export([db_encode/2, db_decode/2, version/0]).

-record(workflow_test_iterator, {
    item_number :: non_neg_integer(),
    item_count :: non_neg_integer()
}).

-type iterator() :: #workflow_test_iterator{}.
-type item() :: binary().

%%%===================================================================
%%% API
%%%===================================================================

-spec get_first(non_neg_integer()) -> iterator().
get_first(ItemCount) ->
    #workflow_test_iterator{item_number = 1, item_count = ItemCount}.

%%%===================================================================
%%% Iterator API
%%%===================================================================

-spec get_next(workflow_engine:execution_context(), iterator()) -> {ok, item(), iterator()} | stop.
get_next(_Context, #workflow_test_iterator{item_number = ItemNumber, item_count = ItemCount})
    when ItemNumber > ItemCount ->
    stop;
get_next(_Context, #workflow_test_iterator{item_number = Number} = Iterator) ->
    {ok, integer_to_binary(Number), Iterator#workflow_test_iterator{item_number = Number + 1}}.

-spec forget_before(iterator()) -> ok.
forget_before(_) ->
    ok.

-spec mark_exhausted(iterator()) -> ok.
mark_exhausted(_) ->
    ok.

%%%===================================================================
%%% Persistent record API
%%%===================================================================

-spec db_encode(jsonable_record:record(), persistent_record:nested_record_encoder()) -> json_utils:json_term().
db_encode(#workflow_test_iterator{item_number = ItemNumber, item_count = ItemCount}, _) ->
    jiffy:encode(#{<<"item_number">> => ItemNumber, <<"item_count">> => ItemCount}).

-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) -> jsonable_record:record().
db_decode(Term, _) ->
    #{<<"item_number">> := ItemNumber, <<"item_count">> := ItemCount} = jiffy:decode(Term, [return_maps]),
    #workflow_test_iterator{item_number = ItemNumber, item_count = ItemCount}.

-spec version() -> persistent_record:record_version().
version() ->
    1.