%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Just a mock module for test.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_store).
-author("Michal Wrzeszcz").

%% API
-export([get_iterator/1, get_next/1, get_item_description/1]).
-export([encode_iterator/1, decode_iterator/1]).

-type item() :: binary().
-type iterator() :: non_neg_integer().

-export_type([item/0, iterator/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_iterator(workflow_engine:execution_id()) -> iterator().
get_iterator(_ExecutionId) ->
    1.

-spec get_next(iterator()) ->
    {ok, item(), ParallelBoxToStart :: workflow_execution_state:index(), iterator()} | none.
get_next(1001) ->
    none;
get_next(Iterator) ->
    {ok, integer_to_binary(Iterator), 1, Iterator + 1}.

-spec get_item_description(item()) -> binary() | atom() | iolist().
get_item_description(Item) ->
    Item.

-spec encode_iterator(iterator()) -> binary().
encode_iterator(Iterator) ->
    jiffy:encode(#{<<"iterator">> => Iterator}).

-spec decode_iterator(binary()) -> iterator().
decode_iterator(Term) ->
    #{<<"iterator">> := Iterator} = jiffy:decode(Term, [return_maps]),
    Iterator.