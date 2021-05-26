%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_container_iterator` functionality for
%%% `atm_list_container`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_list_container_iterator).
-author("Michal Stanisz").

-behaviour(atm_container_iterator).
-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").

%% API
-export([create/1]).

% atm_container_iterator callbacks
-export([get_next_batch/2, jump_to/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type item() :: json_utils:json_term().

-record(atm_list_container_iterator, {
    backend_id :: atm_list_container:id(),
    index = 0 :: non_neg_integer()
}).
-type record() :: #atm_list_container_iterator{}.

-export_type([item/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_list_container:backend_id()) -> record().
create(BackendId) ->
    #atm_list_container_iterator{backend_id = BackendId}.


%%%===================================================================
%%% atm_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(atm_container_iterator:batch_size(), record()) ->
    {ok, [item()], iterator:cursor(), record()} | stop.
get_next_batch(BatchSize, #atm_list_container_iterator{} = Record) ->
    #atm_list_container_iterator{backend_id = BackendId, index = StartIndex} = Record,
    {ok, {Marker, EntrySeries}} = atm_list_store_backend:list(
        BackendId, #{start_from => {index, StartIndex}, limit => BatchSize}),
    Res = lists:map(fun({_Index, {_Timestamp, V}}) -> json_utils:decode(V) end, EntrySeries),
    case {Res, Marker} of
        {[], done} -> 
            stop;
        _ ->
            {LastIndex, _} = lists:last(EntrySeries),
            Cursor = integer_to_binary(LastIndex + 1),
            {ok, Res, Cursor, Record#atm_list_container_iterator{index = LastIndex + 1}}
    end.


-spec jump_to(iterator:cursor(), record()) -> record() | no_return().
jump_to(<<>>, AtmContainerIterator) ->
    AtmContainerIterator#atm_list_container_iterator{index = 0};
jump_to(Cursor, AtmContainerIterator) ->
    AtmContainerIterator#atm_list_container_iterator{index = sanitize_cursor(Cursor)}.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_list_container_iterator{
    backend_id = BackendId,
    index = Index
}, _NestedRecordEncoder) ->
    #{<<"backendId">> => BackendId, <<"index">> => Index}.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"backendId">> := BackendId, <<"index">> := Index}, _NestedRecordDecoder) ->
    #atm_list_container_iterator{
        backend_id = BackendId,
        index = Index
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec sanitize_cursor(iterator:cursor()) -> non_neg_integer() | no_return().
sanitize_cursor(Cursor) ->
    try
        CursorInt = binary_to_integer(Cursor),
        true = is_in_proper_range(CursorInt),
        CursorInt
    catch _:_ ->
        throw(?EINVAL)
    end.


%% @private
-spec is_in_proper_range(integer()) -> boolean().
is_in_proper_range(Num) when Num >= 0 -> true;
is_in_proper_range(_Num) -> false.
