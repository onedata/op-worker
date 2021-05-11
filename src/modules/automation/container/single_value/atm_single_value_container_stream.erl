%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_container_stream` functionality for
%%% `atm_single_value_container`.
%%%
%%%                             !!! Caution !!!
%%% This stream snapshots container's value at creation time so that even if
%%% value kept in container changes the stream will still return the same
%%% old value.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_single_value_container_stream).
-author("Bartosz Walkowicz").

-behaviour(atm_container_stream).
-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").

%% API
-export([create/1]).

% atm_container_stream callbacks
-export([get_next_batch/2, jump_to/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type item() :: json_utils:json_term().

-record(atm_single_value_container_stream, {
    value :: undefined | item(),
    exhausted = false :: boolean()
}).
-type stream() :: #atm_single_value_container_stream{}.

-export_type([item/0, stream/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(undefined | item()) -> stream().
create(Value) ->
    #atm_single_value_container_stream{value = Value, exhausted = false}.


%%%===================================================================
%%% atm_data_stream callbacks
%%%===================================================================


-spec get_next_batch(atm_container_stream:batch_size(), stream()) ->
    {ok, [item()], iterator:cursor(), stream()} | stop.
get_next_batch(_BatchSize, #atm_single_value_container_stream{value = undefined}) ->
    stop;
get_next_batch(_BatchSize, #atm_single_value_container_stream{exhausted = true}) ->
    stop;
get_next_batch(_BatchSize, #atm_single_value_container_stream{value = Value} = AtmContainerStream) ->
    {ok, [Value], <<"fin">>, AtmContainerStream#atm_single_value_container_stream{
        exhausted = true
    }}.


-spec jump_to(iterator:cursor(), stream()) -> stream() | no_return().
jump_to(<<>>, AtmContainerStream) ->
    AtmContainerStream#atm_single_value_container_stream{exhausted = false};
jump_to(<<"fin">>, AtmContainerStream) ->
    AtmContainerStream#atm_single_value_container_stream{exhausted = true};
jump_to(_InvalidCursor, _AtmContainerStream) ->
    throw(?EINVAL).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(stream(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_single_value_container_stream{
    value = undefined,
    exhausted = Exhausted
}, _NestedRecordEncoder) ->
    #{<<"exhausted">> => Exhausted};

db_encode(#atm_single_value_container_stream{
    value = Value,
    exhausted = Exhausted
}, _NestedRecordEncoder) ->
    #{<<"value">> => Value, <<"exhausted">> => Exhausted}.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    stream().
db_decode(#{<<"exhausted">> := Exhausted} = AtmContainerStreamJson, _NestedRecordDecoder) ->
    #atm_single_value_container_stream{
        value = maps:get(<<"value">>, AtmContainerStreamJson, undefined),
        exhausted = Exhausted
    }.
