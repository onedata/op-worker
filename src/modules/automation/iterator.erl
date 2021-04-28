%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines iterator interface.
%%%
%%%                             !!! Caution !!!
%%% This behaviour must be implemented by proper models, that is modules with
%%% records of the same name.
%%% @end
%%%-------------------------------------------------------------------
-module(iterator).
-author("Bartosz Walkowicz").

%% API
-export([
    get_next/1,
    jump_to/2,
    encode/1,
    decode/1
]).

-opaque iterator() :: tuple().
% Marks specific location in collection so that it would be possible to shift
% iterator to this position.
-type marker() :: binary().
-type item() :: term().

-export_type([iterator/0, marker/0, item/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback get_next(iterator()) -> {ok, item(), marker(), iterator()} | stop.

-callback jump_to(marker(), iterator()) -> iterator().

-callback to_json(iterator()) -> json_utils:json_map().

-callback from_json(json_utils:json_map()) -> iterator().


%%%===================================================================
%%% API functions
%%%===================================================================


-spec get_next(iterator()) -> {ok, item(), marker(), iterator()} | stop.
get_next(Iterator) ->
    Module = utils:record_type(Iterator),
    Module:get_next(Iterator).


-spec jump_to(marker(), iterator()) -> iterator().
jump_to(Marker, Iterator) ->
    Module = utils:record_type(Iterator),
    Module:jump_to(Marker, Iterator).


-spec encode(iterator()) -> binary().
encode(Iterator) ->
    Module = utils:record_type(Iterator),
    IteratorJson = Module:to_json(Iterator),
    json_utils:encode(IteratorJson#{<<"_type">> => atom_to_binary(Module, utf8)}).


-spec decode(binary()) -> iterator().
decode(IteratorBin) ->
    IteratorJson = json_utils:decode(IteratorBin),
    {ModuleBin, IteratorJson2} = maps:take(<<"_type">>, IteratorJson),
    Module = binary_to_atom(ModuleBin, utf8),
    Module:from_json(IteratorJson2).
