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
%%% 1) This behaviour must be implemented by proper models, that is modules with
%%%    records of the same name.
%%% 2) Models implementing this behaviour must also implement `persistent_record`
%%%    behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(iterator).
-author("Bartosz Walkowicz").

%% API
-export([
    get_next/2,
    encode/1,
    decode/1
]).

-opaque iterator() :: tuple().
-type item() :: term().

-export_type([iterator/0, item/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback get_next(workflow_engine:execution_context(), iterator()) -> {ok, item(), iterator()} | stop.


%%%===================================================================
%%% API functions
%%%===================================================================


-spec get_next(workflow_engine:execution_context(), iterator()) -> {ok, item(), iterator()} | stop.
get_next(Context, Iterator) ->
    Module = utils:record_type(Iterator),
    Module:get_next(Context, Iterator).


-spec encode(iterator()) -> binary().
encode(Iterator) ->
    Model = utils:record_type(Iterator),

    json_utils:encode(#{
        <<"_type">> => atom_to_binary(Model, utf8),
        <<"_data">> => persistent_record:to_string(Iterator, Model)
    }).


-spec decode(binary()) -> iterator().
decode(IteratorBin) ->
    #{
        <<"_type">> := TypeBin,
        <<"_data">> := Data
    } = json_utils:decode(IteratorBin),

    Model = binary_to_atom(TypeBin, utf8),
    persistent_record:from_string(Data, Model).