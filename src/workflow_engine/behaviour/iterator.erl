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
    forget_before/1,
    mark_exhausted/1,
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

%%--------------------------------------------------------------------
%% @doc
%% Marks all iterators older than given one as invalid - auxiliary
%% data stored for purpose of serving next items with the use of
%% these iterators can be cleaned up.
%% Given iterator, as well as all subsequent ones, are still valid.
%% Later calls to get_next/2 with invalidated iterators can
%% result in undefined behaviour.
%% @end
%%--------------------------------------------------------------------
-callback forget_before(iterator()) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Marks all iterators in given iteration lane as invalid - all auxiliary
%% data stored for purpose of serving next items can be cleaned up.
%% Later calls to get_next/2 can result in undefined behaviour.
%% @end
%%--------------------------------------------------------------------
-callback mark_exhausted(iterator()) -> ok.


%%%===================================================================
%%% API functions
%%%===================================================================


-spec get_next(workflow_engine:execution_context(), iterator()) -> {ok, item(), iterator()} | stop.
get_next(Context, Iterator) ->
    Module = utils:record_type(Iterator),
    Module:get_next(Context, Iterator).


-spec forget_before(iterator()) -> ok.
forget_before(Iterator) ->
    Module = utils:record_type(Iterator),
    Module:forget_before(Iterator).


-spec mark_exhausted(iterator()) -> ok.
mark_exhausted(Iterator) ->
    Module = utils:record_type(Iterator),
    Module:mark_exhausted(Iterator).


-spec encode(iterator()) -> binary().
encode(Iterator) ->
    Model = utils:record_type(Iterator),

    json_utils:encode(#{
        <<"_type">> => atom_to_binary(Model, utf8),
        <<"_data">> => persistent_record:encode(Iterator, Model)
    }).


-spec decode(binary()) -> iterator().
decode(IteratorBin) ->
    #{
        <<"_type">> := TypeBin,
        <<"_data">> := Data
    } = json_utils:decode(IteratorBin),

    Model = binary_to_atom(TypeBin, utf8),
    persistent_record:decode(Data, Model).