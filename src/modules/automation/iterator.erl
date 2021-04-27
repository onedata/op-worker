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
-export([get_next/1, jump_to/2]).


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


%%%===================================================================
%%% API functions
%%%===================================================================


-spec get_next(iterator()) -> {ok, item(), marker(), iterator()} | stop.
get_next(Iterator) ->
    Model = utils:record_type(Iterator),
    Model:get_next(Iterator).


-spec jump_to(marker(), iterator()) -> iterator().
jump_to(Marker, Iterator) ->
    Model = utils:record_type(Iterator),
    Model:jump_to(Marker, Iterator).
