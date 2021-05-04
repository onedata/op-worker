%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for operating on archive attributes.
%%% Archive attributes are user-defined metadata concerning an archive.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_attrs).
-author("Jakub Kudzia").

-include("modules/archive/archive.hrl").

%% API
-export([from_json/1, to_json/1, update/2]).

%% Getters
-export([get_description/1]).

-type attrs() :: #archive_attrs{}.
-type description() :: binary().

-export_type([attrs/0, description/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec from_json(json_utils:json_map()) -> attrs().
from_json(Attrs) ->
    #archive_attrs{
        description = maps:get(<<"description">>, Attrs, undefined)
    }.


-spec to_json(attrs()) -> json_utils:json_map().
to_json(#archive_attrs{description = Description}) ->
    #{
        <<"description">> => utils:ensure_defined(Description, <<>>)
    }.


-spec get_description(attrs()) -> description().
get_description(#archive_attrs{description = Description}) ->
    Description.


-spec update(attrs(), attrs()) -> attrs().
update(CurrentAttrs, NewAttrs) ->
    CurrentAttrs#archive_attrs{
        description = utils:ensure_defined(NewAttrs#archive_attrs.description, CurrentAttrs#archive_attrs.description)
    }.