%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for operating on archive parameters.
%%% Archive parameters are used to configure the process of
%%% creating an archive.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_params).
-author("Jakub Kudzia").

-include("modules/archive/archive.hrl").

%% API
-export([from_json/1, to_json/1]).

%% Getters
-export([get_type/1, get_character/1, get_data_structure/1, get_metadata_structure/1]).

-type params() :: #archive_params{}.
-type type() :: ?FULL_ARCHIVE | ?INCREMENTAL_ARCHIVE.
-type character() :: ?DIP | ?AIP | ?HYBRID.
-type data_structure() :: ?BAGIT | ?SIMPLE_COPY.
-type metadata_structure() :: ?BUILT_IN | ?JSON | ?XML.

-export_type([params/0, type/0, character/0, data_structure/0, metadata_structure/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec from_json(json_utils:json_map()) -> params().
from_json(#{
    <<"type">> := Type,
    <<"character">> := Character,
    <<"dataStructure">> := DataStructure,
    <<"metadataStructure">> := MetadataStructure
}) ->
    #archive_params{
        type = utils:ensure_atom(Type),
        character = utils:ensure_atom(Character),
        data_structure = utils:ensure_atom(DataStructure),
        metadata_structure = utils:ensure_atom(MetadataStructure)
    }.


-spec to_json(params()) -> json_utils:json_map().
to_json(#archive_params{
    type = Type,
    character = Character,
    data_structure = DataStructure,
    metadata_structure = MetadataStructure
}) ->
    #{
        <<"type">> => str_utils:to_binary(Type),
        <<"character">> => str_utils:to_binary(Character),
        <<"dataStructure">> => str_utils:to_binary(DataStructure),
        <<"metadataStructure">> => str_utils:to_binary(MetadataStructure)
    }.


-spec get_type(params()) -> type().
get_type(#archive_params{type = Type}) ->
    Type.

-spec get_character(params()) -> character().
get_character(#archive_params{character = Character}) ->
    Character.

-spec get_data_structure(params()) -> data_structure().
get_data_structure(#archive_params{data_structure = DataStructure}) ->
    DataStructure.

-spec get_metadata_structure(params()) -> metadata_structure().
get_metadata_structure(#archive_params{metadata_structure = MetadataStructure}) ->
    MetadataStructure.