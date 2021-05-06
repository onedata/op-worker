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
-export([from_json/1, to_json/1, get_callback/1]).

%% Getters
-export([]).

-type params() :: #archive_params{}.
-type type() :: ?FULL_ARCHIVE | ?INCREMENTAL_ARCHIVE.
-type dip() :: boolean().
-type data_structure() :: ?BAGIT | ?SIMPLE_COPY.
-type metadata_structure() :: ?BUILT_IN | ?JSON | ?XML.
-type url_callback() :: http_client:url().

-export_type([params/0, type/0, dip/0, data_structure/0, metadata_structure/0, url_callback/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec from_json(json_utils:json_map()) -> params().
from_json(ParamsJson = #{
    <<"type">> := Type,
    <<"dataStructure">> := DataStructure,
    <<"metadataStructure">> := MetadataStructure
}) ->
    #archive_params{
        type = utils:ensure_atom(Type),
        data_structure = utils:ensure_atom(DataStructure),
        metadata_structure = utils:ensure_atom(MetadataStructure),
        % optional values
        dip = utils:ensure_boolean(maps:get(<<"dip">>, ParamsJson, ?DEFAULT_DIP)),
        callback = maps:get(<<"callback">>, ParamsJson, undefined)
    }.


-spec to_json(params()) -> json_utils:json_map().
to_json(#archive_params{
    type = Type,
    data_structure = DataStructure,
    metadata_structure = MetadataStructure,
    dip = Dip,
    callback = CallbackOrUndefined
}) ->
    ParamsJson = #{
        <<"type">> => str_utils:to_binary(Type),
        <<"dataStructure">> => str_utils:to_binary(DataStructure),
        <<"metadataStructure">> => str_utils:to_binary(MetadataStructure),
        <<"dip">> => Dip
    },
    maps_utils:put_if_defined(ParamsJson, <<"callback">>, CallbackOrUndefined).


-spec get_callback(params()) -> url_callback() | undefined.
get_callback(#archive_params{callback = Callback}) ->
    Callback.