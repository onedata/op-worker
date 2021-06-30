%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for operating on archive configuration.
%%% Archive configuration parameters are used to configure the process of
%%% creating an archive.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_config).
-author("Jakub Kudzia").

-behaviour(persistent_record).

-include("modules/dataset/archive.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([from_json/1, to_json/1, sanitize/1]).
%% Getters
-export([get_layout/1, should_create_nested_archives/1, is_incremental/1, get_base_archive_id/1]).
%% Setters
-export([set_base_archive_id/2]).

-type record() :: #archive_config{}.
-type json() :: json_utils:json_map().
%% #{
%%      <<"layout">> := layout(),
%%      <<"incremental">> => incremental(),
%%      <<"baseArchiveId">> => archive:id() | null
%%      <<"includeDip">> => include_dip(),
%%      <<"createNestedArchives">> => boolean()
%% }
-type incremental() :: boolean().
-type include_dip() :: boolean().
-type layout() :: ?ARCHIVE_BAGIT_LAYOUT | ?ARCHIVE_PLAIN_LAYOUT.

-export_type([record/0, json/0, incremental/0, include_dip/0, layout/0]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec from_json(json()) -> record().
from_json(ConfigJson) ->
    #archive_config{
        layout = utils:to_atom(maps:get(<<"layout">>, ConfigJson, ?DEFAULT_LAYOUT)),
        incremental = utils:to_boolean(maps:get(<<"incremental">>, ConfigJson, ?DEFAULT_INCREMENTAL)),
        base_archive_id = utils:null_to_undefined(maps:get(<<"baseArchiveId">>, ConfigJson, ?DEFAULT_BASE_ARCHIVE)),
        include_dip = utils:to_boolean(maps:get(<<"includeDip">>, ConfigJson, ?DEFAULT_INCLUDE_DIP)),
        create_nested_archives = utils:to_boolean(maps:get(<<"createNestedArchives">>, ConfigJson, ?DEFAULT_CREATE_NESTED_ARCHIVES))
    }.


-spec to_json(record()) -> json().
to_json(#archive_config{
    incremental = Incremental,
    base_archive_id = BaseArchive,
    layout = Layout,
    include_dip = IncludeDip,
    create_nested_archives = CreateNestedArchives
}) ->
    #{
        <<"incremental">> => Incremental,
        <<"baseArchiveId">> => utils:undefined_to_null(BaseArchive),
        <<"layout">> => str_utils:to_binary(Layout),
        <<"includeDip">> => IncludeDip,
        <<"createNestedArchives">> => CreateNestedArchives
    }.


-spec sanitize(json_utils:json_map()) -> json().
sanitize(RawConfig) ->
    try
        SanitizedData = middleware_sanitizer:sanitize_data(RawConfig, #{
            optional => #{
                <<"layout">> => {atom, ?ARCHIVE_LAYOUTS},
                <<"includeDip">> => {boolean, ?SUPPORTED_INCLUDE_DIP_VALUES}, % TODO VFS-7653 change to {boolean, any}
                <<"incremental">> => {boolean, any},
                <<"createNestedArchives">> => {boolean, any}
            }
        }),
        case maps:get(<<"incremental">>, SanitizedData) of
            true ->
                BaseArchiveId = maps:get(<<"baseArchiveId">>, RawConfig, null),
                case is_valid_base_archive(BaseArchiveId) of
                    {true, BaseArchiveId2} ->
                        SanitizedData#{<<"baseArchiveId">> => BaseArchiveId2};
                    false ->
                        throw(?ERROR_BAD_VALUE_BINARY(<<"baseArchiveId">>))
                end;
            false ->
                SanitizedData
        end
    catch
        % config is a nested object of the archive object,
        % therefore catch errors and add "config." to name of the key associated with the error
        throw:?ERROR_MISSING_REQUIRED_VALUE(Key) ->
            throw(?ERROR_MISSING_REQUIRED_VALUE(str_utils:format_bin("config.~s", [Key])));
        throw:?ERROR_BAD_VALUE_NOT_ALLOWED(Key, AllowedVals) ->
            throw(?ERROR_BAD_VALUE_NOT_ALLOWED(str_utils:format_bin("config.~s", [Key]), AllowedVals));
        throw:?ERROR_BAD_VALUE_BOOLEAN(Key) ->
            throw(?ERROR_BAD_VALUE_BOOLEAN(str_utils:format_bin("config.~s", [Key])));
        throw:?ERROR_BAD_VALUE_ATOM(Key) ->
            throw(?ERROR_BAD_VALUE_ATOM(str_utils:format_bin("config.~s", [Key])));
        throw:?ERROR_BAD_VALUE_BINARY(Key) ->
            throw(?ERROR_BAD_VALUE_BINARY(str_utils:format_bin("config.~s", [Key])))
    end.


-spec get_layout(record()) -> layout().
get_layout(#archive_config{layout = Layout}) ->
    Layout.


-spec is_incremental(record()) -> boolean().
is_incremental(#archive_config{incremental = Incremental}) ->
    Incremental.

-spec set_base_archive_id(record(), archive:id() | undefined) -> record().
set_base_archive_id(ArchiveConfig, BaseArchiveId) ->
    ArchiveConfig#archive_config{base_archive_id = BaseArchiveId}.

-spec get_base_archive_id(record()) -> archive:id() | undefined.
get_base_archive_id(#archive_config{base_archive_id = BaseArchiveId}) ->
    BaseArchiveId.


-spec should_create_nested_archives(record()) -> boolean().
should_create_nested_archives(#archive_config{create_nested_archives = CreateNestedArchives}) ->
    CreateNestedArchives.


-spec is_valid_base_archive(null | archive:id()) -> false | {true, null | archive:id()}.
is_valid_base_archive(null) ->
    {true, null};
is_valid_base_archive(<<"null">>) ->
    {true, null};
is_valid_base_archive(ArchiveId) ->
    case archive:get_state(ArchiveId) of
        {ok, ?ARCHIVE_PRESERVED} -> {true, ArchiveId};
        {ok, _OtherState} -> false;
        ?ERROR_NOT_FOUND -> false
    end.

%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================

-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(ArchiveConfig, _NestedRecordEncoder) ->
    to_json(ArchiveConfig).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(ArchiveConfigJson, _NestedRecordDecoder) ->
    from_json(ArchiveConfigJson).