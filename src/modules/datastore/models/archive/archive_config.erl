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
-export([
    get_layout/1, is_incremental/1, get_incremental_based_on/1, 
    should_include_dip/1, is_nested_archives_creation_enabled/1, should_follow_symlinks/1
]).
%% Setters
-export([enforce_plain_layout/1]).

-type record() :: #archive_config{}.
-type json() :: json_utils:json_map().
%% #{
%%      <<"layout">> := layout(),
%%      <<"incremental">> => #{
%%          <<"enabled">> := boolean(), 
%%          <<"basedOn">> => archive:id()
%%      },
%%      <<"includeDip">> => include_dip(),
%%      <<"createNestedArchives">> => boolean()
%% }
-type incremental() :: json_utils:json_map().
-type include_dip() :: boolean().
-type layout() :: ?ARCHIVE_BAGIT_LAYOUT | ?ARCHIVE_PLAIN_LAYOUT.

-export_type([record/0, json/0, incremental/0, include_dip/0, layout/0]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2, upgrade_encoded_record/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec from_json(json()) -> record().
from_json(ConfigJson) ->
    #archive_config{
        layout = utils:to_atom(maps:get(<<"layout">>, ConfigJson, ?DEFAULT_LAYOUT)),
        incremental = maps:get(<<"incremental">>, ConfigJson, ?DEFAULT_INCREMENTAL),
        include_dip = utils:to_boolean(maps:get(<<"includeDip">>, ConfigJson, ?DEFAULT_INCLUDE_DIP)),
        create_nested_archives = utils:to_boolean(maps:get(<<"createNestedArchives">>, ConfigJson, ?DEFAULT_CREATE_NESTED_ARCHIVES)),
        follow_symlinks = utils:to_boolean(maps:get(<<"followSymlinks">>, ConfigJson, ?DEFAULT_ARCHIVE_FOLLOW_SYMLINKS))
    }.

-spec to_json(record()) -> json().
to_json(#archive_config{
    incremental = Incremental,
    layout = Layout,
    include_dip = IncludeDip,
    create_nested_archives = CreateNestedArchives,
    follow_symlinks = FollowSymlinks
}) ->
    #{
        <<"incremental">> => Incremental,
        <<"layout">> => str_utils:to_binary(Layout),
        <<"includeDip">> => IncludeDip,
        <<"createNestedArchives">> => CreateNestedArchives,
        <<"followSymlinks">> => FollowSymlinks
    }.


-spec sanitize(json_utils:json_map()) -> json().
sanitize(RawConfig) ->
    try
        SanitizedData = middleware_sanitizer:sanitize_data(RawConfig, #{
            optional => #{
                <<"layout">> => {atom, ?ARCHIVE_LAYOUTS},
                <<"includeDip">> => {boolean, any},
                <<"incremental">> => {json, non_empty},
                <<"createNestedArchives">> => {boolean, any},
                <<"followSymlinks">> => {boolean, any}
            }
        }),
        case maps:get(<<"incremental">>, SanitizedData, ?DEFAULT_INCREMENTAL) of
            #{<<"enabled">> := true} = IncrementalConfig ->
                BaseArchiveId = maps:get(<<"basedOn">>, IncrementalConfig, null),
                case is_valid_base_archive(BaseArchiveId) of
                    {true, _} ->
                        SanitizedData;
                    false ->
                        throw(?ERROR_BAD_VALUE_IDENTIFIER(<<"incremental.basedOn">>))
                end;
            #{<<"enabled">> := false} ->
                SanitizedData;
            #{<<"enabled">> := _NotBoolean} ->
                throw(?ERROR_BAD_VALUE_BOOLEAN(<<"incremental.enabled">>));
            _ ->
                throw(?ERROR_MISSING_REQUIRED_VALUE(<<"incremental.enabled">>))
        end
    catch
        % config is a nested object of the archive object,
        % therefore catch errors and add "config." to name of the key associated with the error
        throw:?ERROR_MISSING_REQUIRED_VALUE(Key) ->
            throw(?ERROR_MISSING_REQUIRED_VALUE(str_utils:format_bin("config.~ts", [Key])));
        throw:?ERROR_BAD_VALUE_NOT_ALLOWED(Key, AllowedVals) ->
            throw(?ERROR_BAD_VALUE_NOT_ALLOWED(str_utils:format_bin("config.~ts", [Key]), AllowedVals));
        throw:?ERROR_BAD_VALUE_BOOLEAN(Key) ->
            throw(?ERROR_BAD_VALUE_BOOLEAN(str_utils:format_bin("config.~ts", [Key])));
        throw:?ERROR_BAD_VALUE_JSON(Key) ->
            throw(?ERROR_BAD_VALUE_JSON(str_utils:format_bin("config.~ts", [Key])));
        throw:?ERROR_BAD_VALUE_ATOM(Key) ->
            throw(?ERROR_BAD_VALUE_ATOM(str_utils:format_bin("config.~ts", [Key])));
        throw:?ERROR_BAD_VALUE_IDENTIFIER(Key) ->
            throw(?ERROR_BAD_VALUE_IDENTIFIER(str_utils:format_bin("config.~ts", [Key])))
    end.


-spec get_layout(record()) -> layout().
get_layout(#archive_config{layout = Layout}) ->
    Layout.


-spec is_incremental(record()) -> boolean().
is_incremental(#archive_config{incremental = Incremental}) ->
    maps:get(<<"enabled">>, Incremental).


-spec get_incremental_based_on(record()) -> archive:id() | undefined.
get_incremental_based_on(#archive_config{incremental = IncrementalConfig}) ->
    utils:null_to_undefined(maps:get(<<"basedOn">>, IncrementalConfig, undefined)).


-spec is_nested_archives_creation_enabled(record()) -> boolean().
is_nested_archives_creation_enabled(#archive_config{create_nested_archives = CreateNestedArchives}) ->
    CreateNestedArchives.


-spec should_include_dip(record()) -> boolean().
should_include_dip(#archive_config{include_dip = IncludeDip}) ->
    IncludeDip.


-spec should_follow_symlinks(record()) -> boolean().
should_follow_symlinks(#archive_config{follow_symlinks = FollowSymlinks}) ->
    FollowSymlinks.


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


-spec enforce_plain_layout(record()) -> record().
enforce_plain_layout(ArchiveConfig) ->
    ArchiveConfig#archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}.

%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================

-spec version() -> persistent_record:record_version().
version() ->
    2.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(ArchiveConfig, _NestedRecordEncoder) ->
    to_json(ArchiveConfig).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(ArchiveConfigJson, _NestedRecordDecoder) ->
    from_json(ArchiveConfigJson).


-spec upgrade_encoded_record(persistent_record:record_version(), json_utils:json_term()) ->
    {persistent_record:record_version(), json_utils:json_term()}.
upgrade_encoded_record(1, ArchiveConfig) ->
     #{
         <<"incremental">> := Incremental,
         <<"layout">> := Layout,
         <<"includeDip">> := IncludeDip,
         <<"createNestedArchives">> := CreateNestedArchives
     } = ArchiveConfig,
     {2, #{
         <<"incremental">> => Incremental,
         <<"layout">> => Layout,
         <<"includeDip">> => IncludeDip,
         <<"createNestedArchives">> => CreateNestedArchives,
         <<"followSymlinks">> => false % new field
     }}.