%%%-------------------------------------------------------------------
%%% @author Malgorzata Plazek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module provides convinience functions designed for
%%% handling CDMI user metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_metadata).

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

-export([get_user_metadata/1, update_user_metadata/2, update_user_metadata/3]).
-export([prepare_metadata/2, prepare_metadata/3]).
-export([get_mimetype/1, get_encoding/1, get_completion_status/1,
    update_mimetype/2, update_encoding/2, update_completion_status/2,
    set_completion_status_according_to_partial_flag/2]).

%% Keys of special cdmi attrs
-define(mimetype_xattr_key, <<"cdmi_mimetype">>).
-define(encoding_xattr_key, <<"cdmi_valuetransferencoding">>).
-define(completion_status_xattr_key, <<"cdmi_completion_status">>).

%% Default values of special cdmi attrs
-define(mimetype_default_value, <<"application/octet-stream">>).
-define(encoding_default_value, <<"base64">>).
-define(completion_status_default_value, <<"Complete">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets user matadata associated with file, which are all xattrs
%% without "cdmi_" prefix.
%% @end
%%--------------------------------------------------------------------
-spec get_user_metadata(Filepath :: onedata_file_api:file_path()) ->
    {ok, [{Name :: binary(), Value :: binary()}]} | no_return().
get_user_metadata(_Filepath) ->
    {ok, [{<<"key">>, <<"value">>}]}. %todo

%%--------------------------------------------------------------------
%% @equiv update_user_metadata(Filepath,UserMetadata,[])
%%--------------------------------------------------------------------
-spec update_user_metadata(Filepath :: onedata_file_api:file_path(),
  UserMetadata :: [{Name :: binary(), Value :: binary()}]) -> ok.
update_user_metadata(Filepath, UserMetadata) ->
    update_user_metadata(Filepath, UserMetadata, []).

%%--------------------------------------------------------------------
%% @doc Updates user metadata listed in URIMetadataNames associated with file.
%% If a matedata name specified in URIMetadataNames, but has no corresponding
%% entry in UserMetadata, entry is removed from user metadata associated with a file.
%% @end
%%--------------------------------------------------------------------
-spec update_user_metadata(Filepath :: onedata_file_api:file_path(),
  UserMetadata :: [{Name :: binary(), Value :: binary()}] | undefined,
    URIMetadataNames :: [Name :: binary()]) -> ok | no_return().
update_user_metadata(_Filepath, _UserMetadata, _AllURIMetadataNames) ->
    ok. %todo

%%--------------------------------------------------------------------
%% @doc Prepares cdmi user and storage system metadata.
%%--------------------------------------------------------------------
-spec prepare_metadata(Filepath :: string(), #file_attr{}) ->
    [{CdmiName :: binary(), Value :: binary()}].
prepare_metadata(_Filepath, _Attrs) ->
    [{<<"key">>, <<"value">>}]. %todo

%%--------------------------------------------------------------------
%% @doc Prepares cdmi user and storage system metadata with given prefix.
%%--------------------------------------------------------------------
-spec prepare_metadata(Filepath :: onedata_file_api:file_path(), Prefix :: binary(),
  #file_attr{}) -> [{CdmiName :: binary(), Value :: binary()}].
prepare_metadata(_Filepath, _Prefix, _Attrs) ->
    [{<<"key">>, <<"value">>}]. %todo

%%--------------------------------------------------------------------
%% @doc Gets mimetype associated with file, returns default value if no mimetype
%% could be found
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(string()) -> binary().
get_mimetype(Filepath) ->
    {ok, Value} =
        onedata_file_api:get_xattr(Filepath, ?mimetype_xattr_key),
    Value.

%%--------------------------------------------------------------------
%% @doc Gets valuetransferencoding associated with file, returns default value if no valuetransferencoding
%% could be found
%% @end
%%--------------------------------------------------------------------
-spec get_encoding(string()) -> binary().
get_encoding(Filepath) ->
    {ok, Value} =
        onedata_file_api:get_xattr(Filepath, ?encoding_xattr_key),
    Value.

%%--------------------------------------------------------------------
%% @doc Gets completion status associated with file, returns default value if no completion status
%% could be found. The result can be: binary("Complete") | binary("Processing") | binary("Error")
%% @end
%%--------------------------------------------------------------------
-spec get_completion_status(string()) -> binary().
get_completion_status(Filepath) ->
    {ok, Value} =
        onedata_file_api:get_xattr(Filepath, ?completion_status_xattr_key),
    Value.

%%--------------------------------------------------------------------
%% @doc Updates mimetype associated with file
%%--------------------------------------------------------------------
-spec update_mimetype(string(), binary()) -> ok | no_return().
update_mimetype(_Filepath, undefined) -> ok;
update_mimetype(Filepath, Mimetype) ->
    ok = onedata_file_api:set_xattr(Filepath, ?mimetype_xattr_key, Mimetype).

%%--------------------------------------------------------------------
%% @doc Updates valuetransferencoding associated with file
%%--------------------------------------------------------------------
-spec update_encoding(string(), binary()) -> ok | no_return().
update_encoding(_Filepath, undefined) -> ok;
update_encoding(Filepath, Encoding) ->
    ok = onedata_file_api:set_xattr(Filepath, ?encoding_xattr_key, Encoding).

%%--------------------------------------------------------------------
%% @doc Updates completion status associated with file
%%--------------------------------------------------------------------
-spec update_completion_status(binary(), binary()) -> ok | no_return().
update_completion_status(_Filepath, undefined) -> ok;
update_completion_status(Filepath, CompletionStatus)
    when CompletionStatus =:= <<"Complete">>
    orelse CompletionStatus =:= <<"Processing">>
    orelse CompletionStatus =:= <<"Error">> ->
    ok = onedata_file_api:set_xattr(
        {path, Filepath}, ?completion_status_xattr_key, CompletionStatus).

%%--------------------------------------------------------------------
%% @doc Updates completion status associated with file,  according to X-CDMI-Partial flag
%%--------------------------------------------------------------------
-spec set_completion_status_according_to_partial_flag(string(), binary()) -> ok | no_return().
set_completion_status_according_to_partial_flag(_Filepath, <<"true">>) -> ok;
set_completion_status_according_to_partial_flag(Filepath, _) ->
    ok = update_completion_status(Filepath, <<"Complete">>).
