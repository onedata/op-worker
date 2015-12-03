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
-include_lib("ctool/include/posix/errors.hrl").

-export([get_user_metadata/2, update_user_metadata/3, update_user_metadata/4]).
-export([prepare_metadata/3, prepare_metadata/4]).
-export([get_mimetype/2, get_encoding/2, get_completion_status/2,
    update_mimetype/3, update_encoding/3, update_completion_status/3,
    set_completion_status_according_to_partial_flag/3]).

%% Keys of special cdmi attrs
-define(MIMETYPE_XATTR_KEY, <<"cdmi_mimetype">>).
-define(ENCODING_XATTR_KEY, <<"cdmi_valuetransferencoding">>).
-define(COMPLETION_STATUS_XATTR_KEY, <<"cdmi_completion_status">>).

%% Default values of special cdmi attrs
-define(MIMETYPE_DEFAULT_VALUE, <<"application/octet-stream">>).
-define(ENCODING_DEFAULT_VALUE, <<"base64">>).
-define(COMPLETION_STATUS_DEFAULT_VALUE, <<"Complete">>).

-define(USER_METADATA_FORBIDDEN_PREFIX, <<"cdmi_">>).
-define(DEFAULT_STORAGE_SYSTEM_METADATA,
    [<<"cdmi_size">>, <<"cdmi_ctime">>, <<"cdmi_atime">>, <<"cdmi_mtime">>, <<"cdmi_owner">>, <<"cdmi_acl">>]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets user matadata associated with file, which are all xattrs
%% without "cdmi_" prefix.
%% @end
%%--------------------------------------------------------------------
-spec get_user_metadata(onedata_auth_api:auth(), onedata_file_api:file_key()) ->
    [{Name :: binary(), Value :: binary()}].
get_user_metadata(Auth, FileKey) ->
    {ok, Names} = onedata_file_api:list_xattr(Auth, FileKey),
    Metadata = lists:map(fun(Name) ->
            {ok, #xattr{value = XattrValue}} = onedata_file_api:get_xattr(Auth, FileKey, Name),
            {Name, XattrValue}
        end, Names),
    filter_user_metadata(Metadata).

%%--------------------------------------------------------------------
%% @equiv update_user_metadata(Auth, FileKey, UserMetadata, []).
%%--------------------------------------------------------------------
-spec update_user_metadata(Auth :: onedata_auth_api:auth(), FileKey :: onedata_file_api:file_key(),
  UserMetadata :: [{Name :: binary(), Value :: binary()}]) -> ok.
update_user_metadata(Auth, FileKey, UserMetadata) ->
    update_user_metadata(Auth, FileKey, UserMetadata, []).

%%--------------------------------------------------------------------
%% @doc Updates user metadata listed in URIMetadataNames associated with file.
%% If a matedata name specified in URIMetadataNames, but has no corresponding
%% entry in UserMetadata, entry is removed from user metadata associated with a file.
%% @end
%%--------------------------------------------------------------------
-spec update_user_metadata(Auth :: onedata_auth_api:auth(), FileKey :: onedata_file_api:file_key(),
  UserMetadata :: [{Name :: binary(), Value :: binary()}] | undefined,
    URIMetadataNames :: [Name :: binary()]) -> ok | no_return().
update_user_metadata(_Auth, _FileKey, undefined, []) ->
    ok;
update_user_metadata(Auth, FileKey, undefined, URIMetadataNames) ->
    update_user_metadata(Auth, FileKey, [], URIMetadataNames);
update_user_metadata(Auth, FileKey, UserMetadata, AllURIMetadataNames) ->
    BodyMetadata = filter_user_metadata(UserMetadata),
    BodyMetadataNames = get_metadata_names(BodyMetadata),
    DeleteAttributeFunction =
        fun
            (<<"cdmi_acl">>) -> ok = onedata_file_api:set_acl(FileKey, []); %todo integrate with new acls
            (Name) -> ok = onedata_file_api:remove_xattr(Auth, FileKey, Name)
        end,
    ReplaceAttributeFunction =
        fun
            ({<<"cdmi_acl">>, _Value}) -> ok;
%%                 ACL = try fslogic_acl:from_json_fromat_to_acl(Value)  %todo integrate with new acls
%%                       catch _:Error ->
%%                           ?debug_stacktrace("Acl conversion error ~p", [Error]),
%%                           throw({?invalid_acl, Error})
%%                       end,
%%                 ok = logical_files_manager:set_acl(FileKey, ACL);
            ({Name, Value}) -> ok = onedata_file_api:set_xattr(Auth, FileKey, #xattr{name = Name, value = Value})
        end,
    case AllURIMetadataNames of
        [] ->
            lists:foreach(DeleteAttributeFunction, get_metadata_names(get_user_metadata(Auth, FileKey)) -- BodyMetadataNames),
            lists:foreach(ReplaceAttributeFunction, BodyMetadata);
        _ ->
            UriMetadataNames = filter_user_metadata(AllURIMetadataNames),
            lists:foreach(DeleteAttributeFunction, UriMetadataNames -- BodyMetadataNames),
            lists:foreach(ReplaceAttributeFunction, filter_URI_Names(BodyMetadata, UriMetadataNames))
    end.

%%--------------------------------------------------------------------
%% @doc Prepares cdmi user and storage system metadata.
%%--------------------------------------------------------------------
-spec prepare_metadata(Auth :: onedata_auth_api:auth(), FileKey :: onedata_file_api:file_key(), #file_attr{}) ->
    [{CdmiName :: binary(), Value :: binary()}].
prepare_metadata(Auth, FileKey, Attrs) ->
    prepare_metadata(Auth, FileKey, <<"">>, Attrs).

%%--------------------------------------------------------------------
%% @doc Prepares cdmi user and storage system metadata with given prefix.
%%--------------------------------------------------------------------
-spec prepare_metadata(Auth :: onedata_auth_api:auth(), FileKey :: onedata_file_api:file_key(), Prefix :: binary(),
  #file_attr{}) -> [{CdmiName :: binary(), Value :: binary()}].
prepare_metadata(Auth, FileKey, Prefix, Attrs) ->
    StorageSystemMetadata = prepare_cdmi_metadata(?DEFAULT_STORAGE_SYSTEM_METADATA, FileKey, Attrs, Prefix),
    UserMetadata = lists:filter(fun({Name, _Value}) -> binary_with_prefix(Name, Prefix) end, get_user_metadata(Auth, FileKey)),
    StorageSystemMetadata ++ UserMetadata.

%%--------------------------------------------------------------------
%% @doc Gets mimetype associated with file, returns default value if no mimetype
%% could be found
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(onedata_auth_api:auth(), onedata_file_api:file_key()) -> binary().
get_mimetype(Auth, FileKey) ->
    case onedata_file_api:get_xattr(Auth, FileKey, ?MIMETYPE_XATTR_KEY) of
        {ok, #xattr{value = Value}} ->
            Value;
        {error, ?ENOATTR} ->
            ?MIMETYPE_DEFAULT_VALUE
    end.

%%--------------------------------------------------------------------
%% @doc Gets valuetransferencoding associated with file, returns default value
%% if no valuetransferencoding could be found
%% @end
%%--------------------------------------------------------------------
-spec get_encoding(onedata_auth_api:auth(), onedata_file_api:file_key()) -> binary().
get_encoding(Auth, FileKey) ->
    case onedata_file_api:get_xattr(Auth, FileKey, ?ENCODING_XATTR_KEY) of
        {ok, #xattr{value = Value}} ->
            Value;
        {error, ?ENOATTR} ->
            ?ENCODING_DEFAULT_VALUE
    end.

%%--------------------------------------------------------------------
%% @doc Gets completion status associated with file, returns default value if
%% no completion status could be found. The result can be:
%% binary("Complete") | binary("Processing") | binary("Error")
%% @end
%%--------------------------------------------------------------------
-spec get_completion_status(onedata_auth_api:auth(), onedata_file_api:file_key()) -> binary().
get_completion_status(Auth, FileKey) ->
    case onedata_file_api:get_xattr(Auth, FileKey, ?COMPLETION_STATUS_XATTR_KEY) of
        {ok, #xattr{value = Value}} ->
            Value;
        {error, ?ENOATTR} ->
            ?COMPLETION_STATUS_DEFAULT_VALUE
    end.

%%--------------------------------------------------------------------
%% @doc Updates mimetype associated with file
%%--------------------------------------------------------------------
-spec update_mimetype(onedata_auth_api:auth(), onedata_file_api:file_key(), binary()) -> ok | no_return().
update_mimetype(_Auth, _FileKey, undefined) -> ok;
update_mimetype(Auth, FileKey, Mimetype) ->
    ok = onedata_file_api:set_xattr(Auth, FileKey, #xattr{name = ?MIMETYPE_XATTR_KEY, value = Mimetype}).

%%--------------------------------------------------------------------
%% @doc Updates valuetransferencoding associated with file
%%--------------------------------------------------------------------
-spec update_encoding(onedata_auth_api:auth(), onedata_file_api:file_key(), binary()) -> ok | no_return().
update_encoding(_Auth, _FileKey, undefined) -> ok;
update_encoding(Auth, FileKey, Encoding) ->
    ok = onedata_file_api:set_xattr(Auth, FileKey, #xattr{name = ?ENCODING_XATTR_KEY, value = Encoding}).

%%--------------------------------------------------------------------
%% @doc Updates completion status associated with file
%%--------------------------------------------------------------------
-spec update_completion_status(onedata_auth_api:auth(), onedata_file_api:file_key(), binary()) ->
    ok | no_return().
update_completion_status(_Auth, _FileKey, undefined) -> ok;
update_completion_status(Auth, FileKey, CompletionStatus)
    when CompletionStatus =:= <<"Complete">>
    orelse CompletionStatus =:= <<"Processing">>
    orelse CompletionStatus =:= <<"Error">> ->
    ok = onedata_file_api:set_xattr(
        Auth, FileKey, #xattr{name = ?COMPLETION_STATUS_XATTR_KEY, value = CompletionStatus}).

%%--------------------------------------------------------------------
%% @doc Updates completion status associated with file
%% according to X-CDMI-Partial flag
%%--------------------------------------------------------------------
-spec set_completion_status_according_to_partial_flag(onedata_auth_api:auth(), onedata_file_api:file_key(), binary()) ->
    ok | no_return().
set_completion_status_according_to_partial_flag(_Auth, _FileKey, <<"true">>) -> ok;
set_completion_status_according_to_partial_flag(Auth, FileKey, _) ->
    ok = update_completion_status(Auth, FileKey, <<"Complete">>).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Filters out metadata with user_metadata_forbidden_prefix.
%%--------------------------------------------------------------------
-spec filter_user_metadata(UserMetadata) -> UserMetadata when
    UserMetadata :: [{CdmiName :: binary(), Value :: binary()}] | [CdmiName :: binary()].
filter_user_metadata(UserMetadata) ->
    lists:filter(
        fun
            ({<<"cdmi_acl">>, _Value}) -> true;
            ({Name, _Value}) -> not binary_with_prefix(Name, ?USER_METADATA_FORBIDDEN_PREFIX);
            (<<"cdmi_acl">>) -> true;
            (Name) -> not binary_with_prefix(Name, ?USER_METADATA_FORBIDDEN_PREFIX)
        end,
        UserMetadata).

%%--------------------------------------------------------------------
%% @doc Predicate that tells whether binary starts with given prefix.
%%--------------------------------------------------------------------
-spec binary_with_prefix(Name :: binary(), Prefix :: binary()) -> true | false.
binary_with_prefix(Name, Prefix) ->
    binary:longest_common_prefix([Name, Prefix]) =:= size(Prefix).

%%--------------------------------------------------------------------
%% @doc Returns first list from unzip result.
%%--------------------------------------------------------------------
-spec get_metadata_names([{A, B}]) -> [A] when A :: term(), B :: term().
get_metadata_names(TupleList) ->
    {Result, _} = lists:unzip(TupleList),
    Result.

%%--------------------------------------------------------------------
%% @doc Filters metadata with names contained in URIMetadataNames list.
%%--------------------------------------------------------------------
-spec filter_URI_Names(UserMetadata, URIMetadataNames :: [CdmiName]) -> UserMetadata when
    UserMetadata :: [{CdmiName, Value :: binary()}], CdmiName :: binary().
filter_URI_Names(UserMetadata, URIMetadataNames) ->
    [{Name,Value} || URIName <- URIMetadataNames, {Name, Value} <- UserMetadata, URIName == Name].

%%--------------------------------------------------------------------
%% @doc Returns system metadata with given prefix, in mochijson parser format
%%--------------------------------------------------------------------
-spec prepare_cdmi_metadata(MetadataNames :: [binary()], FileKey :: onedata_file_api:file_key(), Attrs :: #file_attr{}, Prefix :: binary()) -> list().
prepare_cdmi_metadata([], _FileKey, _Attrs, _Prefix) -> [];
prepare_cdmi_metadata([Name | Rest], FileKey, Attrs, Prefix) ->
    case binary_with_prefix(Name, Prefix) of
        true ->
            case Name of
                <<"cdmi_size">> -> %todo clarify what should be written to cdmi_size for directories
                    [{<<"cdmi_size">>, integer_to_binary(Attrs#file_attr.size)} | prepare_cdmi_metadata(Rest, FileKey, Attrs, Prefix)];
                <<"cdmi_ctime">> -> %todo format times into yyyy-mm-ddThh-mm-ss.ssssssZ
                    [{<<"cdmi_ctime">>, integer_to_binary(Attrs#file_attr.ctime)} | prepare_cdmi_metadata(Rest, FileKey, Attrs, Prefix)];
                <<"cdmi_atime">> ->
                    [{<<"cdmi_atime">>, integer_to_binary(Attrs#file_attr.atime)} | prepare_cdmi_metadata(Rest, FileKey, Attrs, Prefix)];
                <<"cdmi_mtime">> ->
                    [{<<"cdmi_mtime">>, integer_to_binary(Attrs#file_attr.mtime)} | prepare_cdmi_metadata(Rest, FileKey, Attrs, Prefix)];
                <<"cdmi_owner">> ->
                    [{<<"cdmi_owner">>, Attrs#file_attr.uid} | prepare_cdmi_metadata(Rest, FileKey, Attrs, Prefix)];
                <<"cdmi_acl">> ->
                    prepare_cdmi_metadata(Rest, FileKey, Attrs, Prefix)
%%                     case onedata_file_api:get_acl(FileKey) of %todo integrate with new acls
%%                         {ok, Acl} ->
%%                             [{<<"cdmi_acl">>, fslogic_acl:from_acl_to_json_format(Acl)} | prepare_cdmi_metadata(Rest, FileKey, Attrs, Prefix)];
%%                         {logical_file_system_error, Err} when Err =:= ?VEPERM orelse Err =:= ?VEACCES ->
%%                             throw(?forbidden);
%%                         Error -> throw(Error)
%%                     end
            end;
        false -> prepare_cdmi_metadata(Rest, FileKey, Attrs, Prefix)
    end.