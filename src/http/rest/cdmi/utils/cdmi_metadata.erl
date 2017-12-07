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
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("http/rest/cdmi/cdmi_errors.hrl").

-export([get_user_metadata/2, update_user_metadata/3, update_user_metadata/4]).
-export([prepare_metadata/2, prepare_metadata/3, prepare_metadata/4]).
-export([get_mimetype/2, get_encoding/2, get_cdmi_completion_status/2,
    update_mimetype/3, update_encoding/3, update_cdmi_completion_status/3,
    set_cdmi_completion_status_according_to_partial_flag/3]).

%% Default values of special cdmi attrs
-define(MIMETYPE_DEFAULT_VALUE, <<"application/octet-stream">>).
-define(ENCODING_DEFAULT_VALUE, <<"base64">>).
-define(COMPLETION_STATUS_DEFAULT_VALUE, <<"Complete">>).

-define(USER_METADATA_FORBIDDEN_PREFIX_STRING, "cdmi_").
-define(USER_METADATA_FORBIDDEN_PREFIX, <<?USER_METADATA_FORBIDDEN_PREFIX_STRING>>).
-define(DEFAULT_STORAGE_SYSTEM_METADATA,
    [<<"cdmi_size">>, <<"cdmi_ctime">>, <<"cdmi_atime">>, <<"cdmi_mtime">>, <<"cdmi_owner">>, ?ACL_XATTR_NAME]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets user matadata associated with file, which are all xattrs
%% without "cdmi_" prefix.
%% @end
%%--------------------------------------------------------------------
-spec get_user_metadata(onedata_auth_api:auth(), onedata_file_api:file_key()) ->
    maps:map().
get_user_metadata(Auth, FileKey) ->
    {ok, Names} = onedata_file_api:list_xattr(Auth, FileKey, false, true),
    Metadata = lists:filtermap(
        fun
            (<<?USER_METADATA_FORBIDDEN_PREFIX_STRING, _/binary>>) -> false;
            (Name) ->
                case onedata_file_api:get_xattr(Auth, FileKey, Name, false) of
                    {ok, #xattr{value = XattrValue}} ->
                        {true, {Name, XattrValue}};
                    {error, ?ENOATTR} ->
                        false
                end
        end, Names),
    filter_user_metadata_map(maps:from_list(Metadata)).

%%--------------------------------------------------------------------
%% @equiv update_user_metadata(Auth, FileKey, UserMetadata, []).
%%--------------------------------------------------------------------
-spec update_user_metadata(onedata_auth_api:auth(), onedata_file_api:file_key(),
    maps:map()) -> ok.
update_user_metadata(Auth, FileKey, UserMetadata) ->
    update_user_metadata(Auth, FileKey, UserMetadata, []).

%%--------------------------------------------------------------------
%% @doc Updates user metadata listed in URIMetadataNames associated with file.
%% If a matedata name specified in URIMetadataNames, but has no corresponding
%% entry in UserMetadata, entry is removed from user metadata associated with a file.
%% @end
%%--------------------------------------------------------------------
-spec update_user_metadata(onedata_auth_api:auth(), onedata_file_api:file_key(),
    UserMetadata :: maps:map() | undefined, URIMetadataNames :: [Name :: binary()]) ->
    ok | no_return().
update_user_metadata(_Auth, _FileKey, undefined, []) ->
    ok;
update_user_metadata(Auth, FileKey, undefined, URIMetadataNames) ->
    update_user_metadata(Auth, FileKey, #{}, URIMetadataNames);
update_user_metadata(Auth, FileKey, UserMetadata, AllURIMetadataNames) ->
    BodyMetadata = filter_user_metadata_map(UserMetadata),
    BodyMetadataNames = maps:keys(BodyMetadata),
    DeleteAttributeFunction =
        fun
            (?ACL_XATTR_NAME) ->
                ok = onedata_file_api:remove_acl(Auth, FileKey);
            (Name) ->
                ok = onedata_file_api:remove_xattr(Auth, FileKey, Name)
        end,
    ReplaceAttributeFunction =
        fun
            ({?ACL_XATTR_NAME, Value}) ->
                ACL = try acl_logic:from_json_format_to_acl(Value)
                catch _:Error ->
                    ?warning_stacktrace("Acl conversion error ~p", [Error]),
                    throw(?ERROR_INVALID_ACL)
                end,
                ok = onedata_file_api:set_acl(Auth, FileKey, ACL);
            ({Name, Value}) ->
                ok = onedata_file_api:set_xattr(Auth, FileKey, #xattr{name = Name, value = Value})
        end,
    case AllURIMetadataNames of
        [] ->
            lists:foreach(DeleteAttributeFunction, maps:keys(get_user_metadata(Auth, FileKey)) -- BodyMetadataNames),
            lists:foreach(ReplaceAttributeFunction, maps:to_list(BodyMetadata));
        _ ->
            UriMetadataNames = filter_user_metadata_keylist(AllURIMetadataNames),
            lists:foreach(DeleteAttributeFunction, UriMetadataNames -- BodyMetadataNames),
            lists:foreach(ReplaceAttributeFunction, maps:to_list(filter_URI_Names(BodyMetadata, UriMetadataNames)))
    end.

%%--------------------------------------------------------------------
%% @equiv prepare_metadata(Auth, FileKey, <<>>).
%%--------------------------------------------------------------------
-spec prepare_metadata(onedata_auth_api:auth(), onedata_file_api:file_key()) ->
    maps:map().
prepare_metadata(Auth, FileKey) ->
    prepare_metadata(Auth, FileKey, <<>>).

%%--------------------------------------------------------------------
%% @doc Prepares cdmi user and storage system metadata.
%%--------------------------------------------------------------------
-spec prepare_metadata(onedata_auth_api:auth(), onedata_file_api:file_key(), binary()) ->
    maps:map().
prepare_metadata(Auth, FileKey, Prefix) ->
    {ok, Attrs} = onedata_file_api:stat(Auth, FileKey),
    prepare_metadata(Auth, FileKey, Prefix, Attrs).

%%--------------------------------------------------------------------
%% @doc Prepares cdmi user and storage system metadata with given prefix.
%%--------------------------------------------------------------------
-spec prepare_metadata(Auth :: onedata_auth_api:auth(), FileKey :: onedata_file_api:file_key(),
    Prefix :: binary(), #file_attr{}) -> maps:map().
prepare_metadata(Auth, FileKey, Prefix, Attrs) ->
    StorageSystemMetadata = prepare_cdmi_metadata(?DEFAULT_STORAGE_SYSTEM_METADATA, FileKey, Auth, Attrs, Prefix),
    UserMetadata = maps:filter(fun(Name, _Value) ->
        str_utils:binary_starts_with(Name, Prefix) end, get_user_metadata(Auth, FileKey)),
    maps:merge(StorageSystemMetadata, UserMetadata).

%%--------------------------------------------------------------------
%% @doc Gets mimetype associated with file, returns default value if no mimetype
%% could be found
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(onedata_auth_api:auth(), onedata_file_api:file_key()) -> binary().
get_mimetype(Auth, FileKey) ->
    case onedata_file_api:get_mimetype(Auth, FileKey) of
        {ok, Value} ->
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
    case onedata_file_api:get_transfer_encoding(Auth, FileKey) of
        {ok, Value} ->
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
-spec get_cdmi_completion_status(onedata_auth_api:auth(), onedata_file_api:file_key()) -> binary().
get_cdmi_completion_status(Auth, FileKey) ->
    case onedata_file_api:get_cdmi_completion_status(Auth, FileKey) of
        {ok, Value} ->
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
    ok = onedata_file_api:set_mimetype(Auth, FileKey, Mimetype).

%%--------------------------------------------------------------------
%% @doc Updates valuetransferencoding associated with file
%%--------------------------------------------------------------------
-spec update_encoding(onedata_auth_api:auth(), onedata_file_api:file_key(), binary() | undefined) -> ok | no_return().
update_encoding(_Auth, _FileKey, undefined) -> ok;
update_encoding(Auth, FileKey, Encoding) ->
    ok = onedata_file_api:set_transfer_encoding(Auth, FileKey, Encoding).

%%--------------------------------------------------------------------
%% @doc Updates completion status associated with file
%%--------------------------------------------------------------------
-spec update_cdmi_completion_status(onedata_auth_api:auth(), onedata_file_api:file_key(), binary()) ->
    ok | no_return().
update_cdmi_completion_status(_Auth, _FileKey, undefined) -> ok;
update_cdmi_completion_status(Auth, FileKey, CompletionStatus)
    when CompletionStatus =:= <<"Complete">>
    orelse CompletionStatus =:= <<"Processing">>
    orelse CompletionStatus =:= <<"Error">> ->
    ok = onedata_file_api:set_cdmi_completion_status(Auth, FileKey, CompletionStatus).

%%--------------------------------------------------------------------
%% @doc Updates completion status associated with file
%% according to X-CDMI-Partial flag
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status_according_to_partial_flag(onedata_auth_api:auth(), onedata_file_api:file_key(), binary()) ->
    ok | no_return().
set_cdmi_completion_status_according_to_partial_flag(_Auth, _FileKey, <<"true">>) ->
    ok;
set_cdmi_completion_status_according_to_partial_flag(Auth, FileKey, _) ->
    ok = update_cdmi_completion_status(Auth, FileKey, <<"Complete">>).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Filters out metadata with user_metadata_forbidden_prefix.
%%--------------------------------------------------------------------
-spec filter_user_metadata_map(maps:map()) -> maps:map().
filter_user_metadata_map(UserMetadata) when is_map(UserMetadata) ->
    maps:filter(
        fun
            (?ACL_XATTR_NAME, _Value) -> true;
            (Name, _Value) ->
                not str_utils:binary_starts_with(Name, ?USER_METADATA_FORBIDDEN_PREFIX)
        end,
        UserMetadata);
filter_user_metadata_map(_) ->
    throw(?ERROR_INVALID_METADATA).

%%--------------------------------------------------------------------
%% @doc Filters out metadata with user_metadata_forbidden_prefix.
%%--------------------------------------------------------------------
-spec filter_user_metadata_keylist(list()) -> list().
filter_user_metadata_keylist(UserMetadata) when is_list(UserMetadata) ->
    lists:filter(
        fun
            (?ACL_XATTR_NAME) -> true;
            (Name) ->
                not str_utils:binary_starts_with(Name, ?USER_METADATA_FORBIDDEN_PREFIX)
        end,
        UserMetadata);
filter_user_metadata_keylist(_) ->
    throw(?ERROR_INVALID_METADATA).

%%--------------------------------------------------------------------
%% @doc Filters metadata with names contained in URIMetadataNames list.
%%--------------------------------------------------------------------
-spec filter_URI_Names(maps:map(), [CdmiName :: binary()]) -> maps:map().
filter_URI_Names(UserMetadata, URIMetadataNames) ->
    maps:filter(fun(Name, _) ->
        lists:member(Name, URIMetadataNames) end, UserMetadata).

%%--------------------------------------------------------------------
%% @doc Returns system metadata with given prefix, in mochijson parser format
%%--------------------------------------------------------------------
-spec prepare_cdmi_metadata(MetadataNames :: [binary()], onedata_file_api:file_key(),
    onedata_auth_api:auth(), #file_attr{}, Prefix :: binary()) -> maps:map().
prepare_cdmi_metadata([], _FileKey, _Auth, _Attrs, _Prefix) ->
    #{};
prepare_cdmi_metadata([Name | Rest], FileKey, Auth, Attrs, Prefix) ->
    case str_utils:binary_starts_with(Name, Prefix) of
        true ->
            case Name of
                <<"cdmi_size">> -> %todo clarify what should be written to cdmi_size for directories
                    (prepare_cdmi_metadata(Rest, FileKey, Auth, Attrs, Prefix))#{
                        <<"cdmi_size">> => integer_to_binary(Attrs#file_attr.size)
                    };
                <<"cdmi_ctime">> ->
                    (prepare_cdmi_metadata(Rest, FileKey, Auth, Attrs, Prefix))#{
                        <<"cdmi_ctime">> => time_utils:epoch_to_iso8601(Attrs#file_attr.ctime)
                    };
                <<"cdmi_atime">> ->
                    (prepare_cdmi_metadata(Rest, FileKey, Auth, Attrs, Prefix))#{
                        <<"cdmi_atime">> => time_utils:epoch_to_iso8601(Attrs#file_attr.atime)
                    };
                <<"cdmi_mtime">> ->
                    (prepare_cdmi_metadata(Rest, FileKey, Auth, Attrs, Prefix))#{
                        <<"cdmi_mtime">> => time_utils:epoch_to_iso8601(Attrs#file_attr.mtime)
                    };
                <<"cdmi_owner">> ->
                    (prepare_cdmi_metadata(Rest, FileKey, Auth, Attrs, Prefix))#{
                        <<"cdmi_owner">> => Attrs#file_attr.owner_id
                    };
                ?ACL_XATTR_NAME ->
                    case onedata_file_api:get_acl(Auth, FileKey) of
                        {ok, Acl} ->
                            (prepare_cdmi_metadata(Rest, FileKey, Auth, Attrs, Prefix))#{
                                ?ACL_XATTR_NAME => acl_logic:from_acl_to_json_format(Acl)
                            };
                        {error, ?ENOATTR} ->
                            prepare_cdmi_metadata(Rest, FileKey, Auth, Attrs, Prefix)
                    end
            end;
        false -> prepare_cdmi_metadata(Rest, Auth, FileKey, Attrs, Prefix)
    end.