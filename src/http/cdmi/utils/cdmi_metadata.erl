%%%-------------------------------------------------------------------
%%% @author Malgorzata Plazek
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides convenience functions designed for
%%% handling CDMI user metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_metadata).
-author("Malgorzata Plazek").
-author("Bartosz Walkowicz").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

-export([get_user_metadata/2, update_user_metadata/3, update_user_metadata/4]).
-export([prepare_metadata/4]).
-export([
    get_mimetype/2, get_encoding/2, get_cdmi_completion_status/2,
    update_mimetype/3, update_encoding/3, update_cdmi_completion_status/3,
    set_cdmi_completion_status_according_to_partial_flag/3
]).

%% Default values of special cdmi attrs
-define(MIMETYPE_DEFAULT_VALUE, <<"application/octet-stream">>).
-define(ENCODING_DEFAULT_VALUE, <<"base64">>).
-define(COMPLETION_STATUS_DEFAULT_VALUE, <<"Complete">>).

-define(USER_METADATA_FORBIDDEN_PREFIX_STRING, "cdmi_").
-define(USER_METADATA_FORBIDDEN_PREFIX, <<?USER_METADATA_FORBIDDEN_PREFIX_STRING>>).
-define(DEFAULT_STORAGE_SYSTEM_METADATA, [
    <<"cdmi_size">>, <<"cdmi_ctime">>, <<"cdmi_atime">>,
    <<"cdmi_mtime">>, <<"cdmi_owner">>, ?ACL_XATTR_NAME
]).

-define(run(__FunctionCall), check_result(__FunctionCall)).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Gets user metadata associated with file, which are all xattrs
%% without "cdmi_" prefix.
%% @end
%%--------------------------------------------------------------------
-spec get_user_metadata(session:id(), lfm:file_key()) -> map().
get_user_metadata(SessionId, FileKey) ->
    {ok, Names} = ?run(lfm:list_xattr(SessionId, FileKey, false, true)),
    Metadata = lists:foldl(
        fun
            (<<?USER_METADATA_FORBIDDEN_PREFIX_STRING, _/binary>>, Acc) ->
                Acc;
            (Name, Acc) ->
                case lfm:get_xattr(SessionId, FileKey, Name, false) of
                    {ok, #xattr{value = XattrValue}} ->
                        Acc#{Name => XattrValue};
                    {error, ?ENOATTR} ->
                        Acc;
                    {error, Errno} ->
                        throw(?ERROR_POSIX(Errno))
                end
        end,
        #{},
        Names
    ),
    filter_user_metadata_map(Metadata).


%%--------------------------------------------------------------------
%% @equiv update_user_metadata(Auth, FileKey, UserMetadata, []).
%%--------------------------------------------------------------------
-spec update_user_metadata(session:id(), lfm:file_key(), map()) -> ok.
update_user_metadata(SessionId, FileKey, UserMetadata) ->
    update_user_metadata(SessionId, FileKey, UserMetadata, []).


%%--------------------------------------------------------------------
%% @doc
%% Updates user metadata listed in URIMetadataNames associated with file.
%% If a metadata name specified in URIMetadataNames, but has no corresponding
%% entry in UserMetadata, entry is removed from user metadata associated with
%% a file.
%% @end
%%--------------------------------------------------------------------
-spec update_user_metadata(session:id(), lfm:file_key(),
    UserMetadata :: undefined | map(), URIMetadataNames :: [Name :: binary()]) ->
    ok | no_return().
update_user_metadata(_SessionId, _FileKey, undefined, []) ->
    ok;
update_user_metadata(SessionId, FileKey, undefined, URIMetadataNames) ->
    update_user_metadata(SessionId, FileKey, #{}, URIMetadataNames);
update_user_metadata(SessionId, FileKey, UserMetadata, AllURIMetadataNames) ->
    BodyMetadata = filter_user_metadata_map(UserMetadata),
    BodyMetadataNames = maps:keys(BodyMetadata),
    DeleteAttributeFunction =
        fun
            (?ACL_XATTR_NAME) ->
                ?run(lfm:remove_acl(SessionId, FileKey));
            (Name) ->
                ?run(lfm:remove_xattr(SessionId, FileKey, Name))
        end,
    ReplaceAttributeFunction =
        fun
            ({?ACL_XATTR_NAME, Value}) ->
                ACL = try acl_logic:from_json_format_to_acl(Value)
                catch _:Error ->
                    ?warning_stacktrace("Acl conversion error ~p", [Error]),
                    throw(?ERROR_BAD_DATA(<<"acl">>))
                end,
                ?run(lfm:set_acl(SessionId, FileKey, ACL));
            ({Name, Value}) ->
                ?run(lfm:set_xattr(
                    SessionId, FileKey,
                    #xattr{name = Name, value = Value},
                    false, false
                ))
        end,
    case AllURIMetadataNames of
        [] ->
            lists:foreach(
                DeleteAttributeFunction,
                maps:keys(get_user_metadata(SessionId, FileKey)) -- BodyMetadataNames
            ),
            lists:foreach(ReplaceAttributeFunction, maps:to_list(BodyMetadata));
        _ ->
            UriMetadataNames = filter_user_metadata_keylist(AllURIMetadataNames),
            lists:foreach(
                DeleteAttributeFunction,
                UriMetadataNames -- BodyMetadataNames
            ),
            lists:foreach(
                ReplaceAttributeFunction,
                maps:to_list(filter_URI_Names(BodyMetadata, UriMetadataNames))
            )
    end.


%%--------------------------------------------------------------------
%% @doc Prepares cdmi user and storage system metadata with given prefix.
%%--------------------------------------------------------------------
-spec prepare_metadata(session:id(), FileKey :: lfm:file_key(),
    Prefix :: binary(), #file_attr{}) -> map().
prepare_metadata(SessionId, FileKey, Prefix, Attrs) ->
    StorageSystemMetadata = prepare_cdmi_metadata(
        ?DEFAULT_STORAGE_SYSTEM_METADATA, FileKey,
        SessionId, Attrs, Prefix
    ),
    UserMetadata = maps:filter(fun(Name, _Value) ->
        str_utils:binary_starts_with(Name, Prefix)
    end, get_user_metadata(SessionId, FileKey)),
    maps:merge(StorageSystemMetadata, UserMetadata).


%%--------------------------------------------------------------------
%% @doc
%% Gets mimetype associated with file, returns default value if
%% no mimetype could be found.
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(session:id(), lfm:file_key()) -> binary().
get_mimetype(SessionId, FileKey) ->
    case lfm:get_mimetype(SessionId, FileKey) of
        {ok, Value} ->
            Value;
        {error, ?ENOATTR} ->
            ?MIMETYPE_DEFAULT_VALUE;
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets value_transfer_encoding associated with file, returns default value
%% if no value_transfer_encoding could be found.
%% @end
%%--------------------------------------------------------------------
-spec get_encoding(session:id(), lfm:file_key()) -> binary().
get_encoding(SessionId, FileKey) ->
    case lfm:get_transfer_encoding(SessionId, FileKey) of
        {ok, Value} ->
            Value;
        {error, ?ENOATTR} ->
            ?ENCODING_DEFAULT_VALUE;
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets completion status associated with file, returns default value if
%% no completion status could be found. The result can be:
%% binary("Complete") | binary("Processing") | binary("Error")
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(session:id(), lfm:file_key()) -> binary().
get_cdmi_completion_status(SessionId, FileKey) ->
    case lfm:get_cdmi_completion_status(SessionId, FileKey) of
        {ok, Value} ->
            Value;
        {error, ?ENOATTR} ->
            ?COMPLETION_STATUS_DEFAULT_VALUE;
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


%%--------------------------------------------------------------------
%% @doc Updates mimetype associated with file
%%--------------------------------------------------------------------
-spec update_mimetype(session:id(), lfm:file_key(), binary()) ->
    ok | no_return().
update_mimetype(_SessionId, _FileKey, undefined) ->
    ok;
update_mimetype(SessionId, FileKey, Mimetype) ->
    ?run(lfm:set_mimetype(SessionId, FileKey, Mimetype)).


%%--------------------------------------------------------------------
%% @doc Updates value_transfer_encoding associated with file.
%%--------------------------------------------------------------------
-spec update_encoding(session:id(), lfm:file_key(), binary() | undefined) ->
    ok | no_return().
update_encoding(_SessionId, _FileKey, undefined) ->
    ok;
update_encoding(SessionId, FileKey, Encoding) ->
    ?run(lfm:set_transfer_encoding(SessionId, FileKey, Encoding)).


%%--------------------------------------------------------------------
%% @doc Updates completion status associated with file
%%--------------------------------------------------------------------
-spec update_cdmi_completion_status(session:id(), lfm:file_key(), binary()) ->
    ok | no_return().
update_cdmi_completion_status(_SessionId, _FileKey, undefined) ->
    ok;
update_cdmi_completion_status(SessionId, FileKey, CompletionStatus) when
    CompletionStatus =:= <<"Complete">>;
    CompletionStatus =:= <<"Processing">>;
    CompletionStatus =:= <<"Error">>
->
    ?run(lfm:set_cdmi_completion_status(SessionId, FileKey, CompletionStatus)).


%%--------------------------------------------------------------------
%% @doc
%% Updates completion status associated with file
%% according to X-CDMI-Partial flag
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status_according_to_partial_flag(session:id(), lfm:file_key(), binary()) ->
    ok | no_return().
set_cdmi_completion_status_according_to_partial_flag(_SessionId, _FileKey, <<"true">>) ->
    ok;
set_cdmi_completion_status_according_to_partial_flag(SessionId, FileKey, _) ->
    update_cdmi_completion_status(SessionId, FileKey, <<"Complete">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Filters out metadata with user_metadata_forbidden_prefix.
%% @end
%%--------------------------------------------------------------------
-spec filter_user_metadata_map(map()) -> map().
filter_user_metadata_map(UserMetadata) when is_map(UserMetadata) ->
    maps:filter(
        fun
            (?ACL_XATTR_NAME, _Value) ->
                true;
            (Name, _Value) ->
                not str_utils:binary_starts_with(Name, ?USER_METADATA_FORBIDDEN_PREFIX)
        end,
        UserMetadata
    );
filter_user_metadata_map(_) ->
    throw(?ERROR_BAD_DATA(<<"metadata">>)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Filters out metadata with user_metadata_forbidden_prefix.
%% @end
%%--------------------------------------------------------------------
-spec filter_user_metadata_keylist(list()) -> list().
filter_user_metadata_keylist(UserMetadata) when is_list(UserMetadata) ->
    lists:filter(
        fun
            (?ACL_XATTR_NAME) ->
                true;
            (Name) ->
                not str_utils:binary_starts_with(Name, ?USER_METADATA_FORBIDDEN_PREFIX)
        end,
        UserMetadata
    );
filter_user_metadata_keylist(_) ->
    throw(?ERROR_BAD_DATA(<<"metadata">>)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Filters metadata with names contained in URIMetadataNames list.
%% @end
%%--------------------------------------------------------------------
-spec filter_URI_Names(map(), [CdmiName :: binary()]) -> map().
filter_URI_Names(UserMetadata, URIMetadataNames) ->
    maps:filter(fun(Name, _) ->
        lists:member(Name, URIMetadataNames)
    end, UserMetadata).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns system metadata with given prefix, in mochijson parser format.
%% @end
%%--------------------------------------------------------------------
-spec prepare_cdmi_metadata(MetadataNames :: [binary()], lfm:file_key(),
    session:id(), #file_attr{}, Prefix :: binary()) -> map().
prepare_cdmi_metadata([], _FileKey, _SessionId, _Attrs, _Prefix) ->
    #{};
prepare_cdmi_metadata([Name | Rest], FileKey, SessionId, Attrs, Prefix) ->
    case str_utils:binary_starts_with(Name, Prefix) of
        true ->
            case Name of
                <<"cdmi_size">> -> %todo clarify what should be written to cdmi_size for directories
                    (prepare_cdmi_metadata(Rest, FileKey, SessionId, Attrs, Prefix))#{
                        <<"cdmi_size">> => integer_to_binary(Attrs#file_attr.size)
                    };
                <<"cdmi_ctime">> ->
                    (prepare_cdmi_metadata(Rest, FileKey, SessionId, Attrs, Prefix))#{
                        <<"cdmi_ctime">> => time_utils:epoch_to_iso8601(Attrs#file_attr.ctime)
                    };
                <<"cdmi_atime">> ->
                    (prepare_cdmi_metadata(Rest, FileKey, SessionId, Attrs, Prefix))#{
                        <<"cdmi_atime">> => time_utils:epoch_to_iso8601(Attrs#file_attr.atime)
                    };
                <<"cdmi_mtime">> ->
                    (prepare_cdmi_metadata(Rest, FileKey, SessionId, Attrs, Prefix))#{
                        <<"cdmi_mtime">> => time_utils:epoch_to_iso8601(Attrs#file_attr.mtime)
                    };
                <<"cdmi_owner">> ->
                    (prepare_cdmi_metadata(Rest, FileKey, SessionId, Attrs, Prefix))#{
                        <<"cdmi_owner">> => Attrs#file_attr.owner_id
                    };
                ?ACL_XATTR_NAME ->
                    case lfm:get_acl(SessionId, FileKey) of
                        {ok, Acl} ->
                            (prepare_cdmi_metadata(Rest, FileKey, SessionId, Attrs, Prefix))#{
                                ?ACL_XATTR_NAME => acl_logic:from_acl_to_json_format(Acl)
                            };
                        {error, ?ENOATTR} ->
                            prepare_cdmi_metadata(Rest, FileKey, SessionId, Attrs, Prefix);
                        {error, Errno} ->
                            throw(?ERROR_POSIX(Errno))
                    end
            end;
        false ->
            prepare_cdmi_metadata(Rest, SessionId, FileKey, Attrs, Prefix)
    end.


%% @private
-spec check_result(ok | {ok, term()} | {error, term()}) ->
    ok | {ok, term()} | no_return().
check_result(ok) -> ok;
check_result({ok, _} = Res) -> Res;
check_result({error, Errno}) -> throw(?ERROR_POSIX(Errno)).
