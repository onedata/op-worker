%% ===================================================================
%% @author Malgorzata Plazek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides convinience functions designed for
%% handling CDMI user metadata.
%% @end
%% ===================================================================

-module(cdmi_metadata).

<<<<<<< HEAD:src/veil_modules/control_panel/utils/cdmi_metadata.erl
-include("veil_modules/control_panel/cdmi_metadata.hrl").
-include("veil_modules/control_panel/cdmi_error.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
=======
-include("oneprovider_modules/control_panel/cdmi_metadata.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
>>>>>>> develop:src/oneprovider_modules/control_panel/utils/cdmi_metadata.erl

-export([get_user_metadata/1, update_user_metadata/2, update_user_metadata/3]).
-export([prepare_metadata/2, prepare_metadata/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% get_user_metadata/1
%% ====================================================================
%% @doc Gets user matadata associated with file, which are all xattrs
%% without "cdmi_" prefix.
%% @end
-spec get_user_metadata(Filepath :: string()) -> [{Name :: binary(), Value :: binary()}] | no_return().
%% ====================================================================
get_user_metadata(Filepath) ->
    {ok, XAttrs} = logical_files_manager:list_xattr(Filepath),
    filter_user_metadata(XAttrs).

%% update_user_metadata/2
%% ====================================================================
%% @doc Replaces user metadata associated with file.
%% @equiv update_user_metadata(Filepath,UserMetadata,[])
%% @end
%% ====================================================================
-spec update_user_metadata(Filepath :: string(), UserMetadata :: [{Name :: binary(), Value :: binary()}]) -> ok.
update_user_metadata(Filepath, UserMetadata) ->
    update_user_metadata(Filepath, UserMetadata, []).

%% update_user_metadata/3
%% ====================================================================
%% @doc Updates user metadata listed in URIMetadataNames associated with file.
%% If a matedata name specified in URIMetadataNames, but has no corresponding entry in UserMetadata, entry is removed
%% from user metadata associated with a file,
%% ====================================================================
-spec update_user_metadata(Filepath :: string(), UserMetadata :: [{Name :: binary(), Value :: binary()}] | undefined,
    URIMetadataNames :: [Name :: binary()]) -> ok | no_return().
update_user_metadata(_Filepath, undefined, []) -> ok;
update_user_metadata(Filepath, undefined, URIMetadataNames) ->
    update_user_metadata(Filepath, [], URIMetadataNames);
update_user_metadata(Filepath, UserMetadata, AllURIMetadataNames) ->
    BodyMetadata = filter_user_metadata(UserMetadata),
    BodyMetadataNames = get_metadata_names(BodyMetadata),
    DeleteAttributeFunction =
        fun
            (<<"cdmi_acl">>) -> ok = logical_files_manager:set_acl(Filepath, []);
            (Name) -> ok = logical_files_manager:remove_xattr(Filepath, Name)
        end,
    ReplaceAttributeFunction =
        fun
            ({<<"cdmi_acl">>, Value}) -> ok = logical_files_manager:set_acl(Filepath, fslogic_acl:from_json_fromat_to_acl(Value));
            ({Name, Value}) -> ok = logical_files_manager:set_xattr(Filepath, Name, Value)
        end,
    case AllURIMetadataNames of
        [] ->
            lists:foreach(DeleteAttributeFunction, get_metadata_names(get_user_metadata(Filepath)) -- BodyMetadataNames),
            lists:foreach(ReplaceAttributeFunction, BodyMetadata);
        _ ->
            UriMetadataNames = filter_user_metadata(AllURIMetadataNames),
            lists:foreach(DeleteAttributeFunction, UriMetadataNames -- BodyMetadataNames),
            lists:foreach(ReplaceAttributeFunction, filter_URI_Names(BodyMetadata, UriMetadataNames))
    end,
    ok.

%% prepare_metadata/2
%% ====================================================================
%% @doc Prepares cdmi user and storage system metadata.
%% @end
-spec prepare_metadata(Filepath :: string(), #fileattributes{}) -> [{CdmiName :: binary(), Value :: binary()}].
%% ====================================================================
prepare_metadata(Filepath, Attrs) ->
    prepare_metadata(Filepath, <<"">>, Attrs).

%% prepare_metadata/3
%% ====================================================================
%% @doc Prepares cdmi user and storage system metadata with given prefix.
%% @end
-spec prepare_metadata(Filepath :: string(), Prefix :: binary(), #fileattributes{}) ->
    [{CdmiName :: binary(), Value :: binary()}].
%% ====================================================================
prepare_metadata(Filepath, Prefix, Attrs) ->
    StorageSystemMetadata = prepare_cdmi_metadata(?default_storage_system_metadata, Filepath, Attrs, Prefix),
    UserMetadata = lists:filter(fun({Name, _Value}) -> binary_with_prefix(Name, Prefix) end, get_user_metadata(Filepath)),
    StorageSystemMetadata ++ UserMetadata.

%% ====================================================================
%% Internal Functions
%% ====================================================================

%% binary_with_prefix/2
%% ====================================================================
%% @doc Predicate that tells whether binary starts with given prefix.
%% @end
-spec binary_with_prefix(Name :: binary(), Prefix :: binary()) -> true | false.
%% ====================================================================
binary_with_prefix(Name, Prefix) ->
    binary:longest_common_prefix([Name, Prefix]) =:= size(Prefix).

%% get_metadata_names/1
%% ====================================================================
%% @doc Returns first list from unzip result.
%% @end
-spec get_metadata_names([{A, B}]) -> [A] when A :: term(), B :: term().
%% ====================================================================
get_metadata_names(TupleList) ->
    {Result, _} = lists:unzip(TupleList),
    Result.

%% filter_user_metadata/1
%% ====================================================================
%% @doc Filters out metadata with user_metadata_forbidden_prefix.
%% @end
-spec filter_user_metadata(UserMetadata) -> UserMetadata when
    UserMetadata :: [{CdmiName :: binary(), Value :: binary()}] | [CdmiName :: binary()].
%% ====================================================================
filter_user_metadata(UserMetadata) ->
    lists:filter(
        fun
            ({<<"cdmi_acl">>, _Value}) -> true;
            ({Name, _Value}) -> not binary_with_prefix(Name, ?user_metadata_forbidden_prefix);
            (<<"cdmi_acl">>) -> true;
            (Name) -> not binary_with_prefix(Name, ?user_metadata_forbidden_prefix)
        end,
        UserMetadata).

%% filter_URI_Names/2
%% ====================================================================
%% @doc Filters metadata with names contained in URIMetadataNames list.
%% @end
-spec filter_URI_Names(UserMetadata, URIMetadataNames :: [CdmiName]) -> UserMetadata when
    UserMetadata :: [{CdmiName, Value :: binary()}], CdmiName :: binary().
%% ====================================================================
filter_URI_Names(UserMetadata, URIMetadataNames) ->
    [{Name,Value} || URIName <- URIMetadataNames, {Name, Value} <- UserMetadata, URIName == Name].

%% prepare_cdmi_metadata/4
%% ====================================================================
%% @doc Returns system metadata with given prefix, in mochijson parser format
%% @end
-spec prepare_cdmi_metadata(MetadataNames :: [binary()], Filepath :: string(), Attrs :: #fileattributes{}, Prefix :: binary()) -> list().
%% ====================================================================
prepare_cdmi_metadata([], _Filepath, _Attrs, _Prefix) -> [];
prepare_cdmi_metadata([Name | Rest], Filepath, Attrs, Prefix) ->
    case binary_with_prefix(Name, Prefix) of
        true ->
            case Name of
                <<"cdmi_size">> -> %todo clarify what should be written to cdmi_size for directories
                    [{<<"cdmi_size">>, integer_to_binary(Attrs#fileattributes.size)} | prepare_cdmi_metadata(Rest, Filepath, Attrs, Prefix)];
                <<"cdmi_ctime">> -> %todo format times into yyyy-mm-ddThh-mm-ss.ssssssZ
                    [{<<"cdmi_ctime">>, integer_to_binary(Attrs#fileattributes.ctime)} | prepare_cdmi_metadata(Rest, Filepath, Attrs, Prefix)];
                <<"cdmi_atime">> ->
                    [{<<"cdmi_atime">>, integer_to_binary(Attrs#fileattributes.atime)} | prepare_cdmi_metadata(Rest, Filepath, Attrs, Prefix)];
                <<"cdmi_mtime">> ->
                    [{<<"cdmi_mtime">>, integer_to_binary(Attrs#fileattributes.mtime)} | prepare_cdmi_metadata(Rest, Filepath, Attrs, Prefix)];
                <<"cdmi_owner">> ->
                    [{<<"cdmi_owner">>, list_to_binary(Attrs#fileattributes.uname)} | prepare_cdmi_metadata(Rest, Filepath, Attrs, Prefix)];
                <<"cdmi_acl">> ->
                    case logical_files_manager:get_acl(Filepath) of
                        {ok, Acl} ->
                            [{<<"cdmi_acl">>, fslogic_acl:from_acl_to_json_format(Acl)} | prepare_cdmi_metadata(Rest, Filepath, Attrs, Prefix)];
                        {logical_file_system_error, Err} when Err =:= ?VEPERM orelse Err =:= ?VEACCES ->
                            throw(?forbidden);
                        Error -> throw(Error)
                    end
            end;
        false -> prepare_cdmi_metadata(Rest, Filepath, Attrs, Prefix)
    end.
