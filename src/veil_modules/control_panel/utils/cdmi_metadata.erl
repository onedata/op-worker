%% ===================================================================
%% @author Malgorzata Plazek
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides convinience functions designed for
%% handling CDMI user metadata.
%% @end
%% ===================================================================

-module(cdmi_metadata).

-include("veil_modules/control_panel/cdmi_metadata.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

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
-spec get_user_metadata(Filepath :: string()) -> [{Name :: binary(), Value :: binary()}].
%% ====================================================================
get_user_metadata(Filepath) ->
    case logical_files_manager:list_xattr(Filepath) of
        {ok, XAttrs} ->
            filter_user_metadata(XAttrs);
        _ -> []
    end.

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
    URIMetadataNames :: [Name :: binary()]) -> ok.
update_user_metadata(Filepath, undefined, URIMetadataNames) ->
    update_user_metadata(Filepath, [], URIMetadataNames);
update_user_metadata(Filepath, UserMetadata, URIMetadataNames) ->
    UserMetadataFiltered = filter_user_metadata(UserMetadata),
    UserMetadataNamesFiltered = get_metadata_names(UserMetadataFiltered),
    case URIMetadataNames of
        [] ->
            lists:map(fun(Name) -> logical_files_manager:remove_xattr(Filepath, Name) end,
                get_metadata_names(get_user_metadata(Filepath)) -- UserMetadataNamesFiltered),
            lists:map(fun({Name, Value}) -> logical_files_manager:set_xattr(Filepath, Name, Value) end,
                UserMetadataFiltered);
        _ ->
            RequestedNamesFiltered = filter_user_metadata(URIMetadataNames),
            lists:map(fun(Name) -> logical_files_manager:remove_xattr(Filepath, Name) end,
                RequestedNamesFiltered -- UserMetadataNamesFiltered),
            lists:map(fun({Name, Value}) -> logical_files_manager:set_xattr(Filepath, Name, Value) end,
                filter_URI_Names(UserMetadataFiltered, RequestedNamesFiltered))
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
    StorageSystemMetadata = lists:map(fun(X) -> cdmi_metadata_to_attrs(X,Attrs) end, ?default_storage_system_metadata),
    Metadata = lists:append(StorageSystemMetadata, get_user_metadata(Filepath)),
    lists:filter(fun({Name, _Value}) -> binary_with_prefix(Name, Prefix) end, Metadata).

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
            ({Name, _Value}) -> not binary_with_prefix(Name, ?user_metadata_forbidden_prefix);
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

%% cdmi_metadata_to_attrs/2
%% ====================================================================
%% @doc Extracts cdmi metadata from file attributes.
%% @end
-spec cdmi_metadata_to_attrs(CdmiName :: binary(), #fileattributes{}) -> {CdmiName :: binary(), Value :: binary()}.
%% ====================================================================
%todo add cdmi_acl metadata
%todo clarify what should be written to cdmi_size for directories
cdmi_metadata_to_attrs(<<"cdmi_size">>, Attrs) ->
    {<<"cdmi_size">>, integer_to_binary(Attrs#fileattributes.size)};
%todo format times into yyyy-mm-ddThh-mm-ss.ssssssZ
cdmi_metadata_to_attrs(<<"cdmi_ctime">>, Attrs) ->
    {<<"cdmi_ctime">>, integer_to_binary(Attrs#fileattributes.ctime)};
cdmi_metadata_to_attrs(<<"cdmi_atime">>, Attrs) ->
    {<<"cdmi_atime">>, integer_to_binary(Attrs#fileattributes.atime)};
cdmi_metadata_to_attrs(<<"cdmi_mtime">>, Attrs) ->
    {<<"cdmi_mtime">>, integer_to_binary(Attrs#fileattributes.mtime)};
cdmi_metadata_to_attrs(<<"cdmi_owner">>, Attrs) ->
    {<<"cdmi_owner">>, list_to_binary(Attrs#fileattributes.uname)};
cdmi_metadata_to_attrs(_,_Attrs) ->
    {}.
