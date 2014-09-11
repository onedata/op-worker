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

-export([get_user_metadata/1, replace_user_metadata/2]).
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
        {ok, XAttrs} -> lists:filter(fun(X) -> not binary_with_prefix(X, <<"cdmi_">>) end, XAttrs);
        _ -> []
    end.

%% replace_user_metadata/1
%% ====================================================================
%% @doc Replaces user metadata associated with file.
%% ====================================================================
-spec replace_user_metadata(Filepath :: string(), [{Name :: binary(), Value :: binary()}]) -> ok.
replace_user_metadata(Filepath, UserMetadata) ->
    {CurrentNames, _Values} = lists:unzip(get_user_metadata(Filepath)),
    % starts with CDMI
    {RequestedNames, _Values} = lists:unzip(UserMetadata),
    lists:map(fun(Name) -> logical_files_manager:remove_xattr(Filepath, Name) end, CurrentNames -- RequestedNames),
    lists:map(fun({Name, Value}) -> logical_files_manager:set_xattr(Filepath, Name, Value) end, UserMetadata),
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
-spec prepare_metadata(Filepath :: string(), Prefix :: binary(), #fileattributes{}) -> [{CdmiName :: binary(), Value :: binary()}].
%% ====================================================================
prepare_metadata(Filepath, Prefix, Attrs) ->
    StorageSystemMetadata = lists:map(fun(X) -> cdmi_metadata_to_attrs(X,Attrs) end, ?default_storage_system_metadata),
    Metadata = lists:append(StorageSystemMetadata, get_user_metadata(Filepath)),
    lists:filter(fun(X) -> binary_with_prefix(X, Prefix) end, Metadata).

%% ====================================================================
%% Internal Functions
%% ====================================================================

%% binary_with_prefix/2
%% ====================================================================
%% @doc Predicate that tells whether a binary starts with given prefix.
%% @end
-spec binary_with_prefix(Name :: binary(), Prefix :: binary()) -> true | false.
%% ====================================================================
binary_with_prefix({Name, _Value}, Prefix) ->
    binary:longest_common_prefix([Name, Prefix]) =:= size(Prefix).

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
