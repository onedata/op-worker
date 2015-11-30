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

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets user matadata associated with file, which are all xattrs
%% without "cdmi_" prefix.
%% @end
%%--------------------------------------------------------------------
-spec get_user_metadata(Filepath :: onedata_file_api:file_path()) ->
    [{Name :: binary(), Value :: binary()}].
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