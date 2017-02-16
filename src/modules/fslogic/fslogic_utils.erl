%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @todo remove this module
%% @doc This module exports utility tools for fslogic
%% @end
%% ===================================================================
-module(fslogic_utils).

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").

%% API
-export([gen_storage_file_id/1, gen_storage_file_id/2, gen_storage_file_id/3]).
-export([get_local_file_location/1, get_local_file_locations/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec gen_storage_file_id(Entry :: fslogic_worker:file()) ->
    helpers:file() | no_return().
gen_storage_file_id(Entry) -> %todo refactor
    {ok, Path} = gen_storage_path(Entry),
    gen_storage_file_id(Entry, Path).

-spec gen_storage_file_id(Entry :: fslogic_worker:file(), Path :: file_meta:path()) ->
    helpers:file() | no_return().
gen_storage_file_id(Entry, Path) ->
    {ok, #document{key = Key, value = #file_meta{version = Version}}} = file_meta:get(Entry),
    gen_storage_file_id(Key, Path, Version).

-spec gen_storage_file_id(Entry :: fslogic_worker:file(), Path :: file_meta:path(), Version :: non_neg_integer()) ->
    helpers:file() | no_return().
gen_storage_file_id(_Entry, Path, 0) ->
    Path;
gen_storage_file_id(_Entry, Path, Version) when is_integer(Version) ->
    file_meta:snapshot_name(Path, Version).

-spec get_local_file_location(fslogic_worker:ext_file()) ->
    datastore:document() | no_return().
get_local_file_location(Entry) -> %%todo VFS-2813 support multi location, get rid of single file location and use get_local_file_locations/1
    [LocalLocation] = get_local_file_locations(Entry),
    LocalLocation.

-spec get_local_file_locations(fslogic_worker:ext_file()) ->
    [datastore:document()] | no_return().
get_local_file_locations({guid, FileGUID}) ->
    get_local_file_locations({uuid, fslogic_uuid:guid_to_uuid(FileGUID)});
get_local_file_locations(Entry) ->
    LProviderId = oneprovider:get_provider_id(),
    {ok, LocIds} = file_meta:get_locations(Entry),
    Locations = [file_location:get(LocId) || LocId <- LocIds],
    [Location ||
        {ok, Location = #document{value = #file_location{provider_id = ProviderId}}}
            <- Locations, LProviderId =:= ProviderId
    ].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generate storage file_meta:path() for given file_meta:entry()
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_path(file_meta:entry()) ->
    {ok, file_meta:path()}.
gen_storage_path({path, Path}) when is_binary(Path) ->
    {ok, Path};
gen_storage_path(Entry) ->
    gen_storage_path(Entry, []).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal helper for gen_storage_path/1. Accumulates all file meta names
%% and concatenates them into storage path().
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_path(file_meta:entry(), [file_meta:name()]) ->
    {ok, file_meta:path()} | datastore:generic_error() | no_return().
gen_storage_path(Entry, Tokens) ->
    {ok, #document{value = #file_meta{name = Name}} = Doc} = file_meta:get(Entry),
    case file_meta:get_parent(Doc) of
        {ok, #document{key = ?ROOT_DIR_UUID}} ->
            {ok, fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, Name | Tokens])};
        {ok, #document{key = ParentUUID}} ->
            gen_storage_path({uuid, ParentUUID}, [Name | Tokens])
    end.