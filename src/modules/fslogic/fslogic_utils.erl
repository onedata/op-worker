%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module exports utility tools for fslogic
%% @end
%% ===================================================================
-module(fslogic_utils).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").

%% API
-export([random_ascii_lowercase_sequence/1, get_parent/1, gen_storage_file_id/1]).
-export([get_local_file_location/1, get_local_file_locations/1, get_local_storage_file_locations/1]).
-export([wait_for_links/2, wait_for_file_meta/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Create random sequence consisting of lowercase ASCII letters.
%%--------------------------------------------------------------------
-spec random_ascii_lowercase_sequence(Length :: integer()) -> list().
random_ascii_lowercase_sequence(Length) ->
    lists:foldl(fun(_, Acc) -> [random:uniform(26) + 96 | Acc] end, [], lists:seq(1, Length)).


%%--------------------------------------------------------------------
%% @doc Returns parent of given file.
%%--------------------------------------------------------------------
-spec get_parent(fslogic_worker:file()) -> fslogic_worker:file() | no_return().
get_parent({path, Path}) ->
    [_ | R] = lists:reverse(fslogic_path:split(Path)),
    Tokens = lists:reverse(R),
    ParentPath = filepath_utils:ensure_begins_with_prefix(fslogic_path:join(Tokens), ?DIRECTORY_SEPARATOR_BINARY),
    {ok, Doc} = file_meta:get({path, ParentPath}),
    Doc;
get_parent(File) ->
    {ok, Doc} = file_meta:get_parent(File),
    Doc.

-spec gen_storage_file_id(Entry :: fslogic_worker:file()) ->
    helpers:file() | no_return().
gen_storage_file_id(Entry) ->
    {ok, Path} = file_meta:gen_storage_path(Entry),
    {ok, #document{value = #file_meta{version = Version}}} = file_meta:get(Entry),
    file_meta:snapshot_name(Path, Version).


-spec get_local_file_location(fslogic_worker:file()) ->
    datastore:document() | no_return().
get_local_file_location(Entry) -> %todo get rid of single file location and use get_local_file_locations/1
    [LocalLocation] = get_local_file_locations(Entry),
    LocalLocation.


-spec get_local_file_locations(fslogic_worker:file()) ->
    [datastore:document()] | no_return().
get_local_file_locations(Entry) ->
    LProviderId = oneprovider:get_provider_id(),
    {ok, LocIds} = file_meta:get_locations(Entry),
    Locations = [file_location:get(LocId) || LocId <- LocIds],
    [Location ||
        {ok, Location = #document{value = #file_location{provider_id = ProviderId}}}
            <- Locations, LProviderId =:= ProviderId
    ].


-spec get_local_storage_file_locations(datastore:document() | #file_location{} | fslogic_worker:file()) ->
    [{storage:id(), helpers:file()}] | no_return().
get_local_storage_file_locations(#document{value = #file_location{} = Location}) ->
    get_local_storage_file_locations(Location);
get_local_storage_file_locations(#file_location{blocks = Blocks, storage_id = DSID, file_id = DFID}) ->
    lists:usort([{DSID, DFID} | [{SID, FID} || #file_block{storage_id = SID, file_id = FID} <- Blocks]]);
get_local_storage_file_locations(Entry) ->
    #document{} = Doc = get_local_file_location(Entry),
    get_local_storage_file_locations(Doc).


%%--------------------------------------------------------------------
%% @doc
%% Waiting for links document associated with file_meta to be present.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_links(file_meta:uuid(), non_neg_integer()) -> ok | no_return().
wait_for_links(FileUuid, 0) ->
    ?error("Waiting for links document, for file ~p failed.", [FileUuid]),
    throw(no_link_document);
wait_for_links(FileUuid, Retries) ->
    case file_meta:exists({uuid, links_utils:links_doc_key(FileUuid)}) of
        true ->
            ok;
        false ->
            timer:sleep(timer:seconds(1)),
            wait_for_links(FileUuid, Retries - 1)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Waiting for file_meta with given file_uuid to be present.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_file_meta(file_meta:uuid(), non_neg_integer()) -> ok | no_return().
wait_for_file_meta(FileUuid, 0) ->
    ?error("Waiting for file_meta ~p failed.", [FileUuid]),
    throw(no_file_meta_document);
wait_for_file_meta(FileUuid, Retries) ->
    case file_meta:exists({uuid, FileUuid}) of
        true ->
            ok;
        false ->
            timer:sleep(timer:seconds(1)),
            wait_for_links(FileUuid, Retries - 1)
    end.