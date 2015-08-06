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
-include_lib("ctool/include/logging.hrl").

%% API
-export([random_ascii_lowercase_sequence/1, gen_storage_uid/1, get_parent/1, gen_storage_file_id/1]).
-export([get_local_file_location/1]).


%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Generates storage UID/GID based arbitrary binary (e.g. user's global id, space id, etc)
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_uid(ID :: binary()) -> non_neg_integer().
gen_storage_uid(?ROOT_USER_ID) ->
    0;
gen_storage_uid(ID) ->
    <<UID0:16/big-unsigned-integer-unit:8>> = crypto:hash(md5, ID),
    {ok, LowestUID} = application:get_env(?APP_NAME, lowest_generated_storage_uid),
    {ok, HighestUID} = application:get_env(?APP_NAME, highest_generated_storage_uid),
    LowestUID + UID0 rem HighestUID.


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
    ParentPath = fslogic_path:ensure_path_begins_with_slash(fslogic_path:join(Tokens)),
    {ok, Doc} = file_meta:get({path, ParentPath}),
    Doc;
get_parent(File) ->
    {ok, Doc} = file_meta:get_parent(File),
    Doc.

gen_storage_file_id(Entry) ->
    {ok, Path} = file_meta:gen_path(Entry),
    {ok, #document{value = #file_meta{version = Version}}} = file_meta:get(Entry),
    file_meta:snapshot_name(Path, Version).


get_local_file_location(Entry) ->
    LProviderId = cluster_manager:provider_id(),
    {ok, LocIds} = file_meta:get_locations(Entry),
    Locations = [file_location:get(LocId) || LocId <- LocIds],
    [LocalLocation] = [Location || #document{value = #file_location{provider_id = ProviderId}} = Location <- Locations, LProviderId =:= ProviderId],
    LocalLocation.
