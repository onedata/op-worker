%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for effective checking synchronization status of file_meta and file_meta links documents.
%%% For given file synchronization of all files on this file path is checked. Hardlinks are NOT checked.
%%% Uses `effective_cache` under the hood.
%%% TODO VFS-7412 refactor this module (duplicated code in other effective_ caches modules)
%%% There are 3 possible results:
%%%     * synced - all documents on path to given file are synchronized;
%%%     * ?MISSING_FILE_META(MissingUuid) - there is at least one file meta document missing, links status is unknown.
%%%       Returned uuid is the last missing on path (there is guarantee, that all file_meta documents on path after
%%%       this one are synchronized);
%%%     * ?MISSING_FILE_META_LINK(ParentUuid, Name) - there is at least one link document missing, all file_meta
%%%       documents are synced. Returned link is the last missing on path (there is guarantee, that all link documents
%%%       on path after this one are synchronized);
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_sync_status_cache).
-author("Michał Stanisz").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_group/0, init/1, invalidate_on_all_nodes/1]).
-export([get/2, get/3]).
%% RPC API
-export([invalidate/1]).

-type missing_file_meta() :: ?MISSING_FILE_META(file_meta:uuid()).
-type missing_link() :: ?MISSING_FILE_LINK(file_meta:uuid(), file_meta:name()).

-define(CACHE_GROUP, <<"file_meta_links_sync_status_cache_group">>).
-define(CACHE_NAME(SpaceId),
    binary_to_atom(<<"file_meta_links_effective_cache_", SpaceId/binary>>, utf8)).

-define(CACHE_SIZE, op_worker:get_env(file_meta_links_eff_cache_size, 65536)).
-define(CHECK_FREQUENCY, op_worker:get_env(file_meta_links_cache_check_frequency, 30000)).
-define(CACHE_OPTS, #{group => ?CACHE_GROUP}).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_group() -> ok.
init_group() ->
    ok = effective_value:init_group(?CACHE_GROUP, #{
        check_frequency => ?CHECK_FREQUENCY,
        size => ?CACHE_SIZE,
        worker => false
    }).


-spec init(od_space:id() | all) -> ok.
init(all) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:foreach(fun init/1, SpaceIds);
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            ?debug("Unable to initialize file_meta links caches due to: ~tp", [?ERROR_NO_CONNECTION_TO_ONEZONE]);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Unable to initialize file_meta links caches due to: ~tp", [?ERROR_UNREGISTERED_ONEPROVIDER]);
        Error = {error, _} ->
            ?critical("Unable to initialize file_meta links caches due to: ~tp", [Error])
    catch
        Class:Reason:Stacktrace ->
            ?critical_exception("Unable to initialize file_meta link caches", Class, Reason, Stacktrace)
    end;
init(SpaceId) ->
    CacheName = ?CACHE_NAME(SpaceId),
    try
        case effective_value:cache_exists(CacheName) of
            true ->
                ok;
            _ ->
                case effective_value:init_cache(CacheName, ?CACHE_OPTS) of
                    ok ->
                        ok;
                    Error = {error, _} ->
                        ?critical("Unable to initialize file_meta links effective cache for space ~tp due to: ~tp",
                            [SpaceId, Error])
                end
        end
    catch
        Class:Reason:Stacktrace ->
            ?critical_exception(
                "Unable to initialize file_meta links effective cache for space ~tp", [SpaceId],
                Class, Reason, Stacktrace
            )
    end.


-spec invalidate_on_all_nodes(od_space:id()) -> ok.
invalidate_on_all_nodes(SpaceId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = utils:rpc_multicall(Nodes, ?MODULE, invalidate, [SpaceId]),
    
    case BadNodes of
        [] ->
            ok;
        _ ->
            ?error("Invalidation of file_meta links caches for space ~tp failed on nodes: ~tp (RPC error)", [SpaceId, BadNodes])
    end,
    
    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?error(
                "Invalidation of file_meta links caches for space ~tp failed.~n"
                "Reason: ~tp", [SpaceId, Error]
            )
    end, Res).


-spec get(od_space:id(), file_meta:uuid() | file_meta:doc()) ->
    {ok, synced} | {error, missing_file_meta() | missing_link()} | {error, term()}.
get(SpaceId, DocOrUuid) ->
    get(SpaceId, DocOrUuid, #{}).


-spec get(od_space:id(), file_meta:uuid() | file_meta:doc(), effective_value:get_or_calculate_options()) ->
    {ok, synced} | {error, missing_file_meta() | missing_link() | ancestor_deleted} | {error, term()}.
get(SpaceId, Doc = #document{value = #file_meta{}, scope = Scope}, Opts) ->
    CacheName = ?CACHE_NAME(SpaceId),
    Opts2 = Opts#{get_remote_from_scope => Scope},
    case effective_value:get_or_calculate(CacheName, Doc, fun calculate_links_sync_status/1, Opts2) of
        {ok, synced, _} ->
            {ok, synced};
        {error, ?MISSING_FILE_LINK(_, _) = MissingLink} ->
            {error, find_lowest_missing_link(MissingLink, Doc)};
        {error, _} = Error ->
            Error
    end;
get(SpaceId, Uuid, Opts) ->
    case file_meta:get_including_deleted_local_or_remote(Uuid, SpaceId) of
        {ok, Doc} -> get(SpaceId, Doc, Opts);
        ?ERROR_NOT_FOUND -> {error, ?MISSING_FILE_META(Uuid)};
        {error, _} = Error -> Error
    end.

%%%===================================================================
%%% RPC API functions
%%%===================================================================

-spec invalidate(od_space:id()) -> ok.
invalidate(SpaceId) ->
    ok = effective_value:invalidate(?CACHE_NAME(SpaceId)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec calculate_links_sync_status(effective_value:args()) ->
    {ok, synced, effective_value:calculation_info()} | {error, missing_link()}.
calculate_links_sync_status([_, {error, _} = Error, _CalculationInfo]) ->
    Error;
calculate_links_sync_status([#document{value = #file_meta{is_scope = true}}, _ParentValue, CalculationInfo]) ->
    % is_scope is true for space dir - parent should not be checked as it does not exist
    {ok, synced, CalculationInfo};
calculate_links_sync_status([#document{} = FileMetaDoc, _ParentValue, CalculationInfo]) ->
    #document{value = #file_meta{name = Name, parent_uuid = ParentUuid}, scope = Scope} = FileMetaDoc,
    case file_meta_forest:get_local_or_remote(ParentUuid, Name, Scope, Scope) of
        {ok, _} -> {ok, synced, CalculationInfo};
        {error, _} -> {error, ?MISSING_FILE_LINK(ParentUuid, Name)}
    end.


%% @private
-spec find_lowest_missing_link(missing_link(), file_meta:doc()) ->
    missing_link() | ancestor_deleted.
find_lowest_missing_link(
    ?MISSING_FILE_LINK(ParentUuid, _) = MissingLink,
    #document{value = #file_meta{parent_uuid = ParentUuid}}
) ->
    MissingLink;
find_lowest_missing_link(
    MissingLink,
    #document{value = #file_meta{parent_uuid =  ParentUuid, name = Name}, scope = Scope} = Doc
) ->
    case file_meta_forest:get_local_or_remote(ParentUuid, Name, Scope, Scope) of
        {ok, _} ->
            case file_meta:get_parent(Doc) of
                {ok, NextDoc} -> find_lowest_missing_link(MissingLink, NextDoc);
                {error, not_found} -> ancestor_deleted
            end;
        {error, _} ->
            ?MISSING_FILE_LINK(ParentUuid, Name)
    end.
