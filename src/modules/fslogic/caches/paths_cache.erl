%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for management of paths caches.
%%% @end
%%%-------------------------------------------------------------------
-module(paths_cache).
-author("Jakub Kudzia").
-author("Michał Stanisz").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_group/0, init/1, invalidate_on_all_nodes/1]).
-export([get/3, get_canonical/2, get_uuid_based/2]).
%% RPC API
-export([invalidate/1]).

-define(PATH_CACHE_GROUP, <<"paths_cache_group">>).
-define(CANONICAL_PATHS_CACHE_NAME(SpaceId), binary_to_atom(<<"canonical_paths_cache_", SpaceId/binary>>, utf8)).
-define(UUID_BASED_PATHS_CACHE_NAME(SpaceId), binary_to_atom(<<"uuid_paths_cache_", SpaceId/binary>>, utf8)).

-type cache() :: atom().
-type path_type() :: file_meta:path_type().

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_group() -> ok.
init_group() ->
    CheckFrequency = op_worker:get_env(canonical_paths_cache_frequency, 30000),
    Size = op_worker:get_env(canonical_paths_cache_size, 20000),
    ok = effective_value:init_group(?PATH_CACHE_GROUP, #{
        check_frequency => CheckFrequency,
        size => Size,
        worker => false
    }).


-spec init(od_space:id() | all) -> ok.
init(all) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:foreach(fun init/1, SpaceIds);
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            ?debug("Unable to initialize paths caches due to: ~tp", [?ERROR_NO_CONNECTION_TO_ONEZONE]);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Unable to initialize paths caches due to: ~tp", [?ERROR_UNREGISTERED_ONEPROVIDER]);
        Error = {error, _} ->
            ?critical("Unable to initialize paths caches due to: ~tp", [Error])
    catch
        Class:Reason:Stacktrace ->
            ?critical_exception("Unable to initialize path caches", Class, Reason, Stacktrace)
    end;
init(SpaceId) ->
    ok = init(SpaceId, ?CANONICAL_PATHS_CACHE_NAME(SpaceId)),
    ok = init(SpaceId, ?UUID_BASED_PATHS_CACHE_NAME(SpaceId)).


-spec invalidate_on_all_nodes(od_space:id()) -> ok.
invalidate_on_all_nodes(SpaceId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = utils:rpc_multicall(Nodes, ?MODULE, invalidate, [SpaceId]),

    case BadNodes of
        [] ->
            ok;
        _ ->
            ?error("Invalidation of paths caches for space ~tp failed on nodes: ~tp (RPC error)", [SpaceId, BadNodes])
    end,

    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?error(
                "Invalidation of paths caches for space ~tp failed.~n"
                "Reason: ~tp", [SpaceId, Error]
            )
    end, Res).


-spec get_canonical(od_space:id(), file_meta:uuid() | file_meta:doc()) ->
    {ok, file_meta:path()} | {error, term()}.
get_canonical(SpaceId, UuidOrDoc) ->
    get(SpaceId, UuidOrDoc, ?CANONICAL_PATH).


-spec get_uuid_based(od_space:id(), file_meta:uuid() | file_meta:doc()) ->
    {ok, file_meta:path()} | {error, term()}.
get_uuid_based(SpaceId, UuidOrDoc) ->
    get(SpaceId, UuidOrDoc, ?UUID_BASED_PATH).


-spec get(od_space:id(), file_meta:uuid() | file_meta:doc(), path_type()) ->
    {ok, file_meta:path() | file_meta:?UUID_BASED_PATH()} | {error, term()}.
get(SpaceId, Doc = #document{value = #file_meta{}}, PathType) ->
    CacheName = path_type_to_cache_name(SpaceId, PathType),
    case effective_value:get_or_calculate(CacheName, Doc, calculate_path_tokens_callback(PathType)) of
        {ok, Path, _} ->
            {ok, filename:join(Path)};
        {error, ?MISSING_FILE_META(_MissingUuid)} = Error ->
            Error
    end;
get(SpaceId, Uuid, PathType) ->
    case file_meta:get_including_deleted(Uuid) of
        {ok, Doc} -> get(SpaceId, Doc, PathType);
        ?ERROR_NOT_FOUND -> {error, ?MISSING_FILE_META(Uuid)};
        {error, _} = Error -> Error
    end.


%%%===================================================================
%%% RPC API functions
%%%===================================================================

-spec invalidate(od_space:id()) -> ok.
invalidate(SpaceId) ->
    ok = effective_value:invalidate(?CANONICAL_PATHS_CACHE_NAME(SpaceId)),
    ok = effective_value:invalidate(?UUID_BASED_PATHS_CACHE_NAME(SpaceId)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init(od_space:id(), effective_value:cache()) -> ok.
init(Space, Name) ->
    try
        case effective_value:cache_exists(Name) of
            true ->
                ok;
            _ ->
                case effective_value:init_cache(Name, #{group => ?PATH_CACHE_GROUP}) of
                    ok ->
                        ok;
                    Error = {error, _} ->
                        ?critical("Unable to initialize paths cache for space ~tp due to: ~tp",
                            [Space, Error])
                end
        end
    catch
        Class:Reason:Stacktrace ->
            ?critical_exception("Unable to initialize path cache for space ~tp", [Space], Class, Reason, Stacktrace)
    end.


-spec path_type_to_cache_name(od_space:id(), path_type()) -> cache().
path_type_to_cache_name(SpaceId, ?CANONICAL_PATH) ->
    ?CANONICAL_PATHS_CACHE_NAME(SpaceId);
path_type_to_cache_name(SpaceId, ?UUID_BASED_PATH) ->
    ?UUID_BASED_PATHS_CACHE_NAME(SpaceId).


-spec calculate_path_tokens_callback(path_type()) -> fun().
calculate_path_tokens_callback(PathType) ->
    fun([
        #document{
            key = Uuid,
            value = #file_meta{name = Name},
            scope = SpaceId
        },
        ParentValue, CalculationInfo
    ]) ->
        case fslogic_file_id:is_root_dir_uuid(Uuid) of
            true ->
                {ok, [<<"/">>], CalculationInfo};
            false ->
                case fslogic_file_id:is_space_dir_uuid(Uuid) of
                    true ->
                        {ok, [<<"/">>, SpaceId], CalculationInfo};
                    false ->
                        NameOrUuid = case PathType of
                            ?UUID_BASED_PATH -> Uuid;
                            ?CANONICAL_PATH -> Name
                        end,
                        {ok, ParentValue ++ [NameOrUuid], CalculationInfo}
                end
        end
    end.