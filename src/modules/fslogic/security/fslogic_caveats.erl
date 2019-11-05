%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for verification of data caveats.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_caveats).
-author("Bartosz Walkowicz").

-include("proto/common/credentials.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/caveats.hrl").

%% API
-export([
    assert_no_data_caveats/1,
    verify_data_caveats/3
]).

-define(DATA_CAVEATS, [cv_data_path, cv_data_objectid]).
-define(DATA_LOCATION_CAVEATS, [cv_data_path, cv_data_objectid]).

-define(CV_PATH(__PATHS), #cv_data_path{whitelist = __PATHS}).
-define(CV_OBJECTID(__OBJECTIDS), #cv_data_objectid{whitelist = __OBJECTIDS}).


-type relation() :: subpath | ancestor.
-type children() :: gb_sets:set(file_meta:name()).
-type data_location_caveat() :: #cv_data_objectid{} | #cv_data_path{}.

-export_type([relation/0, children/0, data_location_caveat/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_no_data_caveats([caveats:caveat()]) -> ok | no_return().
assert_no_data_caveats(Caveats) when is_list(Caveats) ->
    case caveats:filter(?DATA_CAVEATS, Caveats) of
        [] ->
            ok;
        [DataCaveat | _] ->
            throw(?ERROR_TOKEN_CAVEAT_UNVERIFIED(DataCaveat))
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies whether FileCtx:
%% - lies in path allowed by location caveats or is ancestor to path
%%   allowed by location caveats (only if AllowAncestors flag is
%%   set to true).
%% If above condition doesn't hold then ?EACCES is thrown.
%% @end
%%--------------------------------------------------------------------
-spec verify_data_caveats(user_ctx:ctx(), file_ctx:ctx(), boolean()) ->
    {subpath, file_ctx:ctx()} |
    {ancestor, file_ctx:ctx(), [file_meta:name()]}.
verify_data_caveats(UserCtx, FileCtx, AllowAncestorsOfLocationCaveats) ->
    case user_ctx:get_caveats(UserCtx) of
        [] ->
            {subpath, FileCtx};
        Caveats ->
            SessionDiscriminator = case user_ctx:get_auth(UserCtx) of
                #token_auth{token = SerializedToken} ->
                    SerializedToken;
                SessionAuth ->
                    SessionAuth
            end,
            verify_data_location_caveats(
                SessionDiscriminator, FileCtx,
                caveats:filter(?DATA_LOCATION_CAVEATS, Caveats),
                AllowAncestorsOfLocationCaveats
            )
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec verify_data_location_caveats(
    binary(), file_ctx:ctx(), [caveats:caveat()], boolean()
) ->
    {subpath, file_ctx:ctx()} |
    {ancestor, file_ctx:ctx(), [file_meta:name()]}.
verify_data_location_caveats(_SerializedToken, FileCtx, [], _) ->
    FileCtx;
verify_data_location_caveats(
    SerializedToken, FileCtx0, Caveats, AllowAncestorsOfLocationCaveats
) ->
    Guid = file_ctx:get_guid_const(FileCtx0),
    StrictCacheKey = {strict_data_location_caveats, SerializedToken, Guid},
    LooseCacheKey = {loose_data_location_caveats, SerializedToken, Guid},

    case permissions_cache:check_permission(StrictCacheKey) of
        {ok, ok} ->
            {subpath, FileCtx0};
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            case AllowAncestorsOfLocationCaveats of
                true ->
                    case permissions_cache:check_permission(LooseCacheKey) of
                        {ok, ?EACCES} ->
                            throw(?EACCES);
                        {ok, Children} ->
                            {ancestor, FileCtx0, Children};
                        _ ->
                            verify_and_cache_data_location_caveats(
                                FileCtx0, Caveats,
                                AllowAncestorsOfLocationCaveats,
                                StrictCacheKey, LooseCacheKey, LooseCacheKey
                            )
                    end;
                false ->
                    verify_and_cache_data_location_caveats(
                        FileCtx0, Caveats, AllowAncestorsOfLocationCaveats,
                        StrictCacheKey, LooseCacheKey, StrictCacheKey
                    )
            end
    end.


%% @private
-spec verify_and_cache_data_location_caveats(
    file_ctx:ctx(), [caveats:caveat()],
    AllowAncestorsOfLocationCaveats :: boolean(),
    StrictCacheKey :: term(),
    LooseCacheKey :: term(),
    EaccesCacheKey :: term()
) ->
    {subpath, file_ctx:ctx()} |
    {ancestor, file_ctx:ctx(), [file_meta:name()]}.
verify_and_cache_data_location_caveats(
    FileCtx0, Caveats, AllowAncestorsOfLocationCaveats,
    StrictCacheKey, LooseCacheKey, EaccesCacheKey
) ->
    try
        Result = verify_data_location_caveats(
            FileCtx0, Caveats, AllowAncestorsOfLocationCaveats
        ),
        case Result of
            {subpath, FileCtx1, _} ->
                permissions_cache:cache_permission(StrictCacheKey, ok),
                {subpath, FileCtx1};
            {ancestor, FileCtx1, ChildrenSet} ->
                ChildrenList = gb_sets:to_list(ChildrenSet),
                permissions_cache:cache_permission(LooseCacheKey, ChildrenList),
                {ancestor, FileCtx1, ChildrenList}
        end
    catch _:?EACCES ->
        permissions_cache:cache_permission(EaccesCacheKey, ?EACCES),
        throw(?EACCES)
    end.


%% @private
-spec verify_data_location_caveats(file_ctx:ctx(), [data_location_caveat()],
    AllowAncestors :: boolean()
) ->
    {relation(), file_ctx:ctx(), undefined | children()} | no_return().
verify_data_location_caveats(FileCtx0, Caveats, AllowAncestors) ->
    lists:foldl(fun(Caveat, {CurrRelation, FileCtx1, NamesAcc}) ->
        {CaveatRelation, FileCtx2, Names} = verify_data_location_caveat(
            FileCtx1, Caveat, AllowAncestors
        ),
        case {CaveatRelation, CurrRelation} of
            {subpath, ancestor} ->
                {ancestor, FileCtx2, NamesAcc};
            {subpath, _} ->
                {subpath, FileCtx2, NamesAcc};
            {ancestor, ancestor} ->
                {ancestor, FileCtx2, gb_sets:intersection(Names, NamesAcc)};
            {ancestor, _} ->
                {ancestor, FileCtx2, Names}
        end
    end, {undefined, FileCtx0, undefined}, Caveats).


%% @private
-spec verify_data_location_caveat(file_ctx:ctx(), data_location_caveat(),
    AllowAncestors :: boolean()
) ->
    {relation(), file_ctx:ctx(), undefined | children()} | no_return().
verify_data_location_caveat(FileCtx0, ?CV_PATH(AllowedPaths), false) ->
    {FilePath, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),

    IsFileInAllowedSubPath = lists:any(fun(AllowedPath) ->
        str_utils:binary_starts_with(FilePath, AllowedPath)
    end, AllowedPaths),

    case IsFileInAllowedSubPath of
        true ->
            {subpath, FileCtx1, undefined};
        false ->
            throw(?EACCES)
    end;
verify_data_location_caveat(FileCtx, ?CV_PATH(AllowedPaths), true) ->
    check_data_path_relation(FileCtx, AllowedPaths);

verify_data_location_caveat(FileCtx0, ?CV_OBJECTID(ObjectIds), false) ->
    AllowedGuids = objectids_to_guids(ObjectIds),
    {AncestorsGuids, FileCtx1} = file_ctx:get_ancestors_guids(FileCtx0),

    IsFileInAllowedSubPath = lists:any(fun(Guid) ->
        lists:member(Guid, AllowedGuids)
    end, [file_ctx:get_guid_const(FileCtx1) | AncestorsGuids]),

    case IsFileInAllowedSubPath of
        true ->
            {subpath, FileCtx1, undefined};
        false ->
            throw(?EACCES)
    end;
verify_data_location_caveat(FileCtx, ?CV_OBJECTID(ObjectIds), true) ->
    check_data_path_relation(FileCtx, objectids_to_paths(ObjectIds)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether file is ancestor or subpath to any of specified paths.
%% In case when file is ancestor to one path and subpath to another, then
%% subpath takes precedence.
%% Additionally, if it is ancestor, it returns list of it's immediate
%% children.
%% @end
%%--------------------------------------------------------------------
-spec check_data_path_relation(file_ctx:ctx(), [file_meta:path()]) ->
    {relation(), file_ctx:ctx(), undefined | children()} | no_return().
check_data_path_relation(FileCtx0, AllowedPaths) ->
    {FilePath, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),
    FilePathLen = size(FilePath),

    {Relation, Names} = lists:foldl(fun
        (_AllowedPath, {subpath, _NamesAcc} = Acc) ->
            Acc;
        (AllowedPath, {_Relation, NamesAcc} = Acc) ->
            AllowedPathLen = size(AllowedPath),
            case FilePathLen >= AllowedPathLen of
                true ->
                    % Check if FilePath is allowed by caveats
                    % (AllowedPath is ancestor to FilePath)
                    case str_utils:binary_starts_with(FilePath, AllowedPath) of
                        true -> {subpath, undefined};
                        false -> Acc
                    end;
                false ->
                    % Check if FilePath is ancestor to AllowedPath
                    case AllowedPath of
                        <<FilePath:FilePathLen/binary, "/", SubPath/binary>> ->
                            [Name | _] = string:split(SubPath, <<"/">>),
                            {ancestor, gb_sets:add(Name, utils:ensure_defined(
                                NamesAcc, undefined, gb_sets:new()
                            ))};
                        _ ->
                            Acc
                    end
            end
    end, {undefined, undefined}, AllowedPaths),

    case Relation of
        undefined ->
            throw(?EACCES);
        _ ->
            {Relation, FileCtx1, Names}
    end.


%% @private
-spec objectids_to_guids([file_id:objectid()]) -> [file_id:file_guid()].
objectids_to_guids(Objectids) ->
    lists:filtermap(fun(ObjectId) ->
        try
            {true, element(2, {ok, _} = file_id:objectid_to_guid(ObjectId))}
        catch _:_ ->
            % Invalid objectid does not make entire token invalid
            false
        end
    end, Objectids).


%% @private
-spec objectids_to_paths([file_id:objectid()]) -> [file_meta:path()].
objectids_to_paths(ObjectIds) ->
    lists:filtermap(fun(ObjectId) ->
        try
            {ok, Guid} = file_id:objectid_to_guid(ObjectId),
            FileCtx = file_ctx:new_by_guid(Guid),
            {Path, _} = file_ctx:get_canonical_path(FileCtx),
            {true, Path}
        catch _:_ ->
            % File may have been deleted so it is not possible to resolve
            % it's path so skip it (it does not make token invalid)
            false
        end
    end, ObjectIds).
