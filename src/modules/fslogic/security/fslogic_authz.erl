%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for authorization of fslogic operations.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_authz).
-author("Bartosz Walkowicz").

-include("proto/common/credentials.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/caveats.hrl").

%% API
-export([
    authorize/3, authorize/4
]).

-define(DATA_LOCATION_CAVEATS, [cv_data_path, cv_data_objectid]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv authorize(UserCtx, FileCtx0, AccessRequirements, true).
%% @end
%%--------------------------------------------------------------------
-spec authorize(user_ctx:ctx(), file_ctx:ctx(), [fslogic_access:requirement()]) ->
    file_ctx:ctx() | no_return().
authorize(UserCtx, FileCtx0, AccessRequirements) ->
    authorize(UserCtx, FileCtx0, AccessRequirements, true).


%%--------------------------------------------------------------------
%% @doc
%% Checks access to specified file and verifies data caveats.
%% StrictCaveatVerification means that permission can be granted only
%% for files in subpaths allowed by caveats. Otherwise caveats paths
%% ancestors will be also allowed.
%% @end
%%--------------------------------------------------------------------
-spec authorize(user_ctx:ctx(), file_ctx:ctx(), [fslogic_access:requirement()],
    boolean()) -> file_ctx:ctx() | no_return().
authorize(UserCtx, FileCtx0, AccessRequirements, StrictCaveatsCheck) ->
    FileCtx2 = case check_caveats(UserCtx, FileCtx0, StrictCaveatsCheck) of
        {subpath, FileCtx1} -> FileCtx1;
        {ancestor, FileCtx1, _} -> FileCtx1
    end,
    fslogic_access:assert_granted(UserCtx, FileCtx2, AccessRequirements).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_caveats(user_ctx:ctx(), file_ctx:ctx(), boolean()) ->
    {subpath, file_ctx:ctx()} |
    {ancestor, file_ctx:ctx(), [file_meta:name()]}.
check_caveats(UserCtx, FileCtx, StrictCaveatsCheck) ->
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
            check_data_location_caveats(
                SessionDiscriminator, FileCtx, StrictCaveatsCheck,
                caveats:filter(?DATA_LOCATION_CAVEATS, Caveats)
            )
    end.


%% @private
-spec check_data_location_caveats(binary(), file_ctx:ctx(), boolean(),
    [caveats:caveat()]
) ->
    {subpath, file_ctx:ctx()} |
    {ancestor, file_ctx:ctx(), [file_meta:name()]}.
check_data_location_caveats(_SerializedToken, FileCtx, _, []) ->
    FileCtx;
check_data_location_caveats(
    SerializedToken, FileCtx0, StrictCaveatsCheck, Caveats
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
            case StrictCaveatsCheck of
                true ->
                    check_and_cache_data_location_caveats(
                        FileCtx0, Caveats, StrictCaveatsCheck,
                        StrictCacheKey, LooseCacheKey, StrictCacheKey
                    );
                false ->
                    case permissions_cache:check_permission(LooseCacheKey) of
                        {ok, ?EACCES} ->
                            throw(?EACCES);
                        {ok, Children} ->
                            {ancestor, FileCtx0, Children};
                        _ ->
                            check_and_cache_data_location_caveats(
                                FileCtx0, Caveats, StrictCaveatsCheck,
                                StrictCacheKey, LooseCacheKey, LooseCacheKey
                            )
                    end
            end
    end.


%% @private
-spec check_and_cache_data_location_caveats(
    file_ctx:ctx(), [caveats:caveat()],
    StrictCaveatsCheck :: boolean(),
    StrictCacheKey :: term(),
    LooseCacheKey :: term(),
    EaccesCacheKey :: term()
) ->
    {subpath, file_ctx:ctx()} |
    {ancestor, file_ctx:ctx(), [file_meta:name()]}.
check_and_cache_data_location_caveats(
    FileCtx0, Caveats, StrictCaveatsCheck,
    StrictCacheKey, LooseCacheKey, EaccesCacheKey
) ->
    try
        Result = fslogic_caveats:verify_data_location_caveats(
            FileCtx0, Caveats, StrictCaveatsCheck
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
