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
-export([authorize/3]).

-define(DATA_LOCATION_CAVEATS, [cv_data_path, cv_data_objectid]).


%%%===================================================================
%%% API
%%%===================================================================


-spec authorize(user_ctx:ctx(), file_ctx:ctx(), [fslogic_access:requirement()]) ->
    file_ctx:ctx() | no_return().
authorize(UserCtx, FileCtx0, AccessRequirements) ->
    FileCtx1 = check_constraints(UserCtx, FileCtx0),
    fslogic_access:assert_granted(UserCtx, FileCtx1, AccessRequirements).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_constraints(user_ctx:ctx(), file_ctx:ctx()) ->
    file_ctx:ctx().
check_constraints(UserCtx, FileCtx) ->
    case user_ctx:get_caveats(UserCtx) of
        [] ->
            FileCtx;
        Caveats ->
            SessionDiscriminator = case user_ctx:get_auth(UserCtx) of
                #token_auth{token = SerializedToken} ->
                    SerializedToken;
                SessionAuth ->
                    SessionAuth
            end,
            check_data_location_constraints(
                SessionDiscriminator, FileCtx, Caveats
            )
    end.


%% @private
-spec check_data_location_constraints(binary(), file_ctx:ctx(),
    [caveats:caveat()]) -> file_ctx:ctx().
check_data_location_constraints(SerializedToken, FileCtx, Caveats) ->
    Guid = file_ctx:get_guid_const(FileCtx),
    case caveats:filter(?DATA_LOCATION_CAVEATS, Caveats) of
        [] ->
            FileCtx;
        DataLocationCaveats ->
            check_and_cache_result(
                {data_location_constraints, SerializedToken, Guid},
                FileCtx,
                fun check_data_location_caveats/2,
                [DataLocationCaveats, FileCtx]
            )
    end.


%% @private
-spec check_data_location_caveats([caveats:caveat()], file_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | no_return().
check_data_location_caveats([], FileCtx) ->
    {ok, FileCtx};
check_data_location_caveats([#cv_data_path{whitelist = Paths} | Rest], FileCtx0) ->
    {FilePath, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),
    IsPathAllowed = lists:any(fun(Path) ->
        str_utils:binary_starts_with(Path, FilePath)
    end, Paths),

    case IsPathAllowed of
        true -> check_data_location_caveats(Rest, FileCtx1);
        false -> throw(?EACCES)
    end;
check_data_location_caveats([#cv_data_objectid{whitelist = ObjectIds} | Rest], FileCtx0) ->
    AllowedGuids = lists:map(fun(ObjectId) ->
        {ok, Guid} = file_id:objectid_to_guid(ObjectId),
        Guid
    end, ObjectIds),
    {AncestorsGuids, FileCtx1} = file_ctx:get_ancestors_guids(FileCtx0),

    IsGuidAllowed = lists:any(fun(Guid) ->
        lists:member(Guid, AllowedGuids)
    end, [file_ctx:get_guid_const(FileCtx1) | AncestorsGuids]),

    case IsGuidAllowed of
        true -> check_data_location_caveats(Rest, FileCtx1);
        false -> throw(?EACCES)
    end.


%% @private
-spec check_and_cache_result(
    CacheKey :: term(),
    file_ctx:ctx(),
    CheckFun :: function(),
    CheckFunArgs :: [term()]
) ->
    file_ctx:ctx() | no_return().
check_and_cache_result(CacheKey, FileCtx0, CheckFun, CheckFunArgs) ->
    case permissions_cache:check_permission(CacheKey) of
        {ok, ok} ->
            FileCtx0;
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            try
                {ok, FileCtx1} = apply(CheckFun, CheckFunArgs),
                permissions_cache:cache_permission(CacheKey, ok),
                FileCtx1
            catch _:?EACCES ->
                permissions_cache:cache_permission(CacheKey, ?EACCES),
                throw(?EACCES)
            end
    end.
