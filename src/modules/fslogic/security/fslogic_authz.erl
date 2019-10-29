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
    FileCtx1 = check_caveats(UserCtx, FileCtx0),
    fslogic_access:assert_granted(UserCtx, FileCtx1, AccessRequirements).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_caveats(user_ctx:ctx(), file_ctx:ctx()) ->
    file_ctx:ctx().
check_caveats(UserCtx, FileCtx) ->
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
            check_data_location_caveats(
                SessionDiscriminator, FileCtx, false,
                caveats:filter(?DATA_LOCATION_CAVEATS, Caveats)
            )
    end.


%% @private
-spec check_data_location_caveats(binary(), file_ctx:ctx(), boolean(),
    [caveats:caveat()]) -> file_ctx:ctx().
check_data_location_caveats(_SerializedToken, FileCtx, _AllowAncestors, []) ->
    FileCtx;
check_data_location_caveats(SerializedToken, FileCtx0, AllowAncestors, Caveats) ->
    Guid = file_ctx:get_guid_const(FileCtx0),
    CacheKey = {data_location_caveats, SerializedToken, Guid},
    LsCacheKey = {data_location_caveats_ls, SerializedToken, Guid},

    case permissions_cache:check_permission(CacheKey) of
        {ok, ok} ->
            FileCtx0;
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            try
                case fslogic_caveats:verify_data_location_caveats(FileCtx0, Caveats, AllowAncestors) of
                    {subpath, FileCtx1, _} ->
                        permissions_cache:cache_permission(CacheKey, ok),
                        FileCtx1;
                    {ancestor, FileCtx1, _Names} ->
                        permissions_cache:cache_permission(LsCacheKey, ok),
                        FileCtx1
                end
            catch _:?EACCES ->
                permissions_cache:cache_permission(CacheKey, ?EACCES),
                throw(?EACCES)
            end
    end.
