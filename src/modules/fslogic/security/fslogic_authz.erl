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
-export([authorize/3, check_access/3]).

-type access_type() ::
    owner % Check whether user owns the item
    | traverse_ancestors % validates ancestors' exec permission.
    | owner_if_parent_sticky % Check whether user owns the item but only if parent of the item has sticky bit.
    | share % Check if the file (or its ancestor) is shared
    | write | read | exec | rdwr % posix perms
    | binary(). % acl perms

-type access_definition() ::
    root
    | access_type()
    | {access_type(), 'or', access_type()}.

-export_type([access_definition/0]).

-define(DATA_LOCATION_CAVEATS, [
    cv_data_space, cv_data_path, cv_data_objectid
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec authorize(user_ctx:ctx(), file_ctx:ctx(), [access_definition()]) ->
    file_ctx:ctx() | no_return().
authorize(UserCtx, FileCtx0, AccessDefinitions0) ->
    AccessDefinitions1 = case user_ctx:is_root(UserCtx) of
        true ->
            [];
        false ->
            case user_ctx:is_guest(UserCtx) of
                true -> [share | AccessDefinitions0];
                false -> AccessDefinitions0
            end
    end,

    FileCtx1 = check_constraints(UserCtx, FileCtx0),
    check_access_definitions(UserCtx, FileCtx1, AccessDefinitions1).


-spec check_access(user_ctx:ctx(), file_ctx:ctx(), access_definition()) ->
    file_ctx:ctx() | no_return().
check_access(UserCtx, FileCtx, AccessDef) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx),
    check_and_cache_result(
        {AccessDef, UserId, Guid},
        FileCtx,
        fun rules:check_normal_def/3,
        [AccessDef, UserCtx, FileCtx]
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_access_definitions(user_ctx:ctx(), file_ctx:ctx(),
    [access_definition()]) -> file_ctx:ctx().
check_access_definitions(_UserCtx, FileCtx, []) ->
    FileCtx;
check_access_definitions(UserCtx, FileCtx0, [AccessDefinition | Rest]) ->
    FileCtx1 = check_access(UserCtx, FileCtx0, AccessDefinition),
    check_access_definitions(UserCtx, FileCtx1, Rest).


%% @private
-spec check_constraints(user_ctx:ctx(), file_ctx:ctx()) ->
    file_ctx:ctx().
check_constraints(UserCtx, FileCtx) ->
    case user_ctx:get_auth(UserCtx) of
        #token_auth{token = SerializedToken} ->
            {ok, Token} = tokens:deserialize(SerializedToken),
            Caveats = tokens:get_caveats(Token),

            check_data_location_constraints(SerializedToken, FileCtx, Caveats);
        _ ->
            % Root and guest are not restricted by caveats
            FileCtx
    end.


%% @private
-spec check_data_location_constraints(tokens:serialized(), file_ctx:ctx(),
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
check_data_location_caveats([#cv_data_space{whitelist = Spaces} | Rest], FileCtx0) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx0),
    case lists:member(SpaceId, Spaces) of
        true -> check_data_location_caveats(Rest, FileCtx0);
        false -> throw(?EACCES)
    end;
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
