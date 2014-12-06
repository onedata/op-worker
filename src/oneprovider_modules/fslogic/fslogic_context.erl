%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides and manages fslogic context information
%%       such as protocol version or user's DN
%% @end
%% ===================================================================
-module(fslogic_context).
-author("Rafal Slota").

-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_fuse_id/0, set_fuse_id/1, get_user_dn/0, set_user_dn/1, clear_user_dn/0, set_protocol_version/1, get_protocol_version/0, get_user_id/0]).
-export([set_fs_user_ctx/1, set_fs_root_user_ctx/0, get_fs_user_ctx/0, set_fs_group_ctx/1, set_fs_root_group_ctx/0, get_fs_group_ctx/0]).
-export([get_gr_auth/0, set_gr_auth/2]).
-export([gen_global_fuse_id/2, read_global_fuse_id/1, is_global_fuse_id/1]).
-export([clear_user_ctx/0, clear_gr_auth/0]).
-export([get_user_context/0, set_user_context/1]).
-export([get_context/0, set_context/1]).
-export([get_user_query/0]).

%% ====================================================================
%% API functions
%% ====================================================================


%% get_user_query/0
%% ====================================================================
%% @doc Gets query that shall be used for user_logic:get_user for current request or 'undefined' when no user context is available.
-spec get_user_query() -> {dn, string()} | {global_id, GRUID :: binary()} | undefined.
%% ====================================================================
get_user_query() ->
    case get_gr_auth() of
        {undefined, _} ->
            case get_user_dn() of
                undefined -> undefined;
                DN        -> {dn, DN}
            end;
        {GRUID, _} ->
            {global_id, utils:ensure_list(GRUID)}
    end.


%% get_gr_auth/0
%% ====================================================================
%% @doc Gets user's GRUID and AccessToken for current request. If access token wasn't set, this method will try
%%      to get it from DB based on user's DN ctx.
-spec get_gr_auth() -> {GRUID :: binary(), AccessToken :: binary()} | {undefined, undefined}.
%% ====================================================================
get_gr_auth() ->
    case {get(gruid), get(access_token)} of
        {undefined, _} ->
            case get_user_dn() of
                undefined ->
                    {undefined, undefined};
                DN ->
                    {ok, #db_document{record = #user{global_id = GRUID, access_token = AccessToken}}} = fslogic_objects:get_user({dn, DN}),
                    {GRUID, AccessToken}
            end;
        CTX -> CTX
    end.


%% set_gr_auth/1
%% ====================================================================
%% @doc Sets user's GRUID and AccessToken for current request.
-spec set_gr_auth(GRUID :: binary(), AccessToken :: binary()) -> Result :: any().
%% ====================================================================
set_gr_auth(GRUID, AccessToken) ->
    put(access_token, AccessToken),
    put(gruid, GRUID).


%% clear_gr_auth/0
%% ====================================================================
%% @doc Clears user's GRUID and AccessToken for current request.
-spec clear_gr_auth() -> ok.
%% ====================================================================
clear_gr_auth() ->
    erase(access_token),
    erase(gruid),
    ok.


%% clear_user_ctx/0
%% ====================================================================
%% @doc Clears user's context (both AccessToken and DN) for current request.
-spec clear_user_ctx() -> ok.
%% ====================================================================
clear_user_ctx() ->
    clear_gr_auth(),
    clear_user_dn(),
    ok.

%% get_user_dn/0
%% ====================================================================
%% @doc Gets user's DN for current request or 'undefined' when there is none.
-spec get_user_dn() -> Result when Result :: string() | undefined.
%% ====================================================================
get_user_dn() ->
    get(user_dn).


%% set_user_dn/1
%% ====================================================================
%% @doc Sets user's DN for current request.
-spec set_user_dn(UserDN :: string() | undefined) -> Result when Result :: term().
%% ====================================================================
set_user_dn(UserDN) ->
    put(user_dn, UserDN).


%% clear_user_dn/0
%% ====================================================================
%% @doc Clears user's DN for current request.
-spec clear_user_dn() -> OldValue :: term().
%% ====================================================================
clear_user_dn() ->
    erase(user_dn),
    ok.


%% get_fuse_id/0
%% ====================================================================
%% @doc Gets Fuse ID for current request or 'undefined' when there is none.
-spec get_fuse_id() -> Result when Result :: term() | undefined.
%% ====================================================================
get_fuse_id() ->
    utils:ensure_list(get(fuse_id)).


%% set_fuse_id/1
%% ====================================================================
%% @doc Sets Fuse ID for current request.
-spec set_fuse_id(FuseID :: term()) -> Result when Result :: term().
%% ====================================================================
set_fuse_id(FuseID) ->
    put(fuse_id, FuseID).


%% set_protocol_version/1
%% ====================================================================
%% @doc Sets protocol version for current request.
-spec set_protocol_version(ProtocolVersion :: term()) -> OldValue when OldValue :: term().
%% ====================================================================
set_protocol_version(PVers) ->
    put(protocol_version, PVers).


%% get_protocol_version/0
%% ====================================================================
%% @doc Gets protocol version for current request or 'undefined' when there is none.
-spec get_protocol_version() -> Result when Result :: term() | undefined.
%% ====================================================================
get_protocol_version() ->
    get(protocol_version).


%% get_user_id/0
%% ====================================================================
%% @doc Gets user's id. If there's no user DN in current context, ?CLUSTER_USER_ID is returned
%% @end
-spec get_user_id() -> Result when
    Result :: {ok, UserID} | {error, ErrorDesc},
    UserID :: term(),
    ErrorDesc :: atom.
%% ====================================================================
get_user_id() ->
    case fslogic_objects:get_user() of
        {ok, #db_document{uuid = UID}} -> {ok, UID};
        _ ->
            {ok, ?CLUSTER_USER_ID}
    end.


%% set_fs_user_ctx/1
%% ====================================================================
%% @doc Sets user name that shall be used for file system permissions checks.
%% @end
-spec set_fs_user_ctx(UName :: integer()) -> OldValue :: term().
%% ====================================================================
set_fs_user_ctx(UName) when is_integer(UName) ->
    put(fsctx_uname, UName).


%% set_fs_root_user_ctx/0
%% ====================================================================
%% @doc SHULD BE ONLY USED WHEN ACL PERMISSION HAS BEEN CONFIRMED
%% Sets root as user name that shall be used for file system permissions checks.
%% @end
-spec set_fs_root_user_ctx() -> OldValue :: term().
%% ====================================================================
set_fs_root_user_ctx() ->
    put(fsctx_uname, undefined).


%% get_fs_user_ctx/0
%% ====================================================================
%% @doc Gets user name that is used for the file system permissions checks.
%% @end
-spec get_fs_user_ctx() -> UserName :: string().
%% ====================================================================
get_fs_user_ctx() ->
    case get(fsctx_uname) of
        undefined ->
            0;
        UName -> UName
    end.


%% set_fs_group_ctx/1
%% ====================================================================
%% @doc Sets user's group-id that shall be used for file system permissions checks.
%% @end
-spec set_fs_group_ctx(GroupID :: [integer()]) -> OldValue :: term().
%% ====================================================================
set_fs_group_ctx(GroupID) ->
    put(fsctx_gname, GroupID).


%% set_fs_root_group_ctx/0
%% ====================================================================
%% @doc SHULD BE ONLY USED WHEN ACL PERMISSION HAS BEEN CONFIRMED
%% Sets root as user's group-id that shall be used for file system permissions checks.
%% @end
-spec set_fs_root_group_ctx() -> OldValue :: term().
%% ====================================================================
set_fs_root_group_ctx() ->
    put(fsctx_gname, undefined).


%% get_fs_group_ctx/0
%% ====================================================================
%% @doc Gets user's group list that is used for file system permissions checks.
%%      If the is no context set, "root" group is returned.
%% @end
-spec get_fs_group_ctx() -> GroupID when
    GroupID :: integer().
%% ====================================================================
get_fs_group_ctx() ->
    case get(fsctx_gname) of
        undefined ->
            [-1];
        [] ->
            [-1];
        GroupID -> GroupID
    end.


%% gen_global_fuse_id/1
%% ====================================================================
%% @doc Converts given ProviderId and FuseId into GlobalFuseId that can be recognised by other providers.
%% @end
-spec gen_global_fuse_id(ProviderId :: iolist(), FuseId :: iolist()) -> GlobalFuseId :: binary() | undefined.
%% ====================================================================
gen_global_fuse_id(_, undefined) ->
    undefined;
gen_global_fuse_id(ProviderId, FuseId) ->
    ProviderId1 = utils:ensure_binary(ProviderId),
    FuseId1 = utils:ensure_binary(FuseId),
    <<ProviderId1/binary, "::", FuseId1/binary>>.


%% read_global_fuse_id/1
%% ====================================================================
%% @doc Converts given GlobalFuseId into ProviderId and FuseId. This method inverts gen_global_fuse_id/1.
%%      Fails with exception if given argument has invalid format.
%% @end
-spec read_global_fuse_id(GlobalFuseId :: iolist()) -> {ProviderId :: binary(), FuseId :: binary()} | no_return().
%% ====================================================================
read_global_fuse_id(GlobalFuseId) ->
    GlobalFuseId1 = utils:ensure_binary(GlobalFuseId),
    [ProviderId, FuseId] = binary:split(GlobalFuseId1, <<"::">>),
    {ProviderId, FuseId}.


%% is_global_fuse_id/1
%% ====================================================================
%% @doc Checks if given FuseId can be recognised by other providers (i.e. was generated by gen_global_fuse_id/1).
%% @end
-spec is_global_fuse_id(GlobalFuseId :: iolist()) -> boolean().
%% ====================================================================
is_global_fuse_id(GlobalFuseId) ->
    GlobalFuseId1 = utils:ensure_binary(GlobalFuseId),
    length(binary:split(GlobalFuseId1, <<"::">>)) =:= 2.


%% get_user_context/0
%% ====================================================================
%% @doc Gets user DN, accessToken and GRUID
%% @end
-spec get_user_context() -> {DN :: term(), {GRUID :: term(), Token :: term()}}.
%% ====================================================================
get_user_context() ->
    {get_user_dn(),{get(gruid), get(access_token)}}.


%% set_user_context/1
%% ====================================================================
%% @doc Sets user DN, accessToken and GRUID
%% @end
-spec set_user_context(UserDoc :: user_doc() | {DN :: term(), {GRUID :: term(), Token :: term()}}) -> ok.
%% ====================================================================
set_user_context(#db_document{record = #user{global_id = GRUID, access_token = AccessToken, dn_list = [DN | _]}}) ->
    set_user_context({DN, {GRUID, AccessToken}});
set_user_context(#db_document{record = #user{global_id = GRUID, access_token = AccessToken}}) ->
    set_user_context({undefined, {GRUID, AccessToken}});
set_user_context({DN, {GRUID, AccessToken}}) ->
    set_user_dn(DN),
    set_gr_auth(GRUID, AccessToken),
    ok.

%% get_context/0
%% ====================================================================
%% @doc Gets fuse id and user context
%% @end
-spec get_context() -> term().
%% ====================================================================
get_context() ->
    {get_fuse_id(), get_user_context()}.


%% set_context/1
%% ====================================================================
%% @doc Sets user DN, accessToken and GRUID
%% @end
-spec set_context(Context :: term()) -> ok.
%% ====================================================================
set_context({FuseId, UserContext}) ->
    set_fuse_id(FuseId),
    set_user_context(UserContext),
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================