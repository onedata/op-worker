%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% OZ user data cache
%%% @end
%%%-------------------------------------------------------------------
-module(od_user).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

%% API
-export([save/1, get/1, exists/1, delete/1, update/2, create/1]).
-export([fetch/1, get_or_fetch/2, create_or_update/2, run_after/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_posthooks/0, get_record_struct/1]).

-type id() :: binary().
-type info() :: #od_user{}.
-type doc() :: datastore_doc:doc(info()).
-type diff() :: datastore_doc:diff(info()).

%% Oauth connected accounts in form of proplist:
%%[
%%    {<<"provider_id">>, binary()},
%%    {<<"user_id">>, binary()},
%%    {<<"login">>, binary()},
%%    {<<"name">>, binary()},
%%    {<<"email_list">>, [binary()]}
%%]
-type connected_account() :: proplists:proplist().

-export_type([doc/0, id/0, connected_account/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves space.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    run_and_update_user(fun datastore_model:save/2, [?CTX, Doc]).

%%--------------------------------------------------------------------
%% @doc
%% Updates space.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(UserId, Diff) ->
    run_and_update_user(fun datastore_model:update/3, [?CTX, UserId, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% Creates space.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, id()} | {error, term()}.
create(Doc) ->
    run_and_update_user(fun datastore_model:create/2, [?CTX, Doc]).

%%--------------------------------------------------------------------
%% @doc
%% Deletes space.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> ok | {error, term()}.
get(?ROOT_USER_ID) ->
    {ok, #document{key = ?ROOT_USER_ID, value = #od_user{name = <<"root">>}}};
get(?GUEST_USER_ID) ->
    {ok, #document{key = ?GUEST_USER_ID, value = #od_user{
        name = <<"nobody">>, space_aliases = []
    }}};
get(UserId) ->
    datastore_model:get(?CTX, UserId).

%%--------------------------------------------------------------------
%% @doc
%% Deletes user.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(UserId) ->
    datastore_model:delete(?CTX, UserId).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether user exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(id()) -> boolean().
exists(UserId) ->
    {ok, Exists} = datastore_model:exists(?CTX, UserId),
    Exists.

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(doc(), diff()) -> {ok, id()} | {error, term()}.
create_or_update(#document{key = UserId, value = Default}, Diff) ->
    run_and_update_user(fun datastore_model:update/4, [
        ?CTX, UserId, Diff, Default
    ]).

%%--------------------------------------------------------------------
%% @doc
%% Fetch user from OZ and save it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Auth :: oz_endpoint:auth()) ->
    {ok, datastore:doc()} | {error, Reason :: term()}.
fetch(Auth) ->
    {ok, #user_details{
        id = UserId, name = Name, connected_accounts = ConnectedAccounts,
        alias = Alias, email_list = EmailList}
    } = oz_users:get_details(Auth),
    {ok, #user_spaces{ids = SpaceIds, default = DefaultSpaceId}} =
        oz_users:get_spaces(Auth),
    {ok, EffectiveGroupIds} = oz_users:get_effective_groups(Auth),

    Spaces = utils:pmap(fun(SpaceId) ->
        {ok, #space_details{name = SpaceName}} =
            oz_spaces:get_details(Auth, SpaceId),
        {SpaceId, SpaceName}
    end, SpaceIds),

    OnedataUser = #od_user{
        name = Name,
        space_aliases = Spaces,
        default_space = DefaultSpaceId,
        eff_groups = EffectiveGroupIds,
        connected_accounts = ConnectedAccounts,
        alias = Alias,
        email_list = EmailList
    },
    OnedataUserDoc = #document{key = UserId, value = OnedataUser},

    case od_user:create(OnedataUserDoc) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end,

    utils:pforeach(fun(SpaceId) ->
        od_space:get_or_fetch(Auth, SpaceId, UserId)
    end, SpaceIds),

    utils:pforeach(fun(GroupId) ->
        od_group:get_or_fetch(Auth, GroupId)
    end, EffectiveGroupIds),

    {ok, OnedataUserDoc}.

%%--------------------------------------------------------------------
%% @doc
%% Get user from cache or fetch from OZ and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(Auth :: oz_endpoint:auth(), UserId :: id()) ->
    {ok, datastore:doc()} | {error, term()}.
get_or_fetch(Auth, UserId) ->
    try
        case od_user:get(UserId) of
            {ok, Doc} -> {ok, Doc};
            {error, not_found} -> fetch(Auth);
            Error -> Error
        end
    catch
        _:Reason ->
            ?error_stacktrace("Cannot get or fetch details of onedata user ~p due to: ~p",
                [UserId, Reason]),
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Run function and in case of success, update user's file_meta space structures.
%% @end
%%--------------------------------------------------------------------
-spec run_and_update_user(function(), list()) -> {ok, datastore:key()} | {error, term()}.
run_and_update_user(Function, Args) ->
    case apply(Function, Args) of
        {ok, #document{key = Uuid}} ->
            file_meta:setup_onedata_user(provider, Uuid),
            {ok, Uuid};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% User create/update posthook.
%% @end
%%--------------------------------------------------------------------
-spec run_after(atom(), list(), term()) -> term().
run_after(create, _, {ok, Doc}) ->
    ok = permissions_cache:invalidate(),
    {ok, Doc};
run_after(update, [_, _, _, _], {ok, Doc}) ->
    ok = permissions_cache:invalidate(),
    {ok, Doc};
run_after(save, _, {ok, Doc}) ->
    ok = permissions_cache:invalidate(),
    {ok, Doc};
run_after(update, _, {ok, Doc}) ->
    ok = permissions_cache:invalidate(),
    {ok, Doc};
run_after(_Function, _Args, Result) ->
    Result.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun(Function, Args, Result) ->
        od_user:run_after(Function, Args, Result)
    end].

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {name, string},
        {alias, string},
        {email_list, [string]},
        {connected_accounts, [[{string, term}]]},
        {default_space, string},
        {space_aliases, [{string, string}]},
        {groups, [string]},
        {spaces, [string]},
        {handle_services, [string]},
        {handles, [string]},
        {eff_groups, [string]},
        {eff_spaces, [string]},
        {eff_shares, [string]},
        {eff_providers, [string]},
        {eff_handle_services, [string]},
        {eff_handles, [string]},
        {public_only, boolean},
        {revision_history, [term]}
    ]}.