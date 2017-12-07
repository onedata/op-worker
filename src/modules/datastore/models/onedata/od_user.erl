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
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%% API
-export([fetch/1, get_or_fetch/2, get_public_user_data/2, create_or_update/2]).
-export([record_struct/1]).

-export_type([doc/0, id/0, connected_account/0]).

-type doc() :: datastore:document().
-type id() :: binary().

%% Oauth connected accounts in form of proplist:
%%[
%%    {<<"provider_id">>, binary()},
%%    {<<"user_id">>, binary()},
%%    {<<"login">>, binary()},
%%    {<<"name">>, binary()},
%%    {<<"email_list">>, [binary()]}
%%]
-type connected_account() :: proplists:proplist().

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
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

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    run_and_update_user(fun model:execute_with_default_context/3,
        [?MODULE, save, [Document]]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    run_and_update_user(fun model:execute_with_default_context/3,
        [?MODULE, update, [Key, Diff]]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    run_and_update_user(fun model:execute_with_default_context/3,
        [?MODULE, create, [Document]]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(?ROOT_USER_ID) ->
    {ok, #document{key = ?ROOT_USER_ID, value = #od_user{name = <<"root">>}}};
get(?GUEST_USER_ID) ->
    {ok, #document{key = ?GUEST_USER_ID, value = #od_user{name = <<"nobody">>, space_aliases = []}}};
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(onedata_user_bucket, [
        {?MODEL_NAME, create}, {?MODEL_NAME, save},
        {?MODEL_NAME, create_or_update}, {?MODEL_NAME, update}
    ], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(?MODEL_NAME, create, _, _, {ok, _}) ->
    ok = permissions_cache:invalidate();
'after'(?MODEL_NAME, create_or_update, _, _, {ok, _}) ->
    ok = permissions_cache:invalidate();
'after'(?MODEL_NAME, save, _, _, {ok, _}) ->
    ok = permissions_cache:invalidate();
'after'(?MODEL_NAME, update, _, _, {ok, _}) ->
    ok = permissions_cache:invalidate();
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:generic_error().
create_or_update(Doc, Diff) ->
    run_and_update_user(fun model:execute_with_default_context/3,
        [?MODULE, create_or_update, [Doc, Diff]]).

%%--------------------------------------------------------------------
%% @doc
%% Fetch user from OZ and save it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Auth :: oz_endpoint:auth()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
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
%% Fetch user from OZ, do not save it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch_public_user(Auth :: oz_endpoint:auth(), UserId :: od_user:id()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
fetch_public_user(Auth, UserId) ->
    {ok, #user_details{
        id = UserId, name = Name, connected_accounts = ConnectedAccounts,
        alias = Alias, email_list = EmailList}
    } = oz_users:get_public_details(Auth, UserId),

    OnedataUser = #od_user{
        name = Name,
        connected_accounts = ConnectedAccounts,
        alias = Alias,
        email_list = EmailList
    },

    {ok, #document{key = UserId, value = OnedataUser}}.

%%--------------------------------------------------------------------
%% @doc
%% Get user from cache or fetch from OZ and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(Auth :: oz_endpoint:auth(), UserId :: id()) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Auth, UserId) ->
    try
        case od_user:get(UserId) of
            {ok, Doc} -> {ok, Doc};
            {error, {not_found, _}} -> fetch(Auth);
            Error -> Error
        end
    catch
        _:Reason ->
            ?error_stacktrace("Cannot get or fetch details of onedata user ~p due to: ~p",
                [UserId, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get public user data from cache or fetch from OZ.
%% @end
%%--------------------------------------------------------------------
-spec get_public_user_data(Auth :: oz_endpoint:auth(), UserId :: id()) ->
    {ok, datastore:document()} | datastore:get_error().
get_public_user_data(Auth, UserId) ->
    try
        case od_user:get(UserId) of
            {ok, Doc} -> {ok, Doc};
            {error, {not_found, _}} -> fetch_public_user(Auth, UserId);
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
-spec run_and_update_user(function(), list()) -> {ok, datastore:ext_key()} | datastore:update_error().
run_and_update_user(Function, Args) ->
    case apply(Function, Args) of
        {ok, Uuid} ->
            file_meta:setup_onedata_user(provider, Uuid),
            {ok, Uuid};
        Error ->
            Error
    end.