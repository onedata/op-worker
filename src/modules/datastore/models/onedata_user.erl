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
-module(onedata_user).
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
-export([fetch/1, get_or_fetch/2, create_or_update/2]).

-export_type([id/0, connected_account/0]).

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
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(?ROOT_USER_ID) ->
    {ok, #document{key = ?ROOT_USER_ID, value = #onedata_user{name = <<"root">>}}};
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    % TODO migrate to GLOBALLY_CACHED_LEVEL
    ?MODEL_CONFIG(onedata_user_bucket, [], ?DISK_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
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
-spec create_or_update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
create_or_update(Doc, Diff) ->
    datastore:create_or_update(?STORE_LEVEL, Doc, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Fetch user from OZ and save it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Client :: oz_endpoint:client()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
fetch(Client) ->
    {ok, #user_details{
        id = UserId, name = Name, connected_accounts = ConnectedAccounts,
        alias = Alias, email_list = EmailList}
    } = oz_users:get_details(Client),
    {ok, #user_spaces{ids = SpaceIds, default = DefaultSpaceId}} =
        oz_users:get_spaces(Client),
    {ok, GroupIds} = oz_users:get_groups(Client),
    {ok, EffectiveGroupIds} = oz_users:get_effective_groups(Client),

    Spaces = utils:pmap(fun(SpaceId) ->
        {ok, #space_details{name = SpaceName}} =
            oz_spaces:get_details(Client, SpaceId),
        {SpaceId, SpaceName}
    end, SpaceIds),

    OnedataUser = #onedata_user{
        name = Name,
        spaces = Spaces,
        default_space = DefaultSpaceId,
        group_ids = GroupIds,
        effective_group_ids = EffectiveGroupIds,
        connected_accounts = ConnectedAccounts,
        alias = Alias,
        email_list = EmailList
    },
    OnedataUserDoc = #document{key = UserId, value = OnedataUser},
    {ok, _} = onedata_user:save(OnedataUserDoc),

    utils:pforeach(fun(SpaceId) ->
        space_info:get_or_fetch(Client, SpaceId, UserId)
    end, SpaceIds),

    utils:pforeach(fun(GroupId) ->
        onedata_group:get_or_fetch(Client, GroupId)
    end, GroupIds),

    {ok, OnedataUserDoc}.

%%--------------------------------------------------------------------
%% @doc
%% Get user from cache or fetch from OZ and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(Client :: oz_endpoint:client(), UserId :: id()) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Client, UserId) ->
    try
        case onedata_user:get(UserId) of
            {ok, Doc} -> {ok, Doc};
            {error, {not_found, _}} -> fetch(Client);
            Error -> Error
        end
    catch
        _:Reason ->
            ?error_stacktrace("Cannot get or fetch details of onedata user ~p due to: ~p",
                [UserId, Reason]),
            {error, Reason}
    end.
