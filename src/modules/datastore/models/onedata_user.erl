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
-include_lib("ctool/include/oz/oz_users.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%% API
-export([fetch/1, get_or_fetch/2, get_spaces/1, create_or_update/2]).

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
-spec fetch(Auth :: #auth{}) -> {ok, datastore:document()} | {error, Reason :: term()}.
fetch(#auth{macaroon = Macaroon, disch_macaroons = DMacaroons} = Auth) ->
    try
        Client = {user, {Macaroon, DMacaroons}},
        {ok, #user_details{
            id = UserId, name = Name, connected_accounts = ConnectedAccounts,
            alias = Alias, email_list = EmailList}
        } = oz_users:get_details(Client),
        {ok, #user_spaces{ids = SpaceIds, default = DefaultSpaceId}} =
            oz_users:get_spaces(Client),
        {ok, GroupIds} = oz_users:get_groups(Client),
        [{ok, _} = onedata_group:get_or_fetch(Gid, Auth) || Gid <- GroupIds],
        OnedataUser = #onedata_user{
            name = Name,
            space_ids = [DefaultSpaceId | SpaceIds -- [DefaultSpaceId]],
            group_ids = GroupIds,
            connected_accounts = ConnectedAccounts,
            alias = Alias,
            email_list = EmailList
        },
        [(catch space_info:get_or_fetch(Client, SpaceId, UserId)) || SpaceId <- SpaceIds],
        OnedataUserDoc = #document{key = UserId, value = OnedataUser},
        {ok, _} = onedata_user:save(OnedataUserDoc),
        {ok, OnedataUserDoc}
    catch
        _:Reason ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get user from cache or fetch from OZ and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(datastore:key(), #auth{}) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Key, Token) ->
    case onedata_user:get(Key) of
        {ok, Doc} -> {ok, Doc};
        {error, {not_found, _}} -> fetch(Token);
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of user space IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces(UserId :: onedata_user:id()) ->
    {ok, [SpaceId :: binary()]} | {error, Reason :: term()}.
get_spaces(UserId) ->
    case onedata_user:get(UserId) of
        {ok, #document{value = #onedata_user{space_ids = SpaceIds}}} ->
            {ok, SpaceIds};
        {error, Reason} ->
            {error, Reason}
    end.
